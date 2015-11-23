// Copyright 2015 Red Hat, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cephapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/skyrings/bigfin/backend"
	"github.com/skyrings/bigfin/backend/cephapi/handler"
	"github.com/skyrings/bigfin/backend/cephapimodels"
	"github.com/skyrings/skyring/tools/uuid"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

const (
	CEPH_API_DEFAULT_PREFIX = "api"
	CEPH_API_PORT           = 8002
)

type CephApi struct {
}

func (c CephApi) CreateCluster(clusterName string, fsid uuid.UUID, mons []backend.Mon) (bool, error) {
	return true, nil
}

func (c CephApi) AddMon(clusterName string, mons []backend.Mon) (bool, error) {
	return true, nil
}

func (c CephApi) StartMon(nodes []string) (bool, error) {
	return true, nil
}

func (c CephApi) AddOSD(clusterName string, osd backend.OSD) (bool, error) {
	return true, nil
}

func (c CephApi) CreatePool(name string, mon string, clusterName string, pgnum uint) (bool, error) {
	// Get the cluster id
	cluster_id, err := cluster_id(clusterName, mon)
	if err != nil {
		return false, errors.New(fmt.Sprintf("Could not get id for cluster: %v", err))
	}
	// Replace cluster id in route pattern
	createPoolRoute := CEPH_API_ROUTES["CreatePool"]
	createPoolRoute.Pattern = strings.Replace(createPoolRoute.Pattern, "{cluster-fsid}", cluster_id, 1)

	pool := cephapimodels.CephPoolRequest{
		Name:                name,
		Size:                2,
		MinSize:             1,
		QuotaMaxObjects:     0,
		HashPsPool:          false,
		QuotaMaxBytes:       0,
		PgNum:               64,
		PgpNum:              64,
		CrashReplayInterval: 0,
	}
	buf, err := json.Marshal(pool)
	if err != nil {
		return false, errors.New(fmt.Sprintf("Error formaing request body: %v", err))
	}
	body := bytes.NewBuffer(buf)
	resp, err := route_request(createPoolRoute, mon, body)
	if err != nil || (resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted) {
		return false, errors.New(fmt.Sprintf("Failed to create pool: %v", err))
	} else {
		var asyncReq cephapimodels.CephAsyncRequest
		respBodyStr, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return false, errors.New(fmt.Sprintf("Error parsing response data: %v", err))
		}
		if err := json.Unmarshal(respBodyStr, &asyncReq); err != nil {
			return false, errors.New(fmt.Sprintf("Error parsing response data: %v", err))
		}
		// Keep checking for the status of the request, and if completed return
		for {
			time.Sleep(2 * time.Second)
			route := CEPH_API_ROUTES["GetRequestStatus"]
			route.Pattern = strings.Replace(route.Pattern, "{request-fsid}", asyncReq.RequestId, 1)
			resp, err := route_request(route, mon, bytes.NewBuffer([]byte{}))
			if err != nil {
				return false, errors.New("Error syncing request status from cluster")
			}
			var reqStatus cephapimodels.CephRequestStatus
			respBodyStr, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return false, errors.New(fmt.Sprintf("Error parsing response data: %v", err))
			}
			if err := json.Unmarshal(respBodyStr, &reqStatus); err != nil {
				return false, errors.New(fmt.Sprintf("Error parsing response data: %v", err))
			}
			if reqStatus.State == "complete" {
				break
			}
		}
	}
	return true, nil
}

func (c CephApi) ListPool(mon string, clusterName string) ([]string, error) {
	return []string{}, nil
}

func New() backend.Backend {
	api := new(CephApi)
	api.LoadRoutes()
	return api
}

func cluster_id(clusterName string, mon string) (string, error) {
	resp, err := route_request(CEPH_API_ROUTES["GetCluster"], mon, bytes.NewBuffer([]byte{}))
	if err != nil {
		return "", err
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var clusters []cephapimodels.CephCluster
	if err := json.Unmarshal(respBody, &clusters); err != nil {
		return "", err
	}
	for _, cluster := range clusters {
		if cluster.Name == clusterName {
			return cluster.Id, nil
		}
	}
	return "", errors.New(fmt.Sprintf("Unable to find cluster id for: %s", clusterName))
}

func route_request(route CephApiRoute, mon string, body io.Reader) (*http.Response, error) {
	if route.Method == "POST" {
		return handler.HttpPost(
			fmt.Sprintf("http://%s:%d/%s/v%d/auth/login", mon, CEPH_API_PORT, CEPH_API_DEFAULT_PREFIX, route.Version),
			fmt.Sprintf("http://%s:%d/%s/v%d/%s", mon, CEPH_API_PORT, CEPH_API_DEFAULT_PREFIX, route.Version, route.Pattern),
			"application/json",
			body)
	}
	if route.Method == "GET" {
		return handler.HttpGet(fmt.Sprintf("http://%s:%d/%s/v%d/%s", mon, CEPH_API_PORT, CEPH_API_DEFAULT_PREFIX, route.Version, route.Pattern))
	}
	return nil, errors.New("Invalid method type")
}
