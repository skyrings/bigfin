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
	"github.com/skyrings/bigfin/backend/cephapi/models"
	"github.com/skyrings/skyring-common/conf"
	"github.com/skyrings/skyring-common/db"
	"github.com/skyrings/skyring-common/tools/uuid"
	"gopkg.in/mgo.v2/bson"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	skyringmodels "github.com/skyrings/skyring-common/models"
)

type CephApi struct {
}

func (c CephApi) CreateCluster(clusterName string, fsid uuid.UUID, mons []backend.Mon, ctxt string) (bool, error) {
	return true, nil
}

func (c CephApi) AddMon(clusterName string, mons []backend.Mon, ctxt string) (bool, error) {
	return true, nil
}

func (c CephApi) StartMon(nodes []string, ctxt string) (bool, error) {
	return true, nil
}

func (c CephApi) AddOSD(clusterName string, osd backend.OSD, ctxt string) (map[string][]string, error) {
	return map[string][]string{}, nil
}

func (c CephApi) CreatePool(name string, mon string, clusterName string, pgnum uint, replicas int, quotaMaxObjects int, quotaMaxBytes uint64) (bool, error) {
	// Get the cluster id
	cluster_id, err := cluster_id(clusterName)
	if err != nil {
		return false, errors.New(fmt.Sprintf("Could not get id for cluster: %s. error: %v", clusterName, err))
	}

	// Replace cluster id in route pattern
	createPoolRoute := CEPH_API_ROUTES["CreatePool"]
	createPoolRoute.Pattern = strings.Replace(createPoolRoute.Pattern, "{cluster-fsid}", cluster_id, 1)

	pool := map[string]interface{}{
		"name":              name,
		"size":              replicas,
		"quota_max_objects": quotaMaxObjects,
		"quota_max_bytes":   quotaMaxBytes,
		"pg_num":            int(pgnum),
		"pgp_num":           int(pgnum),
	}

	buf, err := json.Marshal(pool)
	if err != nil {
		return false, errors.New(fmt.Sprintf("Error forming request body. error: %v", err))
	}
	body := bytes.NewBuffer(buf)
	resp, err := route_request(createPoolRoute, mon, body)
	if err != nil || (resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted) {
		return false, errors.New(fmt.Sprintf("Failed to create pool: %s for cluster: %s. error: %v", name, clusterName, err))
	} else {
		ok, err := syncRequestStatus(mon, resp)
		return ok, err
	}
}

func (c CephApi) ListPoolNames(mon string, clusterName string) ([]string, error) {
	return []string{}, nil
}

func (c CephApi) GetClusterStatus(mon string, clusterId uuid.UUID, clusterName string) (status string, err error) {
	// Replace cluster id in route pattern
	getPoolsRoute := CEPH_API_ROUTES["GetClusterStatus"]
	getPoolsRoute.Pattern = strings.Replace(getPoolsRoute.Pattern, "{cluster-fsid}", clusterId.String(), 1)
	resp, err := route_request(getPoolsRoute, mon, bytes.NewBuffer([]byte{}))
	if err != nil {
		return "", err
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var clusterHealth backend.CephClusterHealth
	if err := json.Unmarshal(respBody, &clusterHealth); err != nil {
		return "", err
	}
	return clusterHealth.OverallStatus, nil
}

func (c CephApi) GetClusterStats(mon string, clusterName string) (map[string]int64, error) {
	return nil, nil
}
func New() backend.Backend {
	api := new(CephApi)
	api.LoadRoutes()
	return api
}

func syncRequestStatus(mon string, resp *http.Response) (bool, error) {
	var asyncReq models.CephAsyncRequest
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
		var reqStatus models.CephRequestStatus
		respBodyStr, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return false, errors.New(fmt.Sprintf("Error parsing response data: %v", err))
		}
		if err := json.Unmarshal(respBodyStr, &reqStatus); err != nil {
			return false, errors.New(fmt.Sprintf("Error parsing response data: %v", err))
		}
		if reqStatus.State == "complete" {
			// If request has failed return with error
			if reqStatus.Error {
				return false, errors.New(fmt.Sprintf("Request failed. error: %s", reqStatus.ErrorMessage))
			}
			break
		}
	}
	return true, nil
}

func cluster_id(clusterName string) (string, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	var cluster skyringmodels.Cluster
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(skyringmodels.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Find(bson.M{"name": clusterName}).One(&cluster); err != nil {
		return "", err
	}
	return cluster.ClusterId.String(), nil
}

func route_request(route CephApiRoute, mon string, body io.Reader) (*http.Response, error) {
	if route.Method == "POST" {
		return handler.HttpPost(
			mon,
			fmt.Sprintf("http://%s:%d/%s/v%d/%s", mon, models.CEPH_API_PORT, models.CEPH_API_DEFAULT_PREFIX, route.Version, route.Pattern),
			"application/json",
			body)
	}
	if route.Method == "GET" {
		return handler.HttpGet(fmt.Sprintf("http://%s:%d/%s/v%d/%s", mon, models.CEPH_API_PORT, models.CEPH_API_DEFAULT_PREFIX, route.Version, route.Pattern))
	}
	if route.Method == "PATCH" {
		return handler.HttpPatch(
			mon,
			fmt.Sprintf("http://%s:%d/%s/v%d/%s", mon, models.CEPH_API_PORT, models.CEPH_API_DEFAULT_PREFIX, route.Version, route.Pattern),
			"application/json",
			body)
	}
	if route.Method == "DELETE" {
		return handler.HttpDelete(
			mon,
			fmt.Sprintf("http://%s:%d/%s/v%d/%s", mon, models.CEPH_API_PORT, models.CEPH_API_DEFAULT_PREFIX, route.Version, route.Pattern),
			"application/json",
			body)
	}
	return nil, errors.New(fmt.Sprintf("Invalid method type: %s", route.Method))
}

func (c CephApi) GetPools(mon string, clusterId uuid.UUID) ([]backend.CephPool, error) {
	// Replace cluster id in route pattern
	getPoolsRoute := CEPH_API_ROUTES["GetPools"]
	getPoolsRoute.Pattern = strings.Replace(getPoolsRoute.Pattern, "{cluster-fsid}", clusterId.String(), 1)
	resp, err := route_request(getPoolsRoute, mon, bytes.NewBuffer([]byte{}))
	if err != nil {
		return []backend.CephPool{}, err
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []backend.CephPool{}, err
	}
	var pools []backend.CephPool
	if err := json.Unmarshal(respBody, &pools); err != nil {
		return []backend.CephPool{}, err
	}
	return pools, nil
}

func (c CephApi) UpdatePool(mon string, clusterId uuid.UUID, poolId int, pool map[string]interface{}) (bool, error) {
	// Replace cluster id in route pattern
	updatePoolRoute := CEPH_API_ROUTES["UpdatePool"]
	updatePoolRoute.Pattern = strings.Replace(updatePoolRoute.Pattern, "{cluster-fsid}", clusterId.String(), 1)
	updatePoolRoute.Pattern = strings.Replace(updatePoolRoute.Pattern, "{pool-id}", strconv.Itoa(poolId), 1)

	buf, err := json.Marshal(pool)
	if err != nil {
		return false, errors.New(fmt.Sprintf("Error forming request body: %v", err))
	}
	body := bytes.NewBuffer(buf)
	resp, err := route_request(updatePoolRoute, mon, body)
	if err != nil || (resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted) {
		return false, errors.New(fmt.Sprintf("Failed to update pool-id: %d for cluster: %v.error: %v", poolId, clusterId, err))
	} else {
		ok, err := syncRequestStatus(mon, resp)
		return ok, err
	}
}

func (c CephApi) RemovePool(mon string, clusterId uuid.UUID, clusterName string, pool string, poolId int, ctxt string) (bool, error) {
	// Replace cluster id in route pattern
	removePoolRoute := CEPH_API_ROUTES["RemovePool"]
	removePoolRoute.Pattern = strings.Replace(removePoolRoute.Pattern, "{cluster-fsid}", clusterId.String(), 1)
	removePoolRoute.Pattern = strings.Replace(removePoolRoute.Pattern, "{pool-id}", strconv.Itoa(poolId), 1)

	resp, err := route_request(removePoolRoute, mon, bytes.NewBuffer([]byte{}))
	if err != nil || (resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted) {
		return false, errors.New(fmt.Sprintf("Failed to remove pool-id: %d for cluster: %v.error: %v", poolId, clusterId, err))
	} else {
		ok, err := syncRequestStatus(mon, resp)
		return ok, err
	}
}

func (c CephApi) GetOSDDetails(mon string, clusterName string) (osds []backend.OSDDetails, err error) {
	return []backend.OSDDetails{}, nil
}

func (c CephApi) GetObjectCount(mon string, clusterName string) (string, error) {
	return "", nil
}

func (c CephApi) GetPGSummary(mon string, clusterId uuid.UUID) (backend.PgSummary, error) {

	// Replace cluster id in route pattern
	createPoolRoute := CEPH_API_ROUTES["PGStatistics"]
	createPoolRoute.Pattern = strings.Replace(createPoolRoute.Pattern, "{cluster-fsid}", clusterId.String(), 1)
	resp, err := route_request(createPoolRoute, mon, bytes.NewBuffer([]byte{}))
	var pgsummary backend.PgSummary
	if err != nil {
		return pgsummary, err
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return pgsummary, err
	}
	if err := json.Unmarshal(respBody, &pgsummary); err != nil {
		return pgsummary, err
	}
	return pgsummary, err
}

func (c CephApi) ExecCmd(mon string, clusterId uuid.UUID, cmd string) (bool, string, error) {
	// Replace cluster id in route pattern
	createPoolRoute := CEPH_API_ROUTES["ExecCmd"]
	createPoolRoute.Pattern = strings.Replace(createPoolRoute.Pattern, "{cluster-fsid}", clusterId.String(), 1)
	command := map[string][]string{"command": strings.Split(cmd, " ")}
	buf, err := json.Marshal(command)
	if err != nil {
		return false, "", errors.New(fmt.Sprintf("Error forming request body. error: %v", err))
	}
	body := bytes.NewBuffer(buf)
	resp, err := route_request(createPoolRoute, mon, body)
	if err != nil || resp.StatusCode != http.StatusOK {
		return false, "", errors.New(fmt.Sprintf("Failed to execute command: %s. error: %v", cmd, err))
	} else {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return false, "", err
		}
		var cmdExecResp models.CephCommandResponse
		if err := json.Unmarshal(respBody, &cmdExecResp); err != nil {
			return false, "", err
		}
		if cmdExecResp.Status != 0 {
			return false, "", fmt.Errorf(cmdExecResp.Error)
		} else {
			return true, cmdExecResp.Out, nil
		}
	}
}

func (c CephApi) GetOSDs(mon string, clusterId uuid.UUID) ([]backend.CephOSD, error) {
	// Replace cluster id in route pattern
	getOsdsRoute := CEPH_API_ROUTES["GetOSDs"]
	getOsdsRoute.Pattern = strings.Replace(getOsdsRoute.Pattern, "{cluster-fsid}", clusterId.String(), 1)
	resp, err := route_request(getOsdsRoute, mon, bytes.NewBuffer([]byte{}))
	if err != nil {
		return []backend.CephOSD{}, err
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []backend.CephOSD{}, err
	}
	var osds []backend.CephOSD
	if err := json.Unmarshal(respBody, &osds); err != nil {
		return []backend.CephOSD{}, err
	}
	return osds, nil
}

func (c CephApi) UpdateOSD(mon string, clusterId uuid.UUID, osdId string, params map[string]interface{}) (bool, error) {
	// Replace cluster id in route pattern
	updateOsdRoute := CEPH_API_ROUTES["UpdateOSD"]
	updateOsdRoute.Pattern = strings.Replace(updateOsdRoute.Pattern, "{cluster-fsid}", clusterId.String(), 1)
	updateOsdRoute.Pattern = strings.Replace(updateOsdRoute.Pattern, "{osd-id}", osdId, 1)

	buf, err := json.Marshal(params)
	if err != nil {
		return false, errors.New(fmt.Sprintf("Error forming request body: %v", err))
	}
	body := bytes.NewBuffer(buf)
	resp, err := route_request(updateOsdRoute, mon, body)
	if err != nil || (resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted) {
		return false, errors.New(fmt.Sprintf("Failed to update osd-id: %s for cluster: %v.error: %v", osdId, clusterId, err))
	} else {
		ok, err := syncRequestStatus(mon, resp)
		return ok, err
	}
}

func (c CephApi) GetOSD(mon string, clusterId uuid.UUID, osdId string) (backend.CephOSD, error) {
	getOsdRoute := CEPH_API_ROUTES["GetOSD"]
	getOsdRoute.Pattern = strings.Replace(getOsdRoute.Pattern, "{cluster-fsid}", clusterId.String(), 1)
	getOsdRoute.Pattern = strings.Replace(getOsdRoute.Pattern, "{osd-id}", osdId, 1)

	resp, err := route_request(getOsdRoute, mon, bytes.NewBuffer([]byte{}))
	if err != nil {
		return backend.CephOSD{}, err
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return backend.CephOSD{}, err
	}
	var osds []backend.CephOSD
	if err := json.Unmarshal(respBody, &osds); err != nil {
		return backend.CephOSD{}, err
	}
	if len(osds) > 0 {
		return osds[0], nil
	} else {
		return backend.CephOSD{}, errors.New("Couldn't retrieve the specified OSD")
	}
}

func (c CephApi) GetMonitors(mon string, clusterId uuid.UUID) ([]string, error) {
	getMonsRoute := CEPH_API_ROUTES["GetMons"]
	getMonsRoute.Pattern = strings.Replace(getMonsRoute.Pattern, "{cluster-fsid}", clusterId.String(), 1)

	resp, err := route_request(getMonsRoute, mon, bytes.NewBuffer([]byte{}))
	if err != nil {
		return []string{}, err
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []string{}, err
	}
	var monsDet backend.CephMons
	if err := json.Unmarshal(respBody, &monsDet); err != nil {
		return []string{}, err
	}
	if len(monsDet.Mons) > 0 {
		var list []string
		for _, mon := range monsDet.Mons {
			list = append(list, strings.Split(mon.Addr, ":")[0])
		}
		return list, nil
	} else {
		return []string{}, errors.New("Couldn't retrieve the mons")
	}
}
