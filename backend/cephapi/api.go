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
	skyringmodels "github.com/skyrings/skyring-common/models"
	"github.com/skyrings/skyring-common/monitoring"
	skyring_monitoring "github.com/skyrings/skyring-common/monitoring"
	"github.com/skyrings/skyring-common/tools/uuid"
	"gopkg.in/mgo.v2/bson"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
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

func (c CephApi) CreatePool(name string, mon string, clusterName string, pgnum uint, replicas int, quotaMaxObjects int, quotaMaxBytes uint64, ruleset int, ctxt string) (bool, error) {
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
		"crush_ruleset":     ruleset,
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

func (c CephApi) CreateECPool(
	name string,
	mon string,
	clusterName string,
	pgnum uint,
	replicas int,
	quotaMaxObjects int,
	quotaMaxBytes uint64,
	ecProfile string,
	ruleset int,
	ctxt string) (bool, error) {
	// Get the cluster id
	cluster_id, err := cluster_id(clusterName)
	if err != nil {
		return false, errors.New(fmt.Sprintf("Could not get id for cluster: %s. error: %v", clusterName, err))
	}

	// Replace cluster id in route pattern
	createPoolRoute := CEPH_API_ROUTES["CreatePool"]
	createPoolRoute.Pattern = strings.Replace(createPoolRoute.Pattern, "{cluster-fsid}", cluster_id, 1)

	pool := map[string]interface{}{
		"name":                 name,
		"size":                 replicas,
		"quota_max_objects":    quotaMaxObjects,
		"quota_max_bytes":      quotaMaxBytes,
		"pg_num":               int(pgnum),
		"pgp_num":              int(pgnum),
		"type":                 "erasure",
		"erasure_code_profile": ecProfile,
		"crush_ruleset":        ruleset,
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

func (c CephApi) ListPoolNames(mon string, clusterName string, ctxt string) ([]string, error) {
	return []string{}, nil
}

func (c CephApi) GetClusterStatus(mon string, clusterId uuid.UUID, clusterName string, ctxt string) (status string, err error) {
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

func (c CephApi) GetClusterStats(mon string, clusterName string, ctxt string) (backend.ClusterUtilization, error) {
	return backend.ClusterUtilization{}, nil
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
	var reqStatus models.CephRequestStatus
	for count := 0; count < 30; count++ {
		time.Sleep(2 * time.Second)
		route := CEPH_API_ROUTES["GetRequestStatus"]
		route.Pattern = strings.Replace(route.Pattern, "{request-fsid}", asyncReq.RequestId, 1)
		resp, err := route_request(route, mon, bytes.NewBuffer([]byte{}))
		if err != nil {
			return false, errors.New("Error syncing request status from cluster")
		}
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
	if reqStatus.State == "" || reqStatus.State != "complete" {
		return false, fmt.Errorf("Syncing request status timed out")
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

func (c CephApi) GetPools(mon string, clusterId uuid.UUID, ctxt string) ([]backend.CephPool, error) {
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

func (c CephApi) UpdatePool(mon string, clusterId uuid.UUID, poolId int, pool map[string]interface{}, ctxt string) (bool, error) {
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

func (c CephApi) GetOSDDetails(mon string, clusterName string, ctxt string) (osds []backend.OSDDetails, err error) {
	return []backend.OSDDetails{}, nil
}

func (c CephApi) GetObjectCount(mon string, clusterName string, ctxt string) (map[string]int64, error) {
	return map[string]int64{}, nil
}

func GetPgStatusBasedCount(status string, clusterId uuid.UUID, pgMap map[string]interface{}) (uint64, error) {
	pgCount, pgCountOk := pgMap[status].(map[string]interface{})
	if !pgCountOk {
		return 0, fmt.Errorf("Failed to fetch number of pgs in error for the cluster %v", clusterId)
	}
	ipgCount, ipgCountOk := pgCount["count"].(float64)
	if !ipgCountOk {
		return 0, fmt.Errorf("Failed to fetch number of pgs in error for the cluster %v", clusterId)
	}
	return uint64(ipgCount), nil
}

func (c CephApi) GetPGCount(mon string, clusterId uuid.UUID, ctxt string) (map[string]uint64, error) {
	pgStatsRoute := CEPH_API_ROUTES["GetPGCount"]
	pgStatsRoute.Pattern = strings.Replace(pgStatsRoute.Pattern, "{cluster-fsid}", clusterId.String(), 1)
	resp, err := route_request(pgStatsRoute, mon, bytes.NewBuffer([]byte{}))
	var pgSummary map[string]interface{}
	if err != nil {
		return nil, err
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(respBody, &pgSummary); err != nil {
		return nil, err
	}
	pgMap, pgMapOk := pgSummary["pg"].(map[string]interface{})
	if !pgMapOk {
		return nil, fmt.Errorf("%s - Failed to fetch number of pgs for the cluster %v", ctxt, clusterId)
	}

	pgCount := make(map[string]uint64)

	var errPgCountError error
	var warnPgCountError error
	var okPgCountError error

	/*
		Error Pg Count
	*/

	pgCount[skyring_monitoring.CRITICAL], errPgCountError = GetPgStatusBasedCount(monitoring.CRITICAL, clusterId, pgMap)
	if errPgCountError != nil {
		return nil, fmt.Errorf("%s - Err: %v", ctxt, errPgCountError)
	}

	/*
		Warning Pg Count
	*/

	pgCount[skyringmodels.STATUS_WARN], warnPgCountError = GetPgStatusBasedCount(monitoring.WARN, clusterId, pgMap)
	if warnPgCountError != nil {
		return nil, fmt.Errorf("%s - Err: %v", ctxt, warnPgCountError)
	}

	/*
		Clean Pg Count
	*/

	pgCount[skyringmodels.STATUS_OK], okPgCountError = GetPgStatusBasedCount(monitoring.OK, clusterId, pgMap)
	if okPgCountError != nil {
		return nil, fmt.Errorf("%s - Err: %v", ctxt, okPgCountError)
	}

	return pgCount, nil
}

func (c CephApi) GetPGSummary(mon string, clusterId uuid.UUID, ctxt string) (backend.PgSummary, error) {

	// Replace cluster id in route pattern
	pgStatsRoute := CEPH_API_ROUTES["PGStatistics"]
	pgStatsRoute.Pattern = strings.Replace(pgStatsRoute.Pattern, "{cluster-fsid}", clusterId.String(), 1)
	resp, err := route_request(pgStatsRoute, mon, bytes.NewBuffer([]byte{}))
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

func (c CephApi) ExecCmd(mon string, clusterId uuid.UUID, cmd string, ctxt string) (bool, string, error) {
	// Replace cluster id in route pattern
	execCmdRoute := CEPH_API_ROUTES["ExecCmd"]
	execCmdRoute.Pattern = strings.Replace(execCmdRoute.Pattern, "{cluster-fsid}", clusterId.String(), 1)
	command := map[string][]string{"command": strings.Split(cmd, " ")}
	buf, err := json.Marshal(command)
	if err != nil {
		return false, "", errors.New(fmt.Sprintf("Error forming request body. error: %v", err))
	}
	body := bytes.NewBuffer(buf)
	resp, err := route_request(execCmdRoute, mon, body)
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

func (c CephApi) GetOSDs(mon string, clusterId uuid.UUID, ctxt string) ([]backend.CephOSD, error) {
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

func (c CephApi) UpdateOSD(mon string, clusterId uuid.UUID, osdId string, params map[string]interface{}, ctxt string) (bool, error) {
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

func (c CephApi) GetOSD(mon string, clusterId uuid.UUID, osdId string, ctxt string) (backend.CephOSD, error) {
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

func (c CephApi) GetClusterConfig(mon string, clusterId uuid.UUID, ctxt string) (map[string]string, error) {
	getClusterConfigRoute := CEPH_API_ROUTES["GetClusterConfig"]
	getClusterConfigRoute.Pattern = strings.Replace(getClusterConfigRoute.Pattern, "{cluster-fsid}", clusterId.String(), 1)

	resp, err := route_request(getClusterConfigRoute, mon, bytes.NewBuffer([]byte{}))
	if err != nil {
		return map[string]string{}, err
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return map[string]string{}, err
	}
	var configs map[string]string
	if err := json.Unmarshal(respBody, &configs); err != nil {
		return map[string]string{}, err
	}
	return configs, nil
}

func (c CephApi) CreateCrushRule(mon string, clusterId uuid.UUID, rule backend.CrushRuleRequest, ctxt string) (int, error) {
	// Replace cluster id in route pattern
	var cRuleId int
	route := CEPH_API_ROUTES["CreateCrushRule"]
	route.Pattern = strings.Replace(route.Pattern, "{cluster-fsid}", clusterId.String(), 1)

	buf, err := json.Marshal(rule)
	if err != nil {
		return cRuleId, errors.New(fmt.Sprintf("Error forming request body. error: %v", err))
	}
	body := bytes.NewBuffer(buf)
	resp, err := route_request(route, mon, body)
	if err != nil || (resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted) {
		return cRuleId, errors.New(fmt.Sprintf("Failed to create crush rule for cluster: %s. error: %v", clusterId.String(), err))
	} else {
		ok, err := syncRequestStatus(mon, resp)
		if !ok {
			return cRuleId, err
		}
		cRules, err := c.GetCrushRules(mon, clusterId, ctxt)
		if err != nil {
			return cRuleId, err
		}
		for _, cRule := range cRules {
			//ruleMap := cRule.(map[string]interface{})
			if val, ok := cRule["name"]; ok {
				if rule.Name == val.(string) {
					if val, ok := cRule["ruleset"]; ok {
						cRuleId = int(val.(float64))
						return cRuleId, nil
					}
				}

			}
		}
	}

	return cRuleId, errors.New("Failed to retrieve the Crush Ruleset Id")
}

func (c CephApi) CreateCrushNode(mon string, clusterId uuid.UUID, node backend.CrushNodeRequest, ctxt string) (int, error) {
	// Replace cluster id in route pattern
	var cNodeId int
	route := CEPH_API_ROUTES["CreateCrushNode"]
	route.Pattern = strings.Replace(route.Pattern, "{cluster-fsid}", clusterId.String(), 1)

	buf, err := json.Marshal(node)
	if err != nil {
		return cNodeId, errors.New(fmt.Sprintf("Error forming request body. error: %v", err))
	}
	body := bytes.NewBuffer(buf)
	resp, err := route_request(route, mon, body)
	if err != nil || (resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted) {
		return cNodeId, errors.New(fmt.Sprintf("Failed to create crush rule for cluster: %s. error: %v", clusterId.String(), err))
	} else {
		ok, err := syncRequestStatus(mon, resp)
		if !ok {
			return cNodeId, err
		}
		cNodes, err := c.GetCrushNodes(mon, clusterId, ctxt)
		if err != nil {
			return cNodeId, err
		}
		for _, cNode := range cNodes {
			if cNode.Name == node.Name {
				return cNode.Id, nil
			}
		}
	}

	return cNodeId, errors.New("Failed to retrieve the Crush Node Id")
}

func (c CephApi) GetCrushNodes(mon string, clusterId uuid.UUID, ctxt string) ([]backend.CrushNode, error) {
	route := CEPH_API_ROUTES["GetCrushNodes"]
	route.Pattern = strings.Replace(route.Pattern, "{cluster-fsid}", clusterId.String(), 1)
	resp, err := route_request(route, mon, bytes.NewBuffer([]byte{}))
	if err != nil {
		return []backend.CrushNode{}, err
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []backend.CrushNode{}, err
	}
	var nodes []backend.CrushNode
	if err := json.Unmarshal(respBody, &nodes); err != nil {
		return []backend.CrushNode{}, err
	}
	return nodes, nil
}

func (c CephApi) PatchCrushNode(mon string, clusterId uuid.UUID, crushNodeId int, params map[string]interface{}, ctxt string) (bool, error) {
	// Replace cluster id in route pattern
	route := CEPH_API_ROUTES["PatchCrushNode"]
	route.Pattern = strings.Replace(route.Pattern, "{cluster-fsid}", clusterId.String(), 1)
	route.Pattern = strings.Replace(route.Pattern, "{crush_node_id}", strconv.Itoa(crushNodeId), 1)

	buf, err := json.Marshal(params)
	if err != nil {
		return false, errors.New(fmt.Sprintf("Error forming request body: %v", err))
	}
	body := bytes.NewBuffer(buf)
	resp, err := route_request(route, mon, body)
	if err != nil || (resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted) {
		return false, errors.New(fmt.Sprintf("Failed to update crush node: %v for cluster: %v.error: %v", crushNodeId, clusterId, err))
	} else {
		ok, err := syncRequestStatus(mon, resp)
		return ok, err
	}
}

func (c CephApi) GetCrushRules(mon string, clusterId uuid.UUID, ctxt string) ([]map[string]interface{}, error) {
	// Replace cluster id in route pattern
	route := CEPH_API_ROUTES["GetCrushRules"]
	route.Pattern = strings.Replace(route.Pattern, "{cluster-fsid}", clusterId.String(), 1)

	var m []map[string]interface{}
	resp, err := route_request(route, mon, bytes.NewBuffer([]byte{}))
	if err != nil {
		return m, err
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(respBody, &m); err != nil {
		return m, err
	}

	return m, nil
}

func (c CephApi) GetMonitors(mon string, clusterId uuid.UUID, ctxt string) ([]string, error) {
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
			list = append(list, mon.Name)
		}
		return list, nil
	} else {
		return []string{}, errors.New("Couldn't retrieve the mons")
	}
}

func (c CephApi) GetCluster(mon string, ctxt string) (backend.CephCluster, error) {
	getClusterRoute := CEPH_API_ROUTES["GetCluster"]

	resp, err := route_request(getClusterRoute, mon, bytes.NewBuffer([]byte{}))
	if err != nil {
		return backend.CephCluster{}, err
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return backend.CephCluster{}, err
	}
	var clusters []backend.CephCluster
	if err := json.Unmarshal(respBody, &clusters); err != nil {
		return backend.CephCluster{}, err
	}
	if len(clusters) > 0 {
		return clusters[0], nil
	} else {
		return backend.CephCluster{}, fmt.Errorf("No cluster(s) found")
	}
}

func (c CephApi) GetClusterNetworks(mon string, clusterId uuid.UUID, ctxt string) (skyringmodels.ClusterNetworks, error) {
	getGetClusterNetworksRoute := CEPH_API_ROUTES["GetClusterNetworks"]
	getGetClusterNetworksRoute.Pattern = strings.Replace(
		getGetClusterNetworksRoute.Pattern,
		"{cluster-fsid}",
		clusterId.String(),
		1)
	resp, err := route_request(getGetClusterNetworksRoute, mon, bytes.NewBuffer([]byte{}))
	if err != nil {
		return skyringmodels.ClusterNetworks{}, err
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return skyringmodels.ClusterNetworks{}, err
	}
	var configs map[string]string = make(map[string]string)
	if err := json.Unmarshal(respBody, &configs); err != nil {
		return skyringmodels.ClusterNetworks{}, err
	}
	var clusterNetworks skyringmodels.ClusterNetworks
	clusterNetworks.Public = configs["public_network"]
	if configs["cluster_network"] == "" {
		clusterNetworks.Cluster = clusterNetworks.Public
	} else {
		clusterNetworks.Cluster = configs["cluster_network"]
	}
	return clusterNetworks, nil
}

func (c CephApi) GetClusterNodes(mon string, clusterId uuid.UUID, ctxt string) ([]backend.CephClusterNode, error) {
	getNodesRoute := CEPH_API_ROUTES["GetNodes"]
	getNodesRoute.Pattern = strings.Replace(
		getNodesRoute.Pattern,
		"{cluster-fsid}",
		clusterId.String(),
		1)
	resp, err := route_request(getNodesRoute, mon, bytes.NewBuffer([]byte{}))
	if err != nil {
		return []backend.CephClusterNode{}, err
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []backend.CephClusterNode{}, err
	}
	var clusterNodes []backend.CephClusterNode
	if err := json.Unmarshal(respBody, &clusterNodes); err != nil {
		return []backend.CephClusterNode{}, err
	}
	return clusterNodes, nil
}

func (c CephApi) GetMonStatus(mon string, clusterId uuid.UUID, node string, ctxt string) (backend.MonNodeStatus, error) {
	getMonStatusRoute := CEPH_API_ROUTES["GetMonStatus"]
	getMonStatusRoute.Pattern = strings.Replace(
		getMonStatusRoute.Pattern,
		"{cluster-fsid}",
		clusterId.String(),
		1)
	getMonStatusRoute.Pattern = strings.Replace(
		getMonStatusRoute.Pattern,
		"{mon-name}",
		node,
		1)
	resp, err := route_request(getMonStatusRoute, mon, bytes.NewBuffer([]byte{}))
	if err != nil {
		return backend.MonNodeStatus{}, err
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return backend.MonNodeStatus{}, err
	}
	var monStatus backend.MonNodeStatus
	if err := json.Unmarshal(respBody, &monStatus); err != nil {
		return backend.MonNodeStatus{}, err
	}
	return monStatus, nil
}

func (c CephApi) ParticipatesInCluster(node string, ctxt string) bool {
	return false
}

func (c CephApi) GetPartDeviceDetails(node string, partPath string, ctxt string) (backend.DeviceDetail, error) {
	return backend.DeviceDetail{}, nil
}
