/*Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package provider

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/skyrings/bigfin/backend/cephapi"
	"github.com/skyrings/bigfin/backend/salt"
	"github.com/skyrings/bigfin/bigfinmodels"
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/skyring-common/conf"
	"github.com/skyrings/skyring-common/db"
	"github.com/skyrings/skyring-common/models"
	"github.com/skyrings/skyring-common/tools/logger"
	"github.com/skyrings/skyring-common/tools/uuid"
	"gopkg.in/mgo.v2/bson"
	"net/http"
)

var (
	salt_backend    = salt.New()
	cephapi_backend = cephapi.New()
)

type CephProvider struct{}

func GetRandomMon(clusterId uuid.UUID) (*models.Node, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	var mons models.Nodes
	var clusterNodes models.Nodes
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	if err := coll.Find(bson.M{"clusterid": clusterId}).All(&clusterNodes); err != nil {
		return nil, err
	}
	for _, clusterNode := range clusterNodes {
		for k, v := range clusterNode.Options {
			if k == "mon" && v == "Y" {
				mons = append(mons, clusterNode)
			}
		}
	}
	if len(mons) <= 0 {
		return nil, errors.New(fmt.Sprintf("No mons available for cluster: %v", clusterId))
	}

	// Pick a random mon from the list
	var monNodeId uuid.UUID
	if len(mons) == 1 {
		monNodeId = mons[0].NodeId
	} else {
		randomIndex := utils.RandomNum(0, len(mons)-1)
		monNodeId = mons[randomIndex].NodeId
	}
	var monnode models.Node
	coll = sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	if err := coll.Find(bson.M{"nodeid": monNodeId}).One(&monnode); err != nil {
		return nil, err
	}
	return &monnode, nil
}

func GetMons(param bson.M) (models.Nodes, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	var mons models.Nodes
	var clusterNodes models.Nodes
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	if err := coll.Find(param).All(&clusterNodes); err != nil {
		return mons, err
	}
	for _, clusterNode := range clusterNodes {
		for k, v := range clusterNode.Options {
			if k == models.Mon && v == models.Yes {
				mons = append(mons, clusterNode)
			}
		}
	}
	return mons, nil
}

func CreateDefaultECProfiles(ctxt string, mon string, clusterId uuid.UUID) (bool, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	var cluster models.Cluster
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Find(bson.M{"clusterid": clusterId}).One(&cluster); err != nil {
		logger.Get().Error("%s-Error getting cluster details for %v. error: %v", ctxt, clusterId, err)
		return false, err
	}
	var cmdMap map[string]string = map[string]string{
		"4+2": fmt.Sprintf("ceph osd erasure-code-profile set k4m2 plugin=jerasure k=4 m=2 --cluster %s", cluster.Name),
		"6+3": fmt.Sprintf("ceph osd erasure-code-profile set k6m3 plugin=jerasure k=6 m=3 --cluster %s", cluster.Name),
		"8+4": fmt.Sprintf("ceph osd erasure-code-profile set k8m4 plugin=jerasure k=8 m=4 --cluster %s", cluster.Name),
	}

	for k, v := range cmdMap {
		ok, _, err := cephapi_backend.ExecCmd(mon, clusterId, v, ctxt)
		if err != nil || !ok {
			logger.Get().Error("%s-Error creating EC profile for %s. error: %v", ctxt, k, err)
			continue
		} else {
			logger.Get().Debug("%s-Added EC profile for %s", ctxt, k)
		}
	}
	return true, nil
}

func (s *CephProvider) GetNodeRole(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext

	node_id_str := req.RpcRequestVars["node-id"]
	node_id, err := uuid.Parse(node_id_str)
	if err != nil {
		logger.Get().Error(
			"%s-Error parsing the node id: %s. error: %v",
			ctxt,
			node_id_str,
			err)
		*resp = utils.WriteResponse(
			http.StatusBadRequest,
			fmt.Sprintf("Error parsing the node id: %s. error: %v",
				node_id_str,
				err))
		return err
	}
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	var node models.Node
	var slus []models.StorageLogicalUnit
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	coll1 := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
	if err := coll.Find(bson.M{"nodeid": *node_id}).One(&node); err != nil {
		logger.Get().Error(
			"%s-Error getting the details of node: %v. error: %v",
			ctxt,
			*node_id,
			err)
		*resp = utils.WriteResponse(
			http.StatusInternalServerError,
			"Error getting node details")
		return err
	}
	if err := coll1.Find(bson.M{"nodeid": *node_id}).All(&slus); err != nil {
		logger.Get().Info(
			"%s-No slus for the node: %v. error: %v",
			ctxt,
			*node_id,
			err)
	}
	var roles []string
	if len(slus) > 0 {
		roles = append(roles, bigfinmodels.NODE_SERVICE_OSD)
	}
	if node.Options[models.Mon] == models.Yes {
		roles = append(roles, bigfinmodels.NODE_SERVICE_MON)
	}
	result, err := json.Marshal(roles)
	if err != nil {
		logger.Get().Error(
			"%s-Error marshalling the node roles data. error: %v",
			ctxt,
			err)
		*resp = utils.WriteResponse(
			http.StatusInternalServerError,
			"Error marshalling node roles data")
		return err
	}
	*resp = utils.WriteResponseWithData(http.StatusOK, "", result)
	return nil
}
