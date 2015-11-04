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
	"fmt"
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/skyring/conf"
	"github.com/skyrings/skyring/db"
	"github.com/skyrings/skyring/models"
	"github.com/skyrings/skyring/tools/uuid"
	"gopkg.in/mgo.v2/bson"
	"net/http"
	"strconv"
)

func (s *CephProvider) CreateStorage(req models.RpcRequest, resp *models.RpcResponse) error {
	var request models.AddStorageRequest

	if err := json.Unmarshal(req.RpcRequestData, &request); err != nil {
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request %v", err))
		return err
	}

	// Create the storage pool
	cluster_id_str := req.RpcRequestVars["cluster-id"]
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s", cluster_id_str))
		return err
	}
	ok, err := createPool(*cluster_id, request)
	if err != nil || !ok {
		*resp = utils.WriteResponse(http.StatusInternalServerError, err.Error())
		return err
	}

	// Add storage entity to DB
	var storage models.Storage
	storage_id, err := uuid.New()
	if err != nil {
		*resp = utils.WriteResponse(http.StatusInternalServerError, "Error creating storage id")
		return err
	}
	storage.StorageId = *storage_id
	storage.Name = request.Name
	storage.Type = request.Type
	storage.Tags = request.Tags
	storage.ClusterId = *cluster_id
	storage.Size = request.Size
	storage.Status = models.STATUS_UP
	storage.Options = request.Options
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
	if err := coll.Insert(storage); err != nil {
		*resp = utils.WriteResponse(http.StatusInternalServerError, err.Error())
		return err
	}

	// Send response
	*resp = utils.WriteResponse(http.StatusOK, "Done")
	return nil
}

func createPool(clusterId uuid.UUID, request models.AddStorageRequest) (bool, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	// Get cluster details
	var cluster models.Cluster
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Find(bson.M{"clusterid": clusterId}).One(&cluster); err != nil {
		return false, err
	}

	// Get the mons
	var slus []models.StorageLogicalUnit
	coll = sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
	if err := coll.Find(bson.M{"clusterid": clusterId, "type": models.CEPH_MON}).All(&slus); err != nil {
		return false, err
	}

	// Pick a random mon from the list
	var monNodeId uuid.UUID
	if len(slus) == 1 {
		monNodeId = slus[0].NodeId
	} else {
		randomIndex := utils.RandomNum(0, len(slus)-1)
		monNodeId = slus[randomIndex].NodeId
	}
	var monnode models.Node
	coll = sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	if err := coll.Find(bson.M{"nodeid": monNodeId}).One(&monnode); err != nil {
		return false, err
	}

	// Invoke backend api to create pool
	// TODO: Later once ceph apis are ready, those should be used instead of salt wrapper apis
	var pgNum int
	if request.Options["pgnum"] != "" {
		pgNum, _ = strconv.Atoi(request.Options["pgnum"])
	} else {
		pgNum = 0
	}
	ok, err := salt_backend.CreatePool(request.Name, monnode.Hostname, cluster.Name, uint(pgNum))
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	} else {
		return false, nil
	}
}
