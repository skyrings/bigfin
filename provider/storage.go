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
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/skyring/conf"
	"github.com/skyrings/skyring/db"
	"github.com/skyrings/skyring/models"
	"github.com/skyrings/skyring/tools/task"
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

	asyncTask := func(t *task.Task) {
		t.UpdateStatus("Statrted ceph provider pool creation: %v", t.ID)
		ok, err := createPool(*cluster_id, request, t)
		if err != nil || !ok {
			t.UpdateStatus("Failed to create pool: %v", err)
			return
		}

		t.UpdateStatus("Perisisting the storage entity")
		// Add storage entity to DB
		var storage models.Storage
		storage_id, err := uuid.New()
		if err != nil {
			t.UpdateStatus("Failed to create uuid for storage entity")
			return
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
			t.UpdateStatus("Error persisting the storage entoty")
			return
		}
		t.UpdateStatus("Success")
		t.Done()
	}
	if taskId, err := s.GetTaskManager().Run("CEPH-CreateStorage", asyncTask); err != nil {
		*resp = utils.WriteResponse(http.StatusInternalServerError, "Task creation failed for storage creation")
		return err
	} else {
		*resp = utils.WriteAsyncResponse(taskId, "Task Created", []byte{})
	}
	return nil
}

func createPool(clusterId uuid.UUID, request models.AddStorageRequest, t *task.Task) (bool, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	t.UpdateStatus("Getting cluster details")
	// Get cluster details
	var cluster models.Cluster
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Find(bson.M{"clusterid": clusterId}).One(&cluster); err != nil {
		t.UpdateStatus("Error getting the cluster details")
		return false, err
	}

	t.UpdateStatus("Getting mons for cluster")
	// Get the mons
	var mons models.Nodes
	var clusterNodes models.Nodes
	coll = sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	if err := coll.Find(bson.M{"clusterid": clusterId}).All(&clusterNodes); err != nil {
		t.UpdateStatus("Failed to get mons list for cluster")
		return false, err
	}
	for _, clusterNode := range clusterNodes {
		for k, v := range clusterNode.Options {
			if k == "mon" && v == "Y" {
				mons = append(mons, clusterNode)
			}
		}
	}
	if len(mons) <= 0 {
		t.UpdateStatus("No mons found for cluster")
		return false, errors.New("No mons available")
	}

	t.UpdateStatus("Getting mon node details")
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
		t.UpdateStatus("Error getting mon node details")
		return false, err
	}

	t.UpdateStatus("Creating pool")
	// Invoke backend api to create pool
	var pgNum int
	if request.Options["pgnum"] != "" {
		pgNum, _ = strconv.Atoi(request.Options["pgnum"])
	} else {
		pgNum = 0
	}
	ok, err := cephapi_backend.CreatePool(request.Name, monnode.Hostname, cluster.Name, uint(pgNum))
	if err != nil || !ok {
		return false, err
	} else {
		return true, nil
	}
}
