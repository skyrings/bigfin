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
	"github.com/skyrings/bigfin/tools/logger"
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/skyring/conf"
	"github.com/skyrings/skyring/db"
	"github.com/skyrings/skyring/models"
	"github.com/skyrings/skyring/tools/task"
	"github.com/skyrings/skyring/tools/uuid"
	"gopkg.in/mgo.v2/bson"
	"net/http"
	"strconv"

	bigfin_task "github.com/skyrings/bigfin/tools/task"
)

const (
	DEFAULT_PG_NUM       = 128
	TARGET_PGS_PER_OSD   = 200
	MAX_UTILIZATION_PCNT = 80
)

func (s *CephProvider) CreateStorage(req models.RpcRequest, resp *models.RpcResponse) error {
	var request models.AddStorageRequest

	if err := json.Unmarshal(req.RpcRequestData, &request); err != nil {
		logger.Get().Error("Unbale to parse the request. error: %v", err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request. error: %v", err))
		return err
	}

	// Create the storage pool
	cluster_id_str := req.RpcRequestVars["cluster-id"]
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("Error parsing the cluster id: %s. error: %v", cluster_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s", cluster_id_str))
		return err
	}

	asyncTask := func(t *task.Task) {
		t.UpdateStatus("Statrted ceph provider pool creation: %v", t.ID)
		ok, err := createPool(*cluster_id, request, t)
		if err != nil || !ok {
			utils.FailTask("Create Pool failed", err, t)
			return
		}

		t.UpdateStatus("Perisisting the storage entity")
		// Add storage entity to DB
		var storage models.Storage
		storage_id, err := uuid.New()
		if err != nil {
			utils.FailTask("Error creating id for node", err, t)
			return
		}
		storage.StorageId = *storage_id
		storage.Name = request.Name
		storage.Type = request.Type
		storage.Tags = request.Tags
		storage.ClusterId = *cluster_id
		storage.Size = request.Size
		storage.Status = models.STATUS_UP
		storage.Replicas = request.Replicas
		storage.Profile = request.Profile
		storage.SnapshotsEnabled = request.SnapshotsEnabled
		// TODO: Populate the schedule ids once schedule created
		// storage.SnapshotScheduleIds = <created schedule ids>
		storage.QuotaEnabled = request.QuotaEnabled
		storage.QuotaParams = request.QuotaParams
		storage.Options = request.Options
		sessionCopy := db.GetDatastore().Copy()
		defer sessionCopy.Close()
		coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
		if err := coll.Insert(storage); err != nil {
			utils.FailTask("Error persisting pool", err, t)
			return
		}
		t.UpdateStatus("Success")
		t.Done(models.TASK_STATUS_SUCCESS)
	}
	if taskId, err := bigfin_task.GetTaskManager().Run("CEPH-CreateStorage", asyncTask, nil, nil, nil); err != nil {
		logger.Get().Error("Task creation failed for storage creation. error: %v", err)
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

	t.UpdateStatus("Getting a mon from cluster")
	monnode, err := GetRandomMon(clusterId)
	if err != nil {
		t.UpdateStatus("Error getting mon node details")
		return false, err
	}

	t.UpdateStatus("Creating pool")
	// Invoke backend api to create pool
	var pgNum uint
	if request.Options["pgnum"] != "" {
		val, _ := strconv.ParseUint(request.Options["pgnum"], 10, 32)
		pgNum = uint(val)
	} else {
		pgNum = derive_pgnum(clusterId, request.Size, request.Replicas)
	}

	// Get quota related details if quota enabled
	// If quota enabled, looks for quota config values
	var quotaMaxObjects int
	var quotaMaxBytes uint64
	if request.QuotaEnabled {
		var err error
		if request.QuotaParams["quota_max_objects"] != "" {
			if quotaMaxObjects, err = strconv.Atoi(request.QuotaParams["quota_max_objects"]); err != nil {
				return false, errors.New(fmt.Sprintf("Error parsing quota config value for quota_max_objects"))
			}
		}
		if request.QuotaParams["quota_max_bytes"] != "" {
			if quotaMaxBytes, err = strconv.ParseUint(request.QuotaParams["quota_max_bytes"], 10, 64); err != nil {
				return false, errors.New(fmt.Sprintf("Error parsing quota config value for quota_max_bytes"))
			}
		}
	}

	ok, err := cephapi_backend.CreatePool(request.Name, monnode.Hostname, cluster.Name, uint(pgNum), request.Replicas, quotaMaxObjects, quotaMaxBytes)
	if err != nil || !ok {
		return false, err
	} else {
		return true, nil
	}
}

// RULES FOR DERIVING THE PG NUM
// if no of osds <= 5 then
//   no of PGs = 128
// if no of osds > 5 and <= 10 then
//   no of PGs = 512
// if no of osds > 10 and <= 50 then
//   no of PGs = 4096
// if no of osds > 50 then
//   no of PGs = (Avg Target PGs per OSD * No of OSDs * Data Percentage) / Replica Count
//   -- where
//      Data Percentage = Target Allocation Size / Max Allocation Size
//      -- where
//         Target Allocation Size - provided in request
//         Max Allocation Size = ((Average OSD Size * No of OSDs) / Replica Count) * Max Utilization Factor
//         -- where Max Utilization Factor is set as 0.8
//  Finally round this value of next 2's power
func derive_pgnum(clusterId uuid.UUID, size string, replicaCount int) uint {
	// Get the no of OSDs in the cluster
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
	var slus []models.StorageLogicalUnit
	if err := coll.Find(bson.M{"clusterid": clusterId, "type": models.CEPH_OSD}).All(&slus); err != nil {
		return uint(DEFAULT_PG_NUM)
	}
	osdsNum := len(slus)

	// Calculate the pgnum value
	if osdsNum <= 5 {
		return uint(DEFAULT_PG_NUM)
	}
	if osdsNum <= 10 {
		return uint(512)
	}
	if osdsNum <= 50 {
		return uint(4096)
	}
	avgOsdSize := avg_osd_size(slus)
	maxAllocSize := (avgOsdSize * uint64(len(slus)) / uint64(replicaCount)) * uint64(MAX_UTILIZATION_PCNT) / 100
	pcntData := utils.SizeFromStr(size) / maxAllocSize
	pgnum := (uint64(TARGET_PGS_PER_OSD) * uint64(len(slus)) * uint64(pcntData)) / uint64(replicaCount)
	return utils.NextTwosPower(uint(pgnum))
}

func avg_osd_size(slus []models.StorageLogicalUnit) uint64 {
	var totalOsdSize uint64
	for _, slu := range slus {
		totalOsdSize += slu.StorageDeviceSize
	}
	return totalOsdSize / uint64(len(slus))
}

func (s *CephProvider) GetStorages(req models.RpcRequest, resp *models.RpcResponse) error {
	cluster_id_str := req.RpcRequestVars["cluster-id"]
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("Error parsing the cluster id: %s. error: %v", cluster_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s", cluster_id_str))
		return err
	}
	monnode, err := GetRandomMon(*cluster_id)
	if err != nil {
		logger.Get().Error("Error getting a mon node in cluster. error: %v", err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error getting a mon node in cluster. error: %v", err))
		return err
	}

	// Get cluster details
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	var cluster models.Cluster
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Find(bson.M{"clusterid": *cluster_id}).One(&cluster); err != nil {
		logger.Get().Error("Error getting cluster details. error: %v", err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error getting cluster details. error: %v", err))
		return err
	}

	// Get the pools for the cluster
	pools, err := cephapi_backend.GetPools(monnode.Hostname, cluster.Name)
	if err != nil {
		logger.Get().Error("Error getting storages. error: %v", err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error getting storages. error: %v", err))
		return err
	}
	var storages []models.AddStorageRequest
	for _, pool := range pools {
		storage := models.AddStorageRequest{
			Name:     pool.Name,
			Type:     "replicated",
			Replicas: pool.Size,
		}
		if pool.QuotaMaxObjects != 0 && pool.QuotaMaxBytes != 0 {
			storage.QuotaEnabled = true
			quotaParams := make(map[string]string)
			quotaParams["quota_max_objects"] = strconv.Itoa(pool.QuotaMaxObjects)
			quotaParams["quota_max_bytes"] = strconv.FormatUint(pool.QuotaMaxBytes, 10)
			storage.QuotaParams = quotaParams
		}
		options := make(map[string]string)
		options["id"] = strconv.Itoa(pool.Id)
		options["pgnum"] = strconv.Itoa(pool.PgNum)
		options["pgp_num"] = strconv.Itoa(pool.PgpNum)
		options["full"] = strconv.FormatBool(pool.Full)
		options["hashpspool"] = strconv.FormatBool(pool.HashPsPool)
		options["min_size"] = strconv.FormatUint(pool.MinSize, 10)
		options["crash_replay_interval"] = strconv.Itoa(pool.CrashReplayInterval)
		options["crush_ruleset"] = strconv.Itoa(pool.CrushRuleSet)
		storage.Options = options
		storages = append(storages, storage)
	}
	result, err := json.Marshal(storages)
	if err != nil {
		logger.Get().Error("Error forming the output. error: %v", err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error forming the output. error: %v", err))
		return err
	}
	*resp = utils.WriteResponseWithData(http.StatusOK, "", result)
	return nil
}
