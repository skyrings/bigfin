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
	"net/http"
	"strconv"

	"github.com/skyrings/bigfin/backend/cephapi"
	"github.com/skyrings/bigfin/bigfinmodels"
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/skyring-common/conf"
	"github.com/skyrings/skyring-common/db"
	"github.com/skyrings/skyring-common/models"
	"github.com/skyrings/skyring-common/tools/logger"
	"github.com/skyrings/skyring-common/tools/task"
	"github.com/skyrings/skyring-common/tools/uuid"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	bigfin_conf "github.com/skyrings/bigfin/conf"
	bigfin_task "github.com/skyrings/bigfin/tools/task"
	skyring_util "github.com/skyrings/skyring-common/utils"
)

const (
	DEFAULT_PG_NUM       = 128
	TARGET_PGS_PER_OSD   = 200
	MAX_UTILIZATION_PCNT = 80
)

var ec_pool_sizes = map[string]int{
	"default": 3,
	"k4m2":    6,
	"k6m3":    9,
	"k8m4":    12,
}

var validConfigs = []string{
	"quota_max_objects",
	"quota_max_bytes",
	"pg_num",
}

func (s *CephProvider) CreateStorage(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext
	var request models.AddStorageRequest
	if err := json.Unmarshal(req.RpcRequestData, &request); err != nil {
		logger.Get().Error("%s - Unbale to parse the request. error: %v", ctxt, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request. error: %v", err))
		return err
	}

	// Create the storage pool
	cluster_id_str := req.RpcRequestVars["cluster-id"]
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("%s - Error parsing the cluster id: %s. error: %v", ctxt, cluster_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s", cluster_id_str))
		return err
	}

	asyncTask := func(t *task.Task) {
		sessionCopy := db.GetDatastore().Copy()
		defer sessionCopy.Close()
		for {
			select {
			case <-t.StopCh:
				return
			default:
				var cluster models.Cluster

				t.UpdateStatus("Started ceph provider storage creation: %v", t.ID)

				t.UpdateStatus("Getting cluster details")
				// Get cluster details
				coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
				if err := coll.Find(bson.M{"clusterid": *cluster_id}).One(&cluster); err != nil {
					utils.FailTask(fmt.Sprintf("Error getting the cluster details for :%v", *cluster_id), fmt.Errorf("%s - %v", ctxt, err), t)
					return
				}

				t.UpdateStatus("Getting a mon from cluster")
				monnode, err := GetCalamariMonNode(*cluster_id, ctxt)
				if err != nil {
					utils.FailTask(fmt.Sprintf("Error getting mon node details for cluster: %v", *cluster_id), fmt.Errorf("%s - %v", ctxt, err), t)
					return
				}

				poolId, ok := createPool(ctxt, *cluster_id, request, t)
				if !ok {
					return
				}
				if request.Type != models.STORAGE_TYPE_ERASURE_CODED && len(request.BlockDevices) > 0 {
					createBlockDevices(ctxt, monnode.Hostname, cluster, *poolId, request, t)
				}

				t.UpdateStatus("Syncing SLUs")
				if err := SyncOsdStatus(*cluster_id, ctxt); err != nil {
					utils.FailTask("Error syncing SLUs", err, t)
					return
				}

				initMonitoringRoutines(ctxt, cluster, (*monnode).Hostname, []interface{}{FetchObjectCount})
				_, cStats, err := updateClusterStats(ctxt, cluster, (*monnode).Hostname)
				if err == nil {
					updateStatsToPools(ctxt, cStats, cluster.ClusterId)
				}
				skyring_util.UpdateStorageCountToSummaries(ctxt, cluster)
				UpdateObjectCountToSummaries(ctxt, cluster)
				t.UpdateStatus("Success")
				t.Done(models.TASK_STATUS_SUCCESS)
				return
			}
		}
	}
	if taskId, err := bigfin_task.GetTaskManager().Run(
		bigfin_conf.ProviderName,
		"CEPH-CreateStorage",
		asyncTask,
		nil,
		nil,
		nil); err != nil {
		logger.Get().Error("%s-Task creation failed for create storage %s on cluster: %v. error: %v", ctxt, request.Name, *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, "Task creation failed for storage creation")
		return err
	} else {
		*resp = utils.WriteAsyncResponse(taskId, fmt.Sprintf("Task Created for create storage %s on cluster: %v", request.Name, *cluster_id), []byte{})
	}
	return nil
}

func createBlockDevices(
	ctxt string,
	mon string,
	cluster models.Cluster,
	poolId uuid.UUID,
	request models.AddStorageRequest,
	t *task.Task) {

	t.UpdateStatus("Creating block devices")
	var failedBlkDevices []string
	for _, entry := range request.BlockDevices {
		blockDevice := models.BlockDevice{
			Name:             entry.Name,
			Tags:             entry.Tags,
			ClusterId:        cluster.ClusterId,
			ClusterName:      cluster.Name,
			StorageId:        poolId,
			StorageName:      request.Name,
			Size:             entry.Size,
			SnapshotsEnabled: entry.SnapshotsEnabled,
			// TODO: Populate the schedule ids once schedule created
			// SnapshotScheduleIds = <created schedule ids>
			QuotaEnabled: entry.QuotaEnabled,
			QuotaParams:  entry.QuotaParams,
			Options:      entry.Options,
		}
		if ok := createBlockStorage(
			ctxt,
			mon,
			cluster.ClusterId,
			cluster.Name,
			request.Name,
			blockDevice, t); !ok {
			failedBlkDevices = append(failedBlkDevices, entry.Name)
		}
	}
	if len(failedBlkDevices) > 0 {
		t.UpdateStatus("Block device creation failed for: %v", failedBlkDevices)
	}
}

func createPool(ctxt string, clusterId uuid.UUID, request models.AddStorageRequest, t *task.Task) (*uuid.UUID, bool) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	t.UpdateStatus("Getting cluster details")
	// Get cluster details
	var cluster models.Cluster
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Find(bson.M{"clusterid": clusterId}).One(&cluster); err != nil {
		utils.FailTask(fmt.Sprintf("Error getting the cluster details for :%v", clusterId), fmt.Errorf("%s - %v", ctxt, err), t)
		return nil, false
	}

	t.UpdateStatus("Getting a mon from cluster")
	monnode, err := GetCalamariMonNode(clusterId, ctxt)
	if err != nil {
		utils.FailTask(fmt.Sprintf("Error getting mon node details for cluster: %v", clusterId), fmt.Errorf("%s - %v", ctxt, err), t)
		return nil, false
	}

	t.UpdateStatus("Creating pool")
	// Get quota related details if quota enabled
	// If quota enabled, looks for quota config values
	var quotaMaxObjects int
	var quotaMaxBytes uint64
	if request.QuotaEnabled {
		var err error
		if request.QuotaParams["quota_max_objects"] != "" {
			if quotaMaxObjects, err = strconv.Atoi(request.QuotaParams["quota_max_objects"]); err != nil {
				utils.FailTask(fmt.Sprintf("Error parsing quota config value quota_max_objects for pool %s on cluster: %v", request.Name, clusterId), fmt.Errorf("%s - %v", ctxt, err), t)
				return nil, false
			}
		}
		if request.QuotaParams["quota_max_bytes"] != "" {
			if quotaMaxBytes, err = strconv.ParseUint(request.QuotaParams["quota_max_bytes"], 10, 64); err != nil {
				utils.FailTask(fmt.Sprintf("Error parsing quota config value quota_max_bytes for pool %s on cluster: %v", request.Name, clusterId), fmt.Errorf("%s - %v", ctxt, err), t)
				return nil, false
			}
		}
	}

	// Invoke backend api to create pool
	var pgNum uint
	if request.Options["pgnum"] == "" {
		utils.FailTask("", fmt.Errorf("%s - Pg num not provided", ctxt), t)
		return nil, false
	} else {
		val, _ := strconv.ParseUint(request.Options["pgnum"], 10, 32)
		pgNum = uint(val)
	}
	if request.Type == models.STORAGE_TYPE_ERASURE_CODED {
		ok, err := validECProfile(ctxt, monnode.Hostname, cluster, request.Options["ecprofile"])
		if err != nil {
			utils.FailTask("", fmt.Errorf("%s - Error checking validity of ec profile value. error: %v", ctxt, err), t)
			return nil, false
		}
		if !ok {
			utils.FailTask(
				"",
				fmt.Errorf(
					"%s-Invalid EC profile value: %s passed for pool: %s creation on cluster: %s. error: %v",
					ctxt,
					request.Options["ecprofile"],
					request.Name,
					cluster.Name,
					err),
				t)
			return nil, false
		}
	}
	rulesetmapval, ok := cluster.Options["rulesetmap"]
	if !ok {

		logger.Get().Error("Error getting the ruleset for cluster: %s", cluster.Name)
		utils.FailTask("", fmt.Errorf("%s - Error getting the ruleset for cluster: %s", ctxt, cluster.Name), t)
		return nil, false

	}
	rulesetmap := rulesetmapval.(map[string]interface{})
	rulesetval, ok := rulesetmap[request.Profile]
	if !ok {
		logger.Get().Error("Error getting the ruleset for cluster: %s", cluster.Name)
		return nil, false
	}
	ruleset := rulesetval.(map[string]interface{})

	if request.Type == models.STORAGE_TYPE_ERASURE_CODED {
		// cmd := fmt.Sprintf("ceph --cluster %s osd pool create %s %d %d erasure %s", cluster.Name, request.Name, uint(pgNum), uint(pgNum), request.Options["ecprofile"])
		// ok, _, err = cephapi_backend.ExecCmd(monnode.Hostname, clusterId, cmd, ctxt)
		// time.Sleep(10 * time.Second)
		ok, err = cephapi_backend.CreateECPool(
			request.Name,
			monnode.Hostname,
			cluster.Name,
			uint(pgNum),
			request.Replicas,
			quotaMaxObjects,
			quotaMaxBytes,
			request.Options["ecprofile"],
			ruleset,
			request.Profile,
			ctxt)
	} else {
		ok, err = cephapi_backend.CreatePool(
			request.Name,
			monnode.Hostname,
			cluster.Name,
			uint(pgNum),
			request.Replicas,
			quotaMaxObjects,
			quotaMaxBytes,
			ruleset["rulesetid"].(int),
			ctxt)
	}
	if err == cephapi.ErrTimedOut || err == nil {
		pools, err := cephapi_backend.GetPools(monnode.Hostname, clusterId, ctxt)
		if err != nil {
			utils.FailTask("Error getting created pools", fmt.Errorf("%s - %v", ctxt, err), t)
			return nil, false
		}
		storage_id, err := uuid.New()
		if err != nil {
			utils.FailTask("Error creating id for pool", fmt.Errorf("%s - %v", ctxt, err), t)
			return nil, false
		}
		for _, pool := range pools {
			if request.Name == pool.Name {
				t.UpdateStatus("Perisisting the storage entity")
				var storage models.Storage
				storage.StorageId = *storage_id
				storage.Name = request.Name
				storage.Type = request.Type
				storage.Tags = request.Tags
				storage.ClusterId = clusterId
				storage.Size = request.Size
				storage.Status = models.STORAGE_STATUS_OK
				storage.Replicas = request.Replicas
				storage.Profile = request.Profile
				storage.SnapshotsEnabled = request.SnapshotsEnabled
				// TODO: Populate the schedule ids once schedule created
				// storage.SnapshotScheduleIds = <created schedule ids>
				storage.QuotaEnabled = request.QuotaEnabled
				storage.QuotaParams = request.QuotaParams
				options := make(map[string]string)
				options["id"] = strconv.Itoa(pool.Id)
				options["pg_num"] = strconv.Itoa(pool.PgNum)
				options["pgp_num"] = strconv.Itoa(pool.PgpNum)
				options["full"] = strconv.FormatBool(pool.Full)
				options["hashpspool"] = strconv.FormatBool(pool.HashPsPool)
				options["min_size"] = strconv.FormatUint(pool.MinSize, 10)
				options["crash_replay_interval"] = strconv.Itoa(pool.CrashReplayInterval)
				options["crush_ruleset"] = strconv.Itoa(pool.CrushRuleSet)
				if request.Type == models.STORAGE_TYPE_ERASURE_CODED {
					options["ecprofile"] = request.Options["ecprofile"]
				}
				storage.Options = options

				coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
				if _, err := coll.Upsert(bson.M{"name": storage.Name, "clusterid": storage.ClusterId}, bson.M{"$set": storage}); err != nil {
					utils.FailTask(fmt.Sprintf("Error persisting pool %s for cluster: %s", request.Name, cluster.Name), fmt.Errorf("%s - %v", ctxt, err), t)
					return nil, false
				}
				break
			}
		}
		return storage_id, true
	} else {
		utils.FailTask(fmt.Sprintf("Create pool %s failed on cluster: %s", request.Name, cluster.Name), fmt.Errorf("%s - %v", ctxt, err), t)
		return nil, false
	}
}

func validECProfile(ctxt string, mon string, cluster models.Cluster, ecprofile string) (bool, error) {
	cmd := fmt.Sprintf("ceph osd erasure-code-profile ls --cluster %s --format=json", cluster.Name)
	ok, out, err := cephapi_backend.ExecCmd(mon, cluster.ClusterId, cmd, ctxt)
	var fetchedProfiles []string
	if !ok || err != nil {
		return false, err
	} else {
		if err := json.Unmarshal([]byte(out), &fetchedProfiles); err != nil {
			return false, err
		}
	}
	for _, value := range fetchedProfiles {
		if value == ecprofile {
			return true, nil
		}
	}
	return false, nil
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
func DerivePgNum(clusterId uuid.UUID, size string, replicaCount int, profile string) uint {
	// Get the no of OSDs in the cluster
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
	var slus []models.StorageLogicalUnit
	if err := coll.Find(bson.M{"clusterid": clusterId, "type": models.CEPH_OSD, "storageprofile": profile}).All(&slus); err != nil {
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
	avgOsdSize := avg_osd_size(slus) / 1024
	maxAllocSize := (avgOsdSize * float64(len(slus)) / float64(replicaCount)) * float64(MAX_UTILIZATION_PCNT) / 100
	pcntData := float64(utils.SizeFromStr(size)) / float64(maxAllocSize)
	pgnum := float64(float64(TARGET_PGS_PER_OSD)*float64(len(slus))*pcntData) / float64(replicaCount)
	derivedPgNum := utils.NextTwosPower(uint(pgnum))
	if derivedPgNum < uint(osdsNum) {
		// Consider next 2's power value
		newPgNum := osdsNum / replicaCount
		derivedPgNum = utils.NextTwosPower(uint(newPgNum))
	}
	return derivedPgNum
}

func avg_osd_size(slus []models.StorageLogicalUnit) float64 {
	var totalOsdSize float64
	for _, slu := range slus {
		totalOsdSize += slu.StorageDeviceSize
	}
	return totalOsdSize / float64(len(slus))
}

func (s *CephProvider) GetStorages(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext
	cluster_id_str := req.RpcRequestVars["cluster-id"]
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("%s-Error parsing the cluster id: %s. error: %v", ctxt, cluster_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s", cluster_id_str))
		return err
	}
	monnode, err := GetCalamariMonNode(*cluster_id, ctxt)
	if err != nil {
		logger.Get().Error("%s-Error getting a mon node in cluster: %v. error: %v", ctxt, *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error getting a mon node in cluster. error: %v", err))
		return err
	}

	// Get cluster details
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	var cluster models.Cluster
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Find(bson.M{"clusterid": *cluster_id}).One(&cluster); err != nil {
		logger.Get().Error("%s-Error getting details for cluster: %v. error: %v", ctxt, *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error getting cluster details. error: %v", err))
		return err
	}

	// Get the pools for the cluster
	pools, err := cephapi_backend.GetPools(monnode.Hostname, *cluster_id, ctxt)
	if err != nil {
		logger.Get().Error("%s-Error getting storages for cluster: %s. error: %v", ctxt, cluster.Name, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error getting storages. error: %v", err))
		return err
	}
	var storages []models.AddStorageRequest
	for _, pool := range pools {
		storage := models.AddStorageRequest{
			Name:     pool.Name,
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
		options["pg_num"] = strconv.Itoa(pool.PgNum)
		options["pgp_num"] = strconv.Itoa(pool.PgpNum)
		options["full"] = strconv.FormatBool(pool.Full)
		options["hashpspool"] = strconv.FormatBool(pool.HashPsPool)
		options["min_size"] = strconv.FormatUint(pool.MinSize, 10)
		options["crash_replay_interval"] = strconv.Itoa(pool.CrashReplayInterval)
		options["crush_ruleset"] = strconv.Itoa(pool.CrushRuleSet)
		// Get EC profile details of pool
		ok, out, err := cephapi_backend.ExecCmd(
			monnode.Hostname,
			*cluster_id,
			fmt.Sprintf("ceph --cluster %s osd pool get %s erasure_code_profile --format=json", cluster.Name, pool.Name),
			ctxt)
		if err != nil || !ok {
			storage.Type = models.STORAGE_TYPE_REPLICATED
			logger.Get().Warning("%s-Error getting EC profile details of pool: %s of cluster: %s", ctxt, pool.Name, cluster.Name)
		} else {
			var ecprofileDet bigfinmodels.ECProfileDet
			if err := json.Unmarshal([]byte(out), &ecprofileDet); err != nil {
				logger.Get().Warning("%s-Error parsing EC profile details of pool: %s of cluster: %s", ctxt, pool.Name, cluster.Name)
			} else {
				storage.Type = models.STORAGE_TYPE_ERASURE_CODED
				options["ecprofile"] = ecprofileDet.ECProfile
			}
		}
		storage.Options = options
		storages = append(storages, storage)
	}
	result, err := json.Marshal(storages)
	if err != nil {
		logger.Get().Error("%s-Error forming the output for storage list for cluster: %s. error: %v", ctxt, cluster.Name, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error forming the output. error: %v", err))
		return err
	}
	*resp = utils.WriteResponseWithData(http.StatusOK, "", result)
	return nil
}

func (s *CephProvider) RemoveStorage(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext
	cluster_id_str := req.RpcRequestVars["cluster-id"]
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("%s - Error parsing the cluster id: %s. error: %v", ctxt, cluster_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s", cluster_id_str))
		return err
	}
	storage_id_str := req.RpcRequestVars["storage-id"]
	storage_id, err := uuid.Parse(storage_id_str)
	if err != nil {
		logger.Get().Error("%s - Error parsing the storage id: %s. error: %v", ctxt, storage_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the storage id: %s", storage_id_str))
		return err
	}

	asyncTask := func(t *task.Task) {
		sessionCopy := db.GetDatastore().Copy()
		defer sessionCopy.Close()
		for {
			select {
			case <-t.StopCh:
				return
			default:
				t.UpdateStatus("Started ceph provider pool deletion: %v", t.ID)
				// Get the storage details
				var storage models.Storage
				var cluster models.Cluster

				t.UpdateStatus("Getting details of cluster")
				coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
				if err := coll.Find(bson.M{"clusterid": *cluster_id}).One(&cluster); err != nil {
					utils.FailTask("Error getting details of cluster", fmt.Errorf("%s - %v", ctxt, err), t)
					return
				}

				t.UpdateStatus("Getting details of storage")
				coll1 := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
				if err := coll1.Find(bson.M{"clusterid": *cluster_id, "storageid": *storage_id}).One(&storage); err != nil {
					utils.FailTask("Error getting details of storage", fmt.Errorf("%s - %v", ctxt, err), t)
					return
				}

				t.UpdateStatus("Getting a mon from cluster")
				monnode, err := GetCalamariMonNode(*cluster_id, ctxt)
				if err != nil {
					utils.FailTask("Error getting a mon node for cluster", fmt.Errorf("%s - %v", ctxt, err), t)
					return
				}

				poolId, err := strconv.Atoi(storage.Options["id"])
				if err != nil {
					utils.FailTask("Error getting id of storage", fmt.Errorf("%s - %v", ctxt, err), t)
					return
				}

				t.UpdateStatus("Deleting storage")
				ok, err := cephapi_backend.RemovePool(monnode.Hostname, *cluster_id, cluster.Name, storage.Name, poolId, ctxt)
				if err != nil || !ok {
					utils.FailTask(fmt.Sprintf("Deletion of storage %v failed on cluster: %s", *storage_id, *cluster_id), fmt.Errorf("%s - %v", ctxt, err), t)
					return
				} else {
					t.UpdateStatus("Removing the block devices (if any) for storage entoty")
					coll2 := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_BLOCK_DEVICES)
					if _, err := coll2.RemoveAll(bson.M{"clusterid": *cluster_id, "storageid": *storage_id}); err != nil {
						utils.FailTask(fmt.Sprintf("Error removing block devices for storage %v from DB for cluster: %d", *storage_id, *cluster_id), fmt.Errorf("%s - %v", ctxt, err), t)
						return
					}
					t.UpdateStatus("Removing the storage entity from DB")
					if err := coll1.Remove(bson.M{"clusterid": *cluster_id, "storageid": *storage_id}); err != nil {
						utils.FailTask(fmt.Sprintf("Error removing storage entity from DB for cluster: %d", *cluster_id), fmt.Errorf("%s - %v", ctxt, err), t)
						return
					}
				}

				t.UpdateStatus("Syncing SLUs")
				if err := SyncOsdStatus(*cluster_id, ctxt); err != nil {
					utils.FailTask("Error syncing SLUs", err, t)
					return
				}

				skyring_util.UpdateStorageCountToSummaries(ctxt, cluster)
				UpdateObjectCountToSummaries(ctxt, cluster)

				t.UpdateStatus("Success")
				t.Done(models.TASK_STATUS_SUCCESS)
				return
			}
		}
	}
	if taskId, err := bigfin_task.GetTaskManager().Run(
		bigfin_conf.ProviderName,
		"CEPH-DeleteStorage",
		asyncTask,
		nil,
		nil,
		nil); err != nil {
		logger.Get().Error("%s-Task creation failed for delete storage on cluster: %v. error: %v", ctxt, *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, "Task creation failed for storage deletion")
		return err
	} else {
		*resp = utils.WriteAsyncResponse(taskId, fmt.Sprintf("Task Created for delete storage on cluster: %v", *cluster_id), []byte{})
	}
	return nil
}

func (s *CephProvider) UpdateStorage(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext
	cluster_id_str := req.RpcRequestVars["cluster-id"]
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error(
			"%s - Error parsing the cluster id: %s. error: %v",
			ctxt,
			cluster_id_str,
			err)
		*resp = utils.WriteResponse(
			http.StatusBadRequest,
			fmt.Sprintf(
				"Error parsing the cluster id: %s",
				cluster_id_str))
		return err
	}
	storage_id_str := req.RpcRequestVars["storage-id"]
	storage_id, err := uuid.Parse(storage_id_str)
	if err != nil {
		logger.Get().Error(
			"%s - Error parsing the storage id: %s. error: %v",
			ctxt,
			storage_id_str,
			err)
		*resp = utils.WriteResponse(
			http.StatusBadRequest,
			fmt.Sprintf(
				"Error parsing the storage id: %s",
				storage_id_str))
		return err
	}
	var request models.AddStorageRequest
	if err := json.Unmarshal(req.RpcRequestData, &request); err != nil {
		logger.Get().Error(
			"%s - Unbale to parse the request. error: %v",
			ctxt,
			err)
		*resp = utils.WriteResponse(
			http.StatusBadRequest,
			fmt.Sprintf(
				"Unbale to parse the request. error: %v",
				err))
		return err
	}

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
	var storage models.Storage
	if err := coll.Find(bson.M{"storageid": *storage_id}).One(&storage); err != nil {
		logger.Get().Error(
			"%s - Error getting detals of storage: %v on cluster: %v. error: %v",
			ctxt,
			*storage_id,
			*cluster_id,
			err)
		*resp = utils.WriteResponse(
			http.StatusInternalServerError,
			fmt.Sprintf(
				"Error getting the details of storage: %v",
				*storage_id))
		return err
	}
	id, err := strconv.Atoi(storage.Options["id"])
	if err != nil {
		logger.Get().Error(
			"%s - Error getting id of the pool: %v of cluster: %v. error: %v",
			ctxt,
			*storage_id,
			*cluster_id,
			err)
		*resp = utils.WriteResponse(
			http.StatusInternalServerError,
			fmt.Sprintf(
				"Error getting id of the pool: %v",
				*storage_id))
		return err
	}
	asyncTask := func(t *task.Task) {
		sessionCopy := db.GetDatastore().Copy()
		defer sessionCopy.Close()
		coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
		for {
			select {
			case <-t.StopCh:
				return
			default:
				t.UpdateStatus("Started ceph provider pool updation: %v", t.ID)
				if request.Name != "" && (request.Replicas != 0 || len(request.Options) != 0) {
					utils.FailTask(
						fmt.Sprintf(
							"Invalid mix of fields to update for storage: %v of cluster: %v. "+
								"Name change cannot be mixed with other changes.",
							*storage_id,
							*cluster_id),
						fmt.Errorf("%s-Invalid mix of fields to update", ctxt),
						t)
					return
				}
				for key := range request.Options {
					if ok := skyring_util.StringInSlice(key, validConfigs); !ok {
						utils.FailTask(
							fmt.Sprintf(
								"Invalid configuration: %s mentioned for storage: %v of cluster: %v",
								key,
								*storage_id,
								*cluster_id),
							fmt.Errorf("%s-%v", ctxt, err),
							t)
						return
					}
				}
				t.UpdateStatus("Getting a radom mon from cluster")
				monnode, err := GetCalamariMonNode(*cluster_id, ctxt)
				if err != nil {
					utils.FailTask(
						fmt.Sprintf(
							"Error getting mon node from cluster: %v",
							*cluster_id),
						fmt.Errorf("%s-%v", ctxt, err),
						t)
					return
				}
				var updatedFields = make(map[string]interface{})
				if request.Name != "" {
					updatedFields["name"] = request.Name
				}
				if request.Replicas != 0 {
					updatedFields["size"] = request.Replicas
				}
				if request.QuotaEnabled {
					for key, value := range request.QuotaParams {
						reqVal, _ := strconv.ParseUint(value, 10, 64)
						updatedFields[key] = uint64(reqVal)
					}
				} else {
					if request.QuotaParams["quota_max_objects"] == "0" && request.QuotaParams["quota_max_bytes"] == "0" {
						updatedFields["quota_max_objects"] = 0
						updatedFields["quota_max_bytes"] = 0
					}
				}
				for key, value := range request.Options {
					reqVal, _ := strconv.ParseUint(value, 10, 32)
					updatedFields[key] = uint(reqVal)
				}
				t.UpdateStatus("Updating pool details")
				ok, err := cephapi_backend.UpdatePool(
					monnode.Hostname,
					*cluster_id, id,
					updatedFields,
					ctxt)
				if err != nil || !ok {
					utils.FailTask(
						fmt.Sprintf(
							"Error setting the configurations for storage: %v on cluster: %v",
							*storage_id,
							*cluster_id),
						fmt.Errorf("%s-%v", ctxt, err),
						t)
					return
				}
				var filter bson.M = make(map[string]interface{})
				var updates bson.M = make(map[string]interface{})
				filter["storageid"] = *storage_id
				filter["clusterid"] = *cluster_id
				if request.Name != "" {
					updates["name"] = request.Name
				}
				if request.Replicas != 0 {
					updates["replicas"] = request.Replicas
				}
				if request.QuotaEnabled {
					updates["quotaenabled"] = true
					params := make(map[string]string)
					for key, value := range request.QuotaParams {
						params[key] = string(value)
					}
					updates["quotaparams"] = params
				} else {
					if request.QuotaParams["quota_max_objects"] == "0" && request.QuotaParams["quota_max_bytes"] == "0" {
						updates["quotaenabled"] = false
						updates["quotaparams"] = map[string]string{}
					}
				}

				if value, ok := request.Options["pg_num"]; ok {
					updates["options.pgp_num"] = value
				}
				t.UpdateStatus("Persisting pool updates in DB")
				if err := coll.Update(filter, bson.M{"$set": updates}); err != nil {
					utils.FailTask(
						fmt.Sprintf(
							"Error updating storage entity: %v of cluster: %v",
							*storage_id,
							*cluster_id),
						fmt.Errorf("%s-%v", ctxt, err),
						t)
				}

				cluster, err := getCluster(*cluster_id)
				if err != nil {
					logger.Get().Error("%s - Failed to get details of cluster: %s. error: %v", ctxt, *cluster_id, err)
				} else {
					initMonitoringRoutines(ctxt, cluster, (*monnode).Hostname, []interface{}{FetchOSDStats})
					UpdatePgNumToSummaries(cluster, ctxt)
				}

				if _, ok := updates["name"]; ok {
					coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_BLOCK_DEVICES)
					if _, err := coll.UpdateAll(filter, bson.M{"$set": bson.M{"storagename": updates["name"]}}); err != nil && err != mgo.ErrNotFound {
						utils.FailTask(
							fmt.Sprintf(
								"Storage name has changed for storage:%v. Error while updating this info for RBDs in cluster:%v",
								*storage_id,
								*cluster_id),
							fmt.Errorf("%s-%v", ctxt, err),
							t)
					}
				}
				t.UpdateStatus("Success")
				t.Done(models.TASK_STATUS_SUCCESS)
				return
			}
		}
	}
	if taskId, err := bigfin_task.GetTaskManager().Run(
		bigfin_conf.ProviderName,
		"CEPH-UpdateStorage",
		asyncTask,
		nil,
		nil,
		nil); err != nil {
		logger.Get().Error(
			"%s-Task creation failed for update storage on cluster: %v. error: %v",
			ctxt,
			*cluster_id,
			err)
		*resp = utils.WriteResponse(
			http.StatusInternalServerError,
			"Task creation failed for storage update")
		return err
	} else {
		*resp = utils.WriteAsyncResponse(
			taskId,
			fmt.Sprintf(
				"Task Created for update storage on cluster: %v",
				*cluster_id),
			[]byte{})
	}
	return nil
}
