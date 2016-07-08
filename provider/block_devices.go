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
	"github.com/skyrings/skyring-common/conf"
	"github.com/skyrings/skyring-common/db"
	"github.com/skyrings/skyring-common/models"
	"github.com/skyrings/skyring-common/tools/logger"
	"github.com/skyrings/skyring-common/tools/task"
	"github.com/skyrings/skyring-common/tools/uuid"
	"gopkg.in/mgo.v2/bson"
	"net/http"

	bigfin_conf "github.com/skyrings/bigfin/conf"
	bigfin_task "github.com/skyrings/bigfin/tools/task"
)

func (s *CephProvider) CreateBlockDevice(req models.RpcRequest, resp *models.RpcResponse) error {
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
	var request models.AddStorageBlockDeviceRequest
	if err := json.Unmarshal(req.RpcRequestData, &request); err != nil {
		logger.Get().Error("%s - Unbale to parse the request. error: %v", ctxt, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request. error: %v", err))
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
				var storage models.Storage
				t.UpdateStatus("Started ceph provider block device creation: %v", t.ID)

				t.UpdateStatus("Getting cluster details")
				// Get cluster details
				coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
				if err := coll.Find(bson.M{"clusterid": *cluster_id}).One(&cluster); err != nil {
					utils.FailTask(fmt.Sprintf("Error getting the cluster details for :%v", *cluster_id), fmt.Errorf("%s - %v", ctxt, err), t)
					return
				}

				t.UpdateStatus("Getting details of storage entity")
				coll1 := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
				if err := coll1.Find(bson.M{"storageid": *storage_id}).One(&storage); err != nil {
					utils.FailTask("Error getting details of storage entity", fmt.Errorf("%s - %v", ctxt, err), t)
					return
				}

				t.UpdateStatus("Getting a mon from cluster")
				monnode, err := GetCalamariMonNode(*cluster_id, ctxt)
				if err != nil {
					utils.FailTask(fmt.Sprintf("Error getting mon node details for cluster: %v", *cluster_id), fmt.Errorf("%s - %v", ctxt, err), t)
					return
				}

				t.UpdateStatus("Creating the block device")
				blockDevice := models.BlockDevice{
					Name:             request.Name,
					Tags:             request.Tags,
					ClusterId:        *cluster_id,
					ClusterName:      cluster.Name,
					StorageId:        *storage_id,
					StorageName:      storage.Name,
					Size:             request.Size,
					SnapshotsEnabled: request.SnapshotsEnabled,
					// TODO: Populate the schedule ids once schedule created
					// storage.SnapshotScheduleIds = <created schedule ids>
					QuotaEnabled: request.QuotaEnabled,
					QuotaParams:  request.QuotaParams,
					Options:      request.Options,
				}
				if ok := createBlockStorage(ctxt, monnode.Hostname, *cluster_id, cluster.Name, storage.Name, blockDevice, t); !ok {
					utils.FailTask("Error creating block device", fmt.Errorf("%s - %v", ctxt, err), t)
					return
				}
				t.UpdateStatus("Success")
				t.Done(models.TASK_STATUS_SUCCESS)
				return
			}
		}
	}
	if taskId, err := bigfin_task.GetTaskManager().Run(
		bigfin_conf.ProviderName,
		"CEPH-CreateBlockDevice",
		asyncTask,
		nil,
		nil,
		nil); err != nil {
		logger.Get().Error("%s - Task creation failed for create block device %s on cluster: %v. error: %v", ctxt, request.Name, *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, "Task creation failed for block device creation")
		return err
	} else {
		*resp = utils.WriteAsyncResponse(taskId, fmt.Sprintf("Task Created for create block device %s on cluster: %v", request.Name, *cluster_id), []byte{})
	}

	return nil
}

func createBlockStorage(
	ctxt string,
	mon string,
	clusterId uuid.UUID,
	clusterName string,
	backingStorage string,
	blockDevice models.BlockDevice,
	t *task.Task) bool {

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_BLOCK_DEVICES)

	// Create the block device image
	sizeMBs := utils.SizeFromStr(blockDevice.Size) / 1024
	cmd := fmt.Sprintf("rbd create %s --cluster %s --size %d --pool %s", blockDevice.Name, clusterName, sizeMBs, backingStorage)
	ok, _, err := cephapi_backend.ExecCmd(mon, clusterId, cmd, ctxt)
	if err != nil || !ok {
		utils.FailTask(fmt.Sprintf("Creation of block device failed on cluster: %s", clusterName), fmt.Errorf("%s - %v", ctxt, err), t)
		return false
	} else {
		t.UpdateStatus("Perisisting the block device entity")
		blockedevice_id, err := uuid.New()
		if err != nil {
			utils.FailTask("Error creating id for block device", fmt.Errorf("%s - %v", ctxt, err), t)
			return false
		}
		blockDevice.Id = *blockedevice_id
		if err := coll.Insert(blockDevice); err != nil {
			utils.FailTask(fmt.Sprintf("Error persisting block device entity for cluster: %s", clusterName), fmt.Errorf("%s - %v", ctxt, err), t)
			return false
		}
		cluster, err := getCluster(clusterId)
		if err != nil {
			logger.Get().Error("Failed to get details of cluster: %s. error: %v", clusterId, err)
		} else {
			initMonitoringRoutines(ctxt, cluster, mon, []interface{}{FetchRBDStats, FetchObjectCount})
			UpdateObjectCountToSummaries(ctxt, cluster)
		}
	}

	return true
}

func (s *CephProvider) DeleteBlockDevice(req models.RpcRequest, resp *models.RpcResponse) error {
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
	blockdevice_id_str := req.RpcRequestVars["blockdevice-id"]
	blockdevice_id, err := uuid.Parse(blockdevice_id_str)
	if err != nil {
		logger.Get().Error("%s - Error parsing the block device id: %s. error: %v", ctxt, blockdevice_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the block device id: %s", blockdevice_id_str))
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
				var storage models.Storage
				var blockDevice models.BlockDevice

				t.UpdateStatus("Started ceph provider block device deletion: %v", t.ID)
				t.UpdateStatus("Getting details of cluster")
				coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
				if err := coll.Find(bson.M{"clusterid": *cluster_id}).One(&cluster); err != nil {
					utils.FailTask("Error getting details of cluster", fmt.Errorf("%s - %v", ctxt, err), t)
					return
				}
				t.UpdateStatus("Getting details of storage entity")
				coll1 := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
				if err := coll1.Find(bson.M{"storageid": *storage_id}).One(&storage); err != nil {
					utils.FailTask("Error getting details of storage entity", fmt.Errorf("%s - %v", ctxt, err), t)
					return
				}
				t.UpdateStatus("Getting details of block device entity")
				coll2 := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_BLOCK_DEVICES)
				if err := coll2.Find(bson.M{"clusterid": *cluster_id, "storageid": *storage_id, "id": *blockdevice_id}).One(&blockDevice); err != nil {
					utils.FailTask("Error getting details of block device entity", fmt.Errorf("%s - %v", ctxt, err), t)
					return
				}
				t.UpdateStatus("Getting a mon from cluster")
				monnode, err := GetCalamariMonNode(*cluster_id, ctxt)
				if err != nil {
					utils.FailTask(fmt.Sprintf("Error getting mon node details for cluster: %v", *cluster_id), fmt.Errorf("%s - %v", ctxt, err), t)
					return
				}
				t.UpdateStatus("Deleting block device")
				cmd := fmt.Sprintf("rbd rm %s --cluster %s --pool %s", blockDevice.Name, cluster.Name, storage.Name)
				ok, _, err := cephapi_backend.ExecCmd(monnode.Hostname, *cluster_id, cmd, ctxt)
				if err != nil || !ok {
					utils.FailTask(fmt.Sprintf("Deletion of block device failed on cluster: %s", *cluster_id), fmt.Errorf("%s - %v", ctxt, err), t)
					return
				} else {
					t.UpdateStatus("Removing the block device entity from DB")
					if err := coll2.Remove(bson.M{"clusterid": *cluster_id, "storageid": *storage_id, "id": *blockdevice_id}); err != nil {
						utils.FailTask(fmt.Sprintf("Error removing block device entity from DB for cluster: %d", *cluster_id), fmt.Errorf("%s - %v", ctxt, err), t)
						return
					}
				}
				UpdateObjectCountToSummaries(ctxt, cluster)
				t.UpdateStatus("Success")
				t.Done(models.TASK_STATUS_SUCCESS)
				return
			}
		}
	}
	if taskId, err := bigfin_task.GetTaskManager().Run(
		bigfin_conf.ProviderName,
		"CEPH-DeleteBlockDevice",
		asyncTask,
		nil,
		nil,
		nil); err != nil {
		logger.Get().Error("%s - Task creation failed for delete block device %v on cluster: %v. error: %v", ctxt, *blockdevice_id, *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, "Task creation failed for block device deletion")
		return err
	} else {
		*resp = utils.WriteAsyncResponse(taskId, fmt.Sprintf("Task Created for delet block device %v on cluster: %v", *blockdevice_id, *cluster_id), []byte{})
	}
	return nil
}

func (s *CephProvider) ResizeBlockDevice(req models.RpcRequest, resp *models.RpcResponse) error {
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
	blockdevice_id_str := req.RpcRequestVars["blockdevice-id"]
	blockdevice_id, err := uuid.Parse(blockdevice_id_str)
	if err != nil {
		logger.Get().Error("%s - Error parsing the block device id: %s. error: %v", ctxt, blockdevice_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the block device id: %s", blockdevice_id_str))
		return err
	}
	var request struct {
		Size string `json:"size"`
	}
	if err := json.Unmarshal(req.RpcRequestData, &request); err != nil {
		logger.Get().Error("%s - Unbale to parse the request. error: %v", ctxt, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request. error: %v", err))
		return err
	}
	sizeMBs := utils.SizeFromStr(request.Size) / 1024

	asyncTask := func(t *task.Task) {
		sessionCopy := db.GetDatastore().Copy()
		defer sessionCopy.Close()
		for {
			select {
			case <-t.StopCh:
				return
			default:
				var blockDevice models.BlockDevice
				var cluster models.Cluster
				var storage models.Storage

				t.UpdateStatus("Started ceph provider block device deletion: %v", t.ID)
				t.UpdateStatus("Getting details of cluster")
				coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
				if err := coll.Find(bson.M{"clusterid": *cluster_id}).One(&cluster); err != nil {
					utils.FailTask("Error getting details of cluster", fmt.Errorf("%s - %v", ctxt, err), t)
					return
				}
				t.UpdateStatus("Getting details of storage")
				coll1 := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
				if err := coll1.Find(bson.M{"storageid": *storage_id}).One(&storage); err != nil {
					utils.FailTask("Error getting details of storage", fmt.Errorf("%s - %v", ctxt, err), t)
					return
				}
				t.UpdateStatus("Getting details of block device entity")
				coll2 := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_BLOCK_DEVICES)
				if err := coll2.Find(bson.M{"clusterid": *cluster_id, "storageid": *storage_id, "id": *blockdevice_id}).One(&blockDevice); err != nil {
					utils.FailTask("Error getting details of block device entity", fmt.Errorf("%s - %v", ctxt, err), t)
					return
				}
				t.UpdateStatus("Getting a mon from cluster")
				monnode, err := GetCalamariMonNode(*cluster_id, ctxt)
				if err != nil {
					utils.FailTask(fmt.Sprintf("Error getting mon node details for cluster: %v", *cluster_id), fmt.Errorf("%s - %v", ctxt, err), t)
					return
				}
				t.UpdateStatus("Resizing block device")
				cmd := fmt.Sprintf("rbd resize --image %s --cluster %s --size %d --pool %s", blockDevice.Name, cluster.Name, sizeMBs, storage.Name)
				ok, _, err := cephapi_backend.ExecCmd(monnode.Hostname, *cluster_id, cmd, ctxt)
				if err != nil || !ok {
					utils.FailTask(fmt.Sprintf("Resizing of block device failed on cluster: %s", *cluster_id), fmt.Errorf("%s - %v", ctxt, err), t)
					return
				} else {
					t.UpdateStatus("Updating the block device entity in DB")
					if err := coll2.Update(
						bson.M{"clusterid": *cluster_id, "storageid": *storage_id, "id": *blockdevice_id},
						bson.M{"$set": bson.M{"size": request.Size}}); err != nil {
						utils.FailTask(fmt.Sprintf("Error updating block device entity in DB for cluster: %d", *cluster_id), fmt.Errorf("%s - %v", ctxt, err), t)
						return
					}
				}

				initMonitoringRoutines(ctxt, cluster, (*monnode).Hostname, []interface{}{FetchRBDStats, FetchObjectCount})
				UpdateObjectCountToSummaries(ctxt, cluster)

				t.UpdateStatus("Success")
				t.Done(models.TASK_STATUS_SUCCESS)
				return
			}
		}
	}
	if taskId, err := bigfin_task.GetTaskManager().Run(
		bigfin_conf.ProviderName,
		"CEPH-ResizeBlockDevice",
		asyncTask,
		nil,
		nil,
		nil); err != nil {
		logger.Get().Error("%s - Task creation failed for resize block device %v on cluster: %v. error: %v", ctxt, *blockdevice_id, *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, "Task creation failed for block device resize")
		return err
	} else {
		*resp = utils.WriteAsyncResponse(taskId, fmt.Sprintf("Task Created for resize block device %v on cluster: %v", *blockdevice_id, *cluster_id), []byte{})
	}
	return nil
}
