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
	"github.com/skyrings/bigfin/backend"
	"github.com/skyrings/bigfin/bigfinmodels"
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/skyring-common/conf"
	"github.com/skyrings/skyring-common/db"
	"github.com/skyrings/skyring-common/models"
	"github.com/skyrings/skyring-common/tools/logger"
	"github.com/skyrings/skyring-common/tools/uuid"
	"gopkg.in/mgo.v2/bson"
	"net/http"
	"strconv"
)

func (s *CephProvider) SyncBlockDevices(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext

	cluster_id_str, ok := req.RpcRequestVars["cluster-id"]
	if !ok {
		logger.Get().Error("%s- Cluster-id is not provided along with request", ctxt)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Cluster-id is not provided along with request"))
		return errors.New("Cluster-id is not provided along with request")
	}
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("%s-Error parsing the cluster id: %s. error: %v", ctxt, cluster_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s", cluster_id_str))
		return err
	}
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	// Get the cluster details
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	var cluster models.Cluster
	if err := coll.Find(bson.M{"clusterid": *cluster_id}).One(&cluster); err != nil {
		logger.Get().Error("%s-Error getting cluster details for %v. error: %v", ctxt, *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error getting cluster details for %v", *cluster_id))
		return err
	}

	// Get the pools for the cluster
	coll = sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
	var pools []models.Storage
	if err := coll.Find(bson.M{"clusterid": *cluster_id}).All(&pools); err != nil {
		logger.Get().Error("%s-Error getting the pools for cluster: %v. error: %v", ctxt, *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error getting the pools for cluster: %v", *cluster_id))
		return err
	}

	// Pick a random mon from the list
	monnode, err := GetRandomMon(*cluster_id)
	if err != nil {
		logger.Get().Error("%s-Error getting a mon from cluster: %v. error: %v", ctxt, *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error getting a mon for cluster id: %s", cluster_id_str))
		return err
	}

	for _, pool := range pools {
		coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_BLOCK_DEVICES)
		// Fetch the block devices from DB
		var fetchedDevices []models.BlockDevice
		if err := coll.Find(bson.M{"clusterid": *cluster_id, "storageid": pool.StorageId}).All(&fetchedDevices); err != nil {
			logger.Get().Error("%s-Error fetching the block devices from DB for pool: %s of cluster: %s. error: %v", ctxt, pool.Name, cluster.Name, err)
			continue
		}

		// Get the block devices of the pool
		cmd := fmt.Sprintf("rbd ls %s --cluster %s --format=json", pool.Name, cluster.Name)
		var blkDevices []string
		ok, out, err := cephapi_backend.ExecCmd(monnode.Hostname, *cluster_id, cmd)
		if !ok || err != nil {
			logger.Get().Error("%s-Error getting block devices for pool: %s on cluster: %s. error: %v", ctxt, pool.Name, cluster.Name, err)
			continue
		} else {
			if err := json.Unmarshal([]byte(out), &blkDevices); err != nil {
				logger.Get().Error("%s-Error parsing block devices list of pool: %s on cluster: %s. error: %v", ctxt, pool.Name, cluster.Name, err)
				continue
			}
		}
		var device models.BlockDevice
		for _, blkDevice := range blkDevices {
			cmd := fmt.Sprintf("rbd --cluster %s --image %s -p %s info --format=json", cluster.Name, blkDevice, pool.Name)
			var blkDeviceDet backend.BlockDevice
			ok, out, err := cephapi_backend.ExecCmd(monnode.Hostname, *cluster_id, cmd)
			if !ok || err != nil {
				logger.Get().Error("%s-Error getting information of block device: %s of pool: %s on cluster: %s. error: %v", ctxt, blkDevice, pool.Name, cluster.Name, err)
				continue
			} else {
				if err := json.Unmarshal([]byte(out), &blkDeviceDet); err != nil {
					logger.Get().Error("%s-Error parsing block device details for %s of pool: %s on cluster: %s. error: %v", ctxt, blkDevice, pool.Name, cluster.Name, err)
					continue
				}
			}
			if ok := device_in_fetched_list(fetchedDevices, blkDevice); !ok {
				// Update the block device details
				if err := coll.Update(
					bson.M{"clusterid": *cluster_id, "storageid": pool.StorageId, "id": device.Id},
					bson.M{"$set": bson.M{"size": fmt.Sprintf("%dMB", blkDeviceDet.Size/1048576)}}); err != nil {
					logger.Get().Error("%s-Error updating the details of block device: %s of pool: %s on cluster: %s. error: %v", ctxt, device.Name, pool.Name, cluster.Name, err)
					continue
				}
			} else {
				// Create and add new block device
				id, err := uuid.New()
				if err != nil {
					logger.Get().Error("%s-Error creating id for block device", ctxt)
					continue
				}
				newDevice := models.BlockDevice{
					Id:          *id,
					Name:        blkDevice,
					ClusterId:   *cluster_id,
					ClusterName: cluster.Name,
					StorageId:   pool.StorageId,
					StorageName: pool.Name,
					Size:        fmt.Sprintf("%dMB", blkDeviceDet.Size/1048576),
				}
				if err := coll.Insert(newDevice); err != nil {
					logger.Get().Error("%s-Error adding the block device: %s of pool: %s on cluster: %s. error: %v", ctxt, blkDevice, pool.Name, cluster.Name, err)
					continue
				}
			}
		}

		// Check and deleted unwanted block devices
		for _, fetchedDevice := range fetchedDevices {
			found := false
			for _, blkDevice := range blkDevices {
				if fetchedDevice.Name == blkDevice {
					found = true
					break
				}
			}
			if !found {
				if err := coll.Remove(bson.M{"clusterid": *cluster_id, "storageid": pool.StorageId, "name": fetchedDevice.Name}); err != nil {
					logger.Get().Error("%s-Error removing blokc device: %s of pool: %s on cluster: %s. error: %v", ctxt, fetchedDevice.Name, pool.Name, cluster.Name, err)
					continue
				}
			}
		}
	}

	return nil
}

func device_in_fetched_list(fetchedDevices []models.BlockDevice, device string) bool {
	for _, fetchedDevice := range fetchedDevices {
		if fetchedDevice.Name == device {
			return true
		}
	}
	return false
}

func (s *CephProvider) SyncStorageLogicalUnits(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext

	cluster_id_str, ok := req.RpcRequestVars["cluster-id"]
	if !ok {
		logger.Get().Error("%s-Cluster-id is not provided along with request", ctxt)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Cluster-id is not provided along with request"))
		return errors.New("Cluster-id is not provided along with request")
	}
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("%s-Error parsing the cluster id: %s. error: %v", ctxt, cluster_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s", cluster_id_str))
		return err
	}

	if err := syncOsds(*cluster_id, ctxt); err != nil {
		logger.Get().Error("%s-Error syncing the OSDs for cluster: %v. Err: %v", ctxt, *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error syncing the OSD status. Err: %v", err))
		return err
	}
	*resp = utils.WriteResponse(http.StatusOK, "")
	return nil

}

func syncOsds(clusterId uuid.UUID, ctxt string) error {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)

	// Fetch the OSDs from DB
	var fetchedSlus []models.StorageLogicalUnit
	if err := coll.Find(bson.M{"clusterid": clusterId}).All(&fetchedSlus); err != nil {
		logger.Get().Error("%s-Error fetching SLUs from DB for cluster: %v. error: %v", ctxt, clusterId, err)
		return err
	}

	// Get a random mon node
	monnode, err := GetRandomMon(clusterId)
	if err != nil {
		logger.Get().Error("%s-Error getting a mon node in cluster: %s. error: %v", ctxt, clusterId, err)
		return err
	}

	osds, err := cephapi_backend.GetOSDs(monnode.Hostname, clusterId)
	if err != nil {
		logger.Get().Error("%s-Error getting OSDs list for cluster: %v. error: %v", ctxt, clusterId, err)
		return err
	}

	for _, osd := range osds {
		if ok := osd_in_fetched_list(fetchedSlus, osd); !ok {
			status := mapOsdStatus(osd.Up, osd.In)
			state := mapOsdState(osd.In)
			if err := coll.Update(bson.M{"sluid": osd.Uuid, "clusterid": clusterId}, bson.M{"$set": bson.M{"status": status, "state": state}}); err != nil {
				logger.Get().Error("%s-Error updating the status for slu: %s. error: %v", ctxt, osd.Uuid.String(), err)
				continue
			}
		} else {
			// Get the node details for SLU
			coll1 := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
			var node models.Node
			if err := coll1.Find(bson.M{"hostname": fmt.Sprintf("/%s.*/", osd.Server)}).One(&node); err != nil {
				logger.Get().Error("%s-Error fetching node details for SLU id: %d on cluster: %v. error: %v", ctxt, osd.Id, clusterId, err)
				continue
			}
			newSlu := models.StorageLogicalUnit{
				SluId:     osd.Uuid,
				Name:      fmt.Sprintf("osd.%d", osd.Id),
				Type:      models.CEPH_OSD,
				ClusterId: clusterId,
				NodeId:    node.NodeId,
				// TODO:: Below details should be enabled once calamari provides device details
				//StorageId:
				//StorageDeviceId:
				//StorageDeviceSize:
				//StorageProfile:
				Status: mapOsdStatus(osd.Up, osd.In),
				State:  mapOsdState(osd.In),
			}
			var options map[string]string
			options["in"] = strconv.FormatBool(osd.In)
			options["up"] = strconv.FormatBool(osd.Up)
			newSlu.Options = options
			if err := coll.Insert(newSlu); err != nil {
				logger.Get().Error("%s-Error creating the new SLU for cluster: %v. error: %v", ctxt, clusterId, err)
				continue
			}

		}
	}

	return nil
}

func osd_in_fetched_list(fetchedSlus []models.StorageLogicalUnit, osd backend.CephOSD) bool {
	for _, fetchedSlu := range fetchedSlus {
		if fetchedSlu.SluId == osd.Uuid {
			return true
		}
	}
	return false
}

func mapOsdStatus(status bool, state bool) models.SluStatus {
	if status && state {
		return models.SLU_STATUS_OK
	} else if status && !state {
		return models.SLU_STATUS_WARN
	} else if !status {
		return models.SLU_STATUS_ERROR
	}
	return models.SLU_STATUS_UNKNOWN

}

func mapOsdState(state bool) string {
	if state {
		return bigfinmodels.OSD_STATE_IN
	}
	return bigfinmodels.OSD_STATE_OUT
}

func (s *CephProvider) SyncNodeDetails(req models.RpcRequest, resp *models.RpcResponse) error {
	// ctxt := req.RpcRequestContext

	// cluster_id_str, ok := req.RpcRequestVars["cluster-id"]
	// if !ok {
	// 	logger.Get().Error("%s- Cluster-id is not provided along with request")
	// 	*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Cluster-id is not provided along with request"))
	// 	return errors.New("Cluster-id is not provided along with request")
	// }
	// cluster_id, err := uuid.Parse(cluster_id_str)
	// if err != nil {
	// 	logger.Get().Error("%s-Error parsing the cluster id: %s. error: %v", ctxt, cluster_id_str, err)
	// 	*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s", cluster_id_str))
	// 	return err
	// }

	// // Pick a random mon from the list
	// monnode, err := GetRandomMon(*cluster_id)
	// if err != nil {
	// 	logger.Get().Error("%s-Error getting a mon from cluster: %v. error: %v", ctxt, *cluster_id, err)
	// 	*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error getting a mon for cluster id: %s", cluster_id_str))
	// 	return err
	// }

	// mons, err := cephapi_backend.GetMonitors(monnode.Hostname, *cluster_id)
	// if err != nil {
	// 	logger.Get().Error("%s-Could not get monitors details of cluster: %v. error: %v", ctxt, *cluster_id, err)
	// 	*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Could not get monitors details of cluster: %v", *cluster_id))
	// 	return err
	// }
	// sessionCopy := db.GetDatastore().Copy()
	// defer sessionCopy.Close()
	// coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	// var node models.Node
	// for _, mon := range mons {
	// 	if err := coll.Find(bson.M{"managementip4": mon}).One(&node); err != nil {
	// 		logger.Get().Error("Error getting details of node: %s. error: %v", mon, err)
	// 		continue
	// 	}
	// 	if node.Hostname == "" {
	// 		// Add a new node
	// 	} else {
	// 		// Update the node as mon
	// 	}
	// }

	// //TODO:: Get the OSDs list and see if any if the new nodes used as OSD node
	// //If so create the new node entry in DB

	// // If nodes added to the DB due to sync, manual bootstrap to be done as per below steps
	// // 0. Install the required USM bits on the node
	// // 1. Accept the node
	// // 2. Initialize the node details like MachineId, Disks, Storage etc.
	// // 3.

	return nil
}
