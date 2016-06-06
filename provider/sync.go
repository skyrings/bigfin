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
	skyring_util "github.com/skyrings/skyring-common/utils"
	"gopkg.in/mgo.v2/bson"
	"net/http"
	"strconv"
	"strings"

	bigfin_models "github.com/skyrings/bigfin/bigfinmodels"
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

	// Pick a random mon from the list
	monnode, err := GetCalamariMonNode(*cluster_id, ctxt)
	if err != nil {
		logger.Get().Error(
			"%s-Error getting a mon node in cluster: %v. error: %v",
			ctxt,
			*cluster_id, err)
		*resp = utils.WriteResponse(
			http.StatusInternalServerError,
			fmt.Sprintf(
				"Error getting a mon node in cluster. error: %v",
				err))
		return err
	}

	if err := syncRBDs(monnode.Hostname, *cluster_id, ctxt); err != nil {
		logger.Get().Error(err.Error())
		*resp = utils.WriteResponse(http.StatusInternalServerError, err.Error())
		return err
	}
	*resp = utils.WriteResponse(http.StatusOK, "")
	return nil
}

func syncRBDs(mon string, clusterId uuid.UUID, ctxt string) error {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	// Get the cluster details
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	var cluster models.Cluster
	if err := coll.Find(bson.M{"clusterid": clusterId}).One(&cluster); err != nil {
		return fmt.Errorf(
			"%s-Error getting cluster details for %v. error: %v",
			ctxt,
			clusterId,
			err)
	}

	// Get the pools for the cluster
	coll = sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
	var pools []models.Storage
	if err := coll.Find(bson.M{"clusterid": clusterId}).All(&pools); err != nil {
		return fmt.Errorf(
			"%s-Error getting the pools for cluster: %v. error: %v",
			ctxt,
			clusterId,
			err)
	}

	for _, pool := range pools {
		if pool.Type != models.STORAGE_TYPE_REPLICATED {
			continue
		}
		coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_BLOCK_DEVICES)
		// Fetch the block devices from DB
		var fetchedDevices []models.BlockDevice
		if err := coll.Find(bson.M{"clusterid": clusterId, "storageid": pool.StorageId}).All(&fetchedDevices); err != nil {
			logger.Get().Error(
				"%s-Error fetching the block devices from DB for pool: %s of cluster: %s. error: %v",
				ctxt,
				pool.Name,
				cluster.Name,
				err)
			continue
		}

		// Get the block devices of the pool
		cmd := fmt.Sprintf(
			"rbd ls %s --cluster %s --format=json",
			pool.Name,
			cluster.Name)
		var blkDevices []string
		ok, out, err := cephapi_backend.ExecCmd(
			mon,
			clusterId,
			cmd,
			ctxt)
		if !ok || err != nil {
			logger.Get().Error(
				"%s-Error getting block devices for pool: %s on cluster: %s. error: %v",
				ctxt,
				pool.Name,
				cluster.Name,
				err)
			continue
		} else {
			if err := json.Unmarshal([]byte(out), &blkDevices); err != nil {
				logger.Get().Error(
					"%s-Error parsing block devices list of pool: %s on cluster: %s. error: %v",
					ctxt,
					pool.Name,
					cluster.Name,
					err)
				continue
			}
		}
		for _, blkDevice := range blkDevices {
			cmd := fmt.Sprintf(
				"rbd --cluster %s --image %s -p %s info --format=json",
				cluster.Name,
				blkDevice,
				pool.Name)
			var blkDeviceDet backend.BlockDevice
			ok, out, err := cephapi_backend.ExecCmd(
				mon,
				clusterId,
				cmd,
				ctxt)
			if !ok || err != nil {
				logger.Get().Error(
					"%s-Error getting information of block device: %s "+
						"of pool: %s on cluster: %s. error: %v",
					ctxt,
					blkDevice,
					pool.Name,
					cluster.Name,
					err)
				continue
			} else {
				if err := json.Unmarshal([]byte(out), &blkDeviceDet); err != nil {
					logger.Get().Error(
						"%s-Error parsing block device details for %s "+
							"of pool: %s on cluster: %s. error: %v",
						ctxt,
						blkDevice,
						pool.Name,
						cluster.Name,
						err)
					continue
				}
			}
			if ok := device_in_fetched_list(fetchedDevices, blkDevice); ok {
				// Update the block device details
				if err := coll.Update(
					bson.M{
						"clusterid": clusterId,
						"storageid": pool.StorageId,
						"name":      blkDevice},
					bson.M{
						"$set": bson.M{"size": fmt.Sprintf(
							"%dMB",
							blkDeviceDet.Size/1048576)}}); err != nil {
					logger.Get().Error(
						"%s-Error updating the details of block device: %s "+
							"of pool: %s on cluster: %s. error: %v",
						ctxt,
						blkDevice,
						pool.Name,
						cluster.Name,
						err)
					continue
				}
				logger.Get().Info(
					"%s-Updated the details of block device: %s of pool: %s "+
						"on cluster: %s",
					ctxt,
					blkDevice,
					pool.Name,
					cluster.Name)
			} else {
				// Create and add new block device
				id, err := uuid.New()
				if err != nil {
					logger.Get().Error(
						"%s-Error creating id for block device. error: %v",
						ctxt,
						err)
					continue
				}
				newDevice := models.BlockDevice{
					Id:          *id,
					Name:        blkDevice,
					ClusterId:   clusterId,
					ClusterName: cluster.Name,
					StorageId:   pool.StorageId,
					StorageName: pool.Name,
					Size:        fmt.Sprintf("%dMB", blkDeviceDet.Size/1048576),
				}
				if err := coll.Insert(newDevice); err != nil {
					logger.Get().Error(
						"%s-Error adding the block device: %s of pool: %s "+
							"on cluster: %s. error: %v",
						ctxt,
						blkDevice,
						pool.Name,
						cluster.Name,
						err)
					continue
				}
				logger.Get().Info(
					"%s-Added new block device: %s of pool: %s on cluster: %s",
					ctxt,
					blkDevice,
					pool.Name,
					cluster.Name)
			}
		}

		// Check and delete unwanted block devices
		for _, fetchedDevice := range fetchedDevices {
			found := false
			for _, blkDevice := range blkDevices {
				if fetchedDevice.Name == blkDevice {
					found = true
					break
				}
			}
			if !found {
				if err := coll.Remove(bson.M{
					"clusterid": clusterId,
					"storageid": pool.StorageId,
					"name":      fetchedDevice.Name}); err != nil {
					logger.Get().Error(
						"%s-Error removing blokc device: %s "+
							"of pool: %s on cluster: %s. error: %v",
						ctxt,
						fetchedDevice.Name,
						pool.Name,
						cluster.Name,
						err)
					continue
				}
				logger.Get().Info(
					"%s-Deleted the block device: %s of pool: %s on cluster: %s",
					ctxt,
					fetchedDevice.Name,
					pool.Name,
					cluster.Name)
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

	// Get a random mon node
	monnode, err := GetCalamariMonNode(*cluster_id, ctxt)
	if err != nil {
		logger.Get().Error(
			"%s-Error getting a mon node in cluster: %v. error: %v",
			ctxt,
			*cluster_id, err)
		*resp = utils.WriteResponse(
			http.StatusInternalServerError,
			fmt.Sprintf(
				"Error getting a mon node in cluster. error: %v",
				err))
		return err
	}

	if err := syncOsds(monnode.Hostname, *cluster_id, ctxt); err != nil {
		logger.Get().Error("%s-Error syncing the OSDs for cluster: %v. Err: %v", ctxt, *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error syncing the OSD status. Err: %v", err))
		return err
	}
	*resp = utils.WriteResponse(http.StatusOK, "")
	return nil

}

func syncOsds(mon string, clusterId uuid.UUID, ctxt string) error {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll_slu := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
	coll_nodes := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)

	// Fetch the OSDs from DB
	var fetchedSlus []models.StorageLogicalUnit
	if err := coll_slu.Find(bson.M{"clusterid": clusterId}).All(&fetchedSlus); err != nil {
		logger.Get().Error("%s-Error fetching SLUs from DB for cluster: %v. error: %v", ctxt, clusterId, err)
		return err
	}
	var fetchedSlusMap = make(map[string]models.StorageLogicalUnit)
	for _, fetchedSlu := range fetchedSlus {
		fetchedSlusMap[fmt.Sprintf(
			"%s:%s",
			fetchedSlu.NodeId.String(),
			fetchedSlu.Options["device_partuuid"])] = fetchedSlu
	}

	osds, err := cephapi_backend.GetOSDs(mon, clusterId, ctxt)
	if err != nil {
		logger.Get().Error("%s-Error getting OSDs list for cluster: %v. error: %v", ctxt, clusterId, err)
		return err
	}
	for _, osd := range osds {
		// Get the node details for SLU
		var node models.Node
		if err := coll_nodes.Find(
			bson.M{"hostname": osd.Server}).One(&node); err != nil {
			logger.Get().Error(
				"%s-Error fetching node details for SLU id: %d on cluster: %v. error: %v",
				ctxt,
				osd.Id,
				clusterId,
				err)
			continue
		}

		deviceDetails, err := salt_backend.GetPartDeviceDetails(
			node.Hostname,
			osd.OsdData,
			ctxt)
		if err != nil {
			logger.Get().Error(
				"%s-Error getting device details of osd.%d. error: %v",
				ctxt,
				osd.Id,
				err)
			continue
		}

		journalDeviceDetails, err := salt_backend.GetJournalPartDeviceDetails(
			node.Hostname,
			osd.OsdJournal,
			ctxt)
		if err != nil {
			logger.Get().Error(
				"%s-Error getting journal details of osd.%d. error: %v",
				ctxt,
				osd.Id,
				err)
			continue
		}

		if _, ok := fetchedSlusMap[fmt.Sprintf("%s:%s", node.NodeId.String(), deviceDetails.PartUuid.String())]; ok {
			status := mapOsdStatus(osd.Up, osd.In)
			state := mapOsdState(osd.In)
			skyring_util.AppendServiceToNode(bson.M{"nodeid": node.NodeId}, fmt.Sprintf("%s-%s", bigfinmodels.NODE_SERVICE_OSD, fmt.Sprintf("osd.%d", osd.Id)), mapOSDStatusToServiceStatus(state), ctxt)
			if err := coll_slu.Update(
				bson.M{
					"name":      fmt.Sprintf("osd.%d", osd.Id),
					"nodeid":    node.NodeId,
					"clusterid": clusterId},
				bson.M{"$set": bson.M{
					"sluid":  osd.Uuid,
					"status": status,
					"state":  state,
					"name":   fmt.Sprintf("osd.%d", osd.Id),
				}}); err != nil {
				logger.Get().Error("%s-Error updating the slu: %s. error: %v", ctxt, osd.Uuid.String(), err)
				continue
			}
			logger.Get().Info("%s-Updated the slu: osd.%d on cluster: %v", ctxt, osd.Id, clusterId)
		} else {
			newSlu := models.StorageLogicalUnit{
				SluId:     osd.Uuid,
				Name:      fmt.Sprintf("osd.%d", osd.Id),
				Type:      models.CEPH_OSD,
				ClusterId: clusterId,
				NodeId:    node.NodeId,
				Status:    mapOsdStatus(osd.Up, osd.In),
				State:     mapOsdState(osd.In),
			}

			var journalDiskSSD bool
			for _, storageDisk := range node.StorageDisks {
				if storageDisk.Name == journalDeviceDetails.DevName {
					journalDiskSSD = storageDisk.SSD
				}
			}

			journalDetail := JournalDetail{
				JournalDisk:     journalDeviceDetails.DevName,
				JournalDiskUuid: journalDeviceDetails.PartUuid.String(),
				Size:            journalDeviceDetails.Size,
				SSD:             journalDiskSSD,
				OsdJournal:      osd.OsdJournal,
				Reweight:        float64(osd.Reweight),
			}

			newSlu.StorageDeviceId = deviceDetails.Uuid
			newSlu.StorageDeviceSize = deviceDetails.Size
			newSlu.StorageProfile = get_disk_profile(node.StorageDisks, deviceDetails.PartName)
			var options = make(map[string]interface{})
			options["in"] = strconv.FormatBool(osd.In)
			options["up"] = strconv.FormatBool(osd.Up)
			options["node"] = node.Hostname
			options["publicip4"] = node.PublicIP4
			options["clusterip4"] = node.ClusterIP4
			options["device"] = deviceDetails.DevName
			options["device_partuuid"] = deviceDetails.PartUuid.String()
			options["fstype"] = deviceDetails.FSType
			options["journal"] = journalDetail
			newSlu.Options = options
			if err := coll_slu.Insert(newSlu); err != nil {
				logger.Get().Error(
					"%s-Error creating the new SLU for cluster: %v. error: %v",
					ctxt,
					clusterId,
					err)
				continue
			}
			logger.Get().Info("%s-Added new slu: osd.%d on cluster: %v", ctxt, osd.Id, clusterId)
		}
	}
	// Remove the unwanted slus
	for _, fetchedSlu := range fetchedSlus {
		if ok := fetched_slu_in_osds_list(osds, fetchedSlu); !ok {
			if err := coll_slu.Remove(bson.M{"sluid": fetchedSlu.SluId}); err != nil {
				logger.Get().Error(
					"%s-Error removing the slu: %s for cluster: %v. error: %v",
					ctxt,
					fetchedSlu.Name,
					clusterId,
					err)
			}
		}
	}

	return nil
}

func get_disk_profile(disks []models.Disk, partName string) string {
	for _, disk := range disks {
		if disk.DevName == partName {
			return disk.StorageProfile
		}
	}
	return ""
}

func (s *CephProvider) SyncStorages(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext
	cluster_id_str := req.RpcRequestVars["cluster-id"]
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error(
			"%s-Error parsing the cluster id: %s. error: %v",
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
	monnode, err := GetCalamariMonNode(*cluster_id, ctxt)
	if err != nil {
		logger.Get().Error(
			"%s-Error getting a mon node in cluster: %v. error: %v",
			ctxt,
			*cluster_id, err)
		*resp = utils.WriteResponse(
			http.StatusInternalServerError,
			fmt.Sprintf(
				"Error getting a mon node in cluster. error: %v",
				err))
		return err
	}

	// Sync the pools for the cluster
	if err := syncStoragePools(
		monnode.Hostname,
		*cluster_id,
		ctxt); err != nil {
		logger.Get().Error(
			"%s-Error syncing storages for cluster: %v. error: %v",
			ctxt,
			*cluster_id,
			err)
		*resp = utils.WriteResponse(
			http.StatusInternalServerError,
			fmt.Sprintf(
				"Error syncing storages. error: %v",
				err))
		return err
	}
	*resp = utils.WriteResponse(http.StatusOK, "")
	return nil
}

func syncStoragePools(mon string, clusterId uuid.UUID, ctxt string) error {
	pools, err := cephapi_backend.GetPools(mon, clusterId, ctxt)
	if err != nil {
		return err
	}
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll1 := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	var cluster models.Cluster
	if err := coll1.Find(bson.M{"clusterid": clusterId}).One(&cluster); err != nil {
		return fmt.Errorf(
			"%s-Error getting details of cluster: %v. error: %v",
			ctxt,
			clusterId,
			err)
	}
	var storages []models.Storage
	for _, pool := range pools {
		storage := models.Storage{
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
			mon,
			clusterId,
			fmt.Sprintf(
				"ceph --cluster %s osd pool get %s erasure_code_profile --format=json",
				cluster.Name,
				pool.Name),
			ctxt)
		if err != nil || !ok {
			storage.Type = models.STORAGE_TYPE_REPLICATED
			logger.Get().Warning(
				"%s-Error getting EC profile details of pool: %s of cluster: %s",
				ctxt,
				pool.Name,
				cluster.Name)
		} else {
			var ecprofileDet bigfinmodels.ECProfileDet
			if err := json.Unmarshal([]byte(out), &ecprofileDet); err != nil {
				logger.Get().Warning(
					"%s-Error parsing EC profile details of pool: %s of cluster: %s",
					ctxt,
					pool.Name,
					cluster.Name)
			} else {
				storage.Type = models.STORAGE_TYPE_ERASURE_CODED
				options["ecprofile"] = ecprofileDet.ECProfile
			}
		}
		storage.Options = options
		storages = append(storages, storage)
	}

	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)

	// Get the list of storage entities from DB
	var fetchedStorages models.Storages
	if err := coll.Find(bson.M{"clusterid": clusterId}).All(&fetchedStorages); err != nil {
		return fmt.Errorf("Error getting storage entities from DB. error: %v", err)
	}

	// Insert/update storages
	for _, storage := range storages {
		// Check if the pool already exists, if so update else insert
		if !storage_in_list(fetchedStorages, storage.Name) {
			// Not found, insert
			storage.ClusterId = clusterId
			uuid, err := uuid.New()
			if err != nil {
				logger.Get().Error(
					"%s-Error creating id for the new storage entity: %s. error: %v",
					ctxt,
					storage.Name,
					err)
				continue
			}
			storage.StorageId = *uuid
			if err := coll.Insert(storage); err != nil {
				logger.Get().Error(
					"%s-Error adding storage:%s to DB on cluster: %s. error: %v",
					ctxt,
					storage.Name,
					cluster.Name,
					err)
				continue
			}
			logger.Get().Info(
				"%s-Added the new storage entity: %s on cluster: %s",
				ctxt,
				storage.Name,
				cluster.Name)
		} else {
			// Update
			if err := coll.Update(
				bson.M{"name": storage.Name},
				bson.M{"$set": bson.M{
					"options":       storage.Options,
					"quota_enabled": storage.QuotaEnabled,
					"quota_params":  storage.QuotaParams,
				}}); err != nil {
				logger.Get().Error(
					"%s-Error updating the storage entity: %s on cluster: %s. error: %v",
					ctxt,
					storage.Name,
					cluster.Name,
					err)
				continue
			}
			logger.Get().Info(
				"%s-Updated details of storage entity: %s on cluster: %s",
				ctxt,
				storage.Name,
				cluster.Name)
		}
	}
	// Delete the un-wanted storages
	for _, fetchedStorage := range fetchedStorages {
		found := false
		for _, storage := range storages {
			if storage.Name == fetchedStorage.Name {
				found = true
				break
			}
		}
		if !found {
			if err := coll.Remove(bson.M{"storageid": fetchedStorage.StorageId}); err != nil {
				logger.Get().Error(
					"%s-Error removing the storage: %s of cluster: %s. error: %v",
					ctxt,
					fetchedStorage.Name,
					cluster.Name,
					err)
			}
			logger.Get().Info(
				"%s-Removed storage entity: %s on cluster: %s",
				ctxt,
				fetchedStorage.Name,
				cluster.Name)
		}
	}
	return nil
}

func (s *CephProvider) SyncStorageNodes(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext
	cluster_id_str := req.RpcRequestVars["cluster-id"]
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error(
			"%s-Error parsing the cluster id: %s. error: %v",
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
	monnode, err := GetCalamariMonNode(*cluster_id, ctxt)
	if err != nil {
		logger.Get().Error(
			"%s-Error getting a mon node in cluster: %v. error: %v",
			ctxt,
			*cluster_id, err)
		*resp = utils.WriteResponse(
			http.StatusInternalServerError,
			fmt.Sprintf(
				"Error getting a mon node in cluster. error: %v",
				err))
		return err
	}

	// Sync the nodes for the cluster
	if err := syncStorageNodes(
		monnode.Hostname,
		*cluster_id,
		ctxt); err != nil {
		logger.Get().Error(
			"%s-Error syncing storage nodes for cluster: %v. error: %v",
			ctxt,
			*cluster_id,
			err)
		*resp = utils.WriteResponse(
			http.StatusInternalServerError,
			fmt.Sprintf(
				"Error syncing storage nodes. error: %v",
				err))
		return err
	}
	*resp = utils.WriteResponse(http.StatusOK, "")
	return nil
}

func syncStorageNodes(mon string, clusterId uuid.UUID, ctxt string) error {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	var failedNodes, succeededNodes []string

	// Get the nodes for the cluster from DB
	var fetchedNodes models.Nodes
	if err := coll.Find(bson.M{"clusterid": clusterId}).All(&fetchedNodes); err != nil {
		return fmt.Errorf(
			"%s-Error fetching the nodes of cluster: %v from DB. error: %v",
			ctxt,
			clusterId,
			err)
	}

	nodes, err := cephapi_backend.GetClusterNodes(mon, clusterId, ctxt)
	if err != nil {
		return fmt.Errorf(
			"%s-Error getting nodes of the cluster: %v. error: %v",
			ctxt,
			clusterId,
			err)
	}

	var fetchedNode models.Node
	for _, node := range nodes {
		var updates bson.M = make(map[string]interface{})
		for _, service := range node.Services {
			if err := coll.Find(bson.M{"hostname": node.FQDN}).One(&fetchedNode); err != nil {
				logger.Get().Warning(
					"%s-Failed to update OSD role for node: %s. error: %v",
					ctxt,
					node.FQDN,
					err)
				failedNodes = append(failedNodes, node.FQDN)
				continue
			}
			nodeRoles := fetchedNode.Roles

			if service.Type == bigfin_models.NODE_SERVICE_MON {
				updates["clusterid"] = clusterId
				updates["options.mon"] = models.Yes
				if strings.HasPrefix(mon, node.FQDN) {
					updates["options.calamari"] = models.Yes
				}

				if ok := skyring_util.StringInSlice(MON, nodeRoles); !ok {
					updates["roles"] = append(nodeRoles, MON)
				}
				skyring_util.AppendServiceToNode(bson.M{"hostname": fetchedNode.Hostname}, bigfinmodels.NODE_SERVICE_MON, models.STATUS_UP, ctxt)
				if err := coll.Update(
					bson.M{"hostname": fetchedNode.Hostname},
					bson.M{"$set": updates}); err != nil {
					failedNodes = append(failedNodes, node.FQDN)
				}
				succeededNodes = append(succeededNodes, node.Hostname)
			}
			if service.Type == bigfin_models.NODE_SERVICE_OSD {
				if ok := skyring_util.StringInSlice(OSD, nodeRoles); !ok {
					if err := coll.Update(
						bson.M{"hostname": fetchedNode.Hostname},
						bson.M{"$push": bson.M{"roles": OSD}}); err != nil {
						logger.Get().Warning(
							"%s-Failed to update the OSD role for the node: %s. error: %v",
							ctxt,
							node.FQDN,
							err)
						failedNodes = append(failedNodes, node.FQDN)
					}
				}
			}
		}
	}

	if len(failedNodes) > 0 {
		logger.Get().Warning(
			"%s-Updating node details failed for [%s]",
			ctxt,
			failedNodes)
	}
	for _, node := range succeededNodes {
		mondet, err := cephapi_backend.GetMonStatus(
			mon,
			clusterId,
			node,
			ctxt)
		if err != nil {
			logger.Get().Warning(
				"%s-Failed to get mon status of node: %s. error: %v",
				ctxt,
				node,
				err)
		}
		if mondet.State == "leader" {
			if err := coll.Find(
				bson.M{"hostname": node}).One(&fetchedNode); err != nil {
				logger.Get().Warning(
					"%s-Failed to mark mon node: %s as leader. error: %v",
					ctxt,
					node,
					err)
				break
			}
			if err := coll.Update(
				bson.M{"clusterid": clusterId, "hostname": fetchedNode.Hostname},
				bson.M{"$set": bson.M{"options.leader": models.Yes}}); err != nil {
				logger.Get().Warning(
					"%s-Failed to update leader status of mon node: %s. error: %v",
					ctxt,
					node,
					err)
			}
		}
	}

	// Remove the unwanted nodes from DB
	// for _, fetchedNode := range fetchedNodes {
	// 	if ok := node_in_cluster_nodes(nodes, fetchedNode); !ok {
	// 		if err := coll.Remove(bson.M{
	// 			"clusterid": clusterId,
	// 			"hostname":  fetchedNode.Hostname}); err != nil {
	// 			logger.Get().Error(
	// 				"%s-Error removing the node: %s for cluster: %v. error: %v",
	// 				ctxt,
	// 				fetchedNode.Hostname,
	// 				clusterId,
	// 				err)
	// 		}
	// 	}
	// }
	return nil
}

func node_in_cluster_nodes(clusterNodes []backend.CephClusterNode, node models.Node) bool {
	for _, clusterNode := range clusterNodes {
		if clusterNode.FQDN == node.Hostname {
			return true
		}
	}
	return false
}

func fetched_slu_in_osds_list(osds []backend.CephOSD, slu models.StorageLogicalUnit) bool {
	for _, osd := range osds {
		if uuid.Equal(osd.Uuid, slu.SluId) {
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

func storage_in_list(fetchedStorages models.Storages, name string) bool {
	for _, storage := range fetchedStorages {
		if storage.Name == name {
			return true
		}
	}
	return false
}
