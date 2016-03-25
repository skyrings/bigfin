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
	"github.com/skyrings/skyring-common/monitoring"
	"github.com/skyrings/skyring-common/tools/logger"
	"github.com/skyrings/skyring-common/tools/task"
	"github.com/skyrings/skyring-common/tools/uuid"
	"github.com/skyrings/skyring-common/utils"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"net/http"
	"strconv"
	"strings"
	"time"

	bigfin_conf "github.com/skyrings/bigfin/conf"
	bigfin_task "github.com/skyrings/bigfin/tools/task"
)

var (
	cluster_status_map = map[string]models.ClusterStatus{
		"HEALTH_OK":   models.CLUSTER_STATUS_OK,
		"HEALTH_WARN": models.CLUSTER_STATUS_WARN,
		"HEALTH_ERR":  models.CLUSTER_STATUS_ERROR,
	}
)

const (
	RULEOFFSET = 10000
)

func (s *CephProvider) CreateCluster(req models.RpcRequest, resp *models.RpcResponse) error {
	var request models.AddClusterRequest

	ctxt := req.RpcRequestContext

	if err := json.Unmarshal(req.RpcRequestData, &request); err != nil {
		logger.Get().Error(fmt.Sprintf("%s-Unbale to parse the create cluster request for %s. error: %v", ctxt, request.Name, err))
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request. error: %v", err))
		return err
	}

	// Get corresponding nodes from DB
	nodes, err := util.GetNodes(request.Nodes)
	if err != nil {
		logger.Get().Error("%s-Error getting nodes from DB while create cluster %s. error: %v", ctxt, request.Name, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error getting nodes from DB. error: %v", err))
		return err
	}

	// Get the cluster and public IPs for nodes
	node_ips, err := nodeIPs(request.Networks, nodes)
	if err != nil {
		logger.Get().Error("%s-Node IP does not fall in provided subnets for cluster: %s. error: %v", ctxt, request.Name, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Node IP does not fall in provided subnets. error: %v", err))
		return nil
	}

	// Invoke the cluster create backend
	cluster_uuid, err := uuid.New()
	if err != nil {
		logger.Get().Error("%s-Error creating cluster id while create cluster %s. error: %v", ctxt, request.Name, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error creating cluster id. error: %v", err))
		return nil
	}
	var mons []backend.Mon
	for _, req_node := range request.Nodes {
		if util.StringInSlice("MON", req_node.NodeType) {
			var mon backend.Mon
			nodeid, _ := uuid.Parse(req_node.NodeId)
			mon.Node = nodes[*nodeid].Hostname
			mon.PublicIP4 = node_ips[*nodeid]["public"]
			mon.ClusterIP4 = node_ips[*nodeid]["cluster"]
			mons = append(mons, mon)
		}
	}
	if len(mons) == 0 {
		logger.Get().Error(fmt.Sprintf("%s-No mons mentioned in the node list while create cluster %s", ctxt, request.Name))
		*resp = utils.WriteResponse(http.StatusInternalServerError, "No mons mentioned in the node list")
		return errors.New(fmt.Sprintf("No mons mentioned in the node list while create cluster %s", request.Name))
	}

	asyncTask := func(t *task.Task) {
		sessionCopy := db.GetDatastore().Copy()
		defer sessionCopy.Close()
		for {
			select {
			case <-t.StopCh:
				return
			default:
				t.UpdateStatus("Started ceph provider task for cluster creation: %v", t.ID)

				// Add cluster to DB
				t.UpdateStatus("Persisting cluster details")
				var cluster models.Cluster
				cluster.ClusterId = *cluster_uuid
				cluster.Name = request.Name
				cluster.CompatVersion = request.CompatVersion
				cluster.Type = request.Type
				cluster.Status = models.CLUSTER_STATUS_UNKNOWN
				cluster.WorkLoad = request.WorkLoad
				cluster.Tags = request.Tags
				cluster.Options = request.Options
				cluster.Networks = request.Networks
				cluster.OpenStackServices = request.OpenStackServices
				cluster.State = models.CLUSTER_STATE_CREATING
				cluster.AlmStatus = models.ALARM_STATUS_CLEARED
				cluster.AutoExpand = !request.DisableAutoExpand

				cluster.MonitoringInterval = request.MonitoringInterval
				if cluster.MonitoringInterval == 0 {
					cluster.MonitoringInterval = monitoring.DefaultClusterMonitoringInterval
				}
				coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
				if err := coll.Insert(cluster); err != nil {
					utils.FailTask(fmt.Sprintf("%s-Error persisting the cluster %s", ctxt, request.Name), err, t)
					return
				}
				t.UpdateStatus("Creating cluster with mon: %s", mons[0].Node)
				ret_val, err := salt_backend.CreateCluster(request.Name, *cluster_uuid, []backend.Mon{mons[0]}, ctxt)
				if err != nil {
					utils.FailTask(fmt.Sprintf("%s-Cluster creation failed for %s", ctxt, request.Name), err, t)
					setClusterState(*cluster_uuid, models.CLUSTER_STATE_FAILED, ctxt)
					return
				}

				if ret_val {
					var failedMons, succeededMons []string
					succeededMons = append(succeededMons, mons[0].Node)
					// Add other mons
					if len(mons) > 1 {
						t.UpdateStatus("Adding mons")
						for _, mon := range mons[1:] {
							if ret_val, err := salt_backend.AddMon(
								request.Name,
								[]backend.Mon{mon},
								ctxt); err != nil || !ret_val {
								failedMons = append(failedMons, mon.Node)
							} else {
								succeededMons = append(succeededMons, mon.Node)
								t.UpdateStatus(fmt.Sprintf("Added mon node: %s", mon.Node))
							}
						}
					}
					if len(failedMons) > 0 {
						t.UpdateStatus(fmt.Sprintf("Failed to add mon(s) %v", failedMons))
					}

					t.UpdateStatus("Updating node details for cluster")
					// Update nodes details
					coll1 := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
					for _, node := range nodes {
						if err := coll1.Update(
							bson.M{"nodeid": node.NodeId},
							bson.M{"$set": bson.M{
								"clusterip4": node_ips[node.NodeId]["cluster"],
								"publicip4":  node_ips[node.NodeId]["public"]}}); err != nil {
							logger.Get().Error(
								"%s-Error updating the details for node: %s. error: %v",
								ctxt,
								node.Hostname,
								err)
							t.UpdateStatus(fmt.Sprintf("Failed to update details of node: %s", node.Hostname))
						}
					}

					// Start and persist the mons
					t.UpdateStatus("Starting and persisting mons")
					ret_val, err = startAndPersistMons(*cluster_uuid, succeededMons, ctxt)
					if !ret_val || err != nil {
						logger.Get().Error(
							"%s-Error starting/persisting mons. error: %v",
							ctxt,
							err)
						t.UpdateStatus("Failed to start/persist mons")
					}

					// Add OSDs
					t.UpdateStatus("Getting updated nodes list for OSD creation")
					updated_nodes, err := util.GetNodes(request.Nodes)
					if err != nil {
						utils.FailTask(fmt.Sprintf("%s-Error getting updated nodes list post create cluster %s", ctxt, request.Name), err, t)
						return
					}
					t.UpdateStatus("Adding OSDs")
					failedOSDs, succeededOSDs := addOSDs(
						*cluster_uuid,
						request.Name,
						updated_nodes,
						request.Nodes,
						t,
						ctxt)
					if len(succeededOSDs) == 0 {
						utils.FailTask(fmt.Sprintf(
							"%s-Failed adding all OSDs while create cluster %s",
							ctxt,
							request.Name),
							err,
							t)
						return
					}
					if len(failedOSDs) != 0 {
						var osds []string
						for _, osd := range failedOSDs {
							osds = append(osds, fmt.Sprintf("%s:%s", osd.Node, osd.Device))
						}
						t.UpdateStatus(fmt.Sprintf("OSD addition failed for %v", osds))
					}

					// Update the cluster status at the last
					clusterStatus, err := cluster_status(*cluster_uuid, request.Name, ctxt)
					if err := coll.Update(
						bson.M{"clusterid": *cluster_uuid},
						bson.M{"$set": bson.M{
							"status": clusterStatus,
							"state":  models.CLUSTER_STATE_ACTIVE}}); err != nil {
						t.UpdateStatus("Error updating the cluster status")
						return
					}

					// Delete the default created pool "rbd"
					t.UpdateStatus("Removing default created pool \"rbd\"")
					monnode, err := GetRandomMon(*cluster_uuid)
					if err != nil {
						logger.Get().Error("%s-Could not get random mon for default pool deletion", ctxt)
						t.UpdateStatus("Could not get random mon for default pool deletion")
						// No need to fail the cluster creation task
						t.UpdateStatus("Success")
						t.Done(models.TASK_STATUS_SUCCESS)
						return
					}
					// First pool in the cluster so poolid = 0
					ok, err := cephapi_backend.RemovePool(monnode.Hostname, *cluster_uuid, request.Name, "rbd", 0, ctxt)
					if err != nil || !ok {
						// Wait and try once more
						time.Sleep(10 * time.Second)
						ok, err := cephapi_backend.RemovePool(monnode.Hostname, *cluster_uuid, request.Name, "rbd", 0, ctxt)
						if err != nil || !ok {
							logger.Get().Warning("%s - Could not delete the default create pool \"rbd\" for cluster: %s", ctxt, request.Name)
							t.UpdateStatus("Could not delete the default create pool \"rbd\"")
						}
					}

					// Create default EC profiles
					t.UpdateStatus("Creating default EC profiles")
					if ok, err := CreateDefaultECProfiles(ctxt, monnode.Hostname, *cluster_uuid); !ok || err != nil {
						logger.Get().Error("%s-Error creating default EC profiles for cluster: %s. error: %v", ctxt, request.Name, err)
						t.UpdateStatus("Could not create default EC profile")
					}

					//Update the CRUSH MAP
					t.UpdateStatus("Updating the CRUSH Map")
					if err := updateCrushMap(ctxt, monnode.Hostname, *cluster_uuid); !ok || err != nil {
						logger.Get().Error("%s-Error updating the Crush map for cluster: %s. error: %v", ctxt, request.Name, err)
						t.UpdateStatus("Failed to update Crush map")
					}

					t.UpdateStatus("Success")
					t.Done(models.TASK_STATUS_SUCCESS)
					return
				}
			}
		}
	}
	if taskId, err := bigfin_task.GetTaskManager().Run(
		bigfin_conf.ProviderName,
		"CEPH-CreateCluster",
		asyncTask,
		nil,
		nil,
		nil); err != nil {
		logger.Get().Error("%s - Task creation failed for create cluster %s. error: %v", ctxt, request.Name, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Task creation failed for create cluster %s", request.Name))
		return err
	} else {
		*resp = utils.WriteAsyncResponse(taskId, fmt.Sprintf("Task Created for create cluster: %s", request.Name), []byte{})
	}
	return nil
}

func setClusterState(clusterId uuid.UUID, state models.ClusterState, ctxt string) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Update(bson.M{"clusterid": clusterId}, bson.M{"$set": bson.M{"state": state}}); err != nil {
		logger.Get().Warning("%s-Error updating the state for cluster: %v", ctxt, clusterId)
	}
}

func setClusterStatus(clusterId uuid.UUID, status models.ClusterStatus, ctxt string) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Update(bson.M{"clusterid": clusterId}, bson.M{"$set": bson.M{"status": status}}); err != nil {
		logger.Get().Warning("%s-Error updating the status for cluster: %v", ctxt, clusterId)
	}
}

func nodeIPs(networks models.ClusterNetworks, nodes map[uuid.UUID]models.Node) (map[uuid.UUID]map[string]string, error) {
	var node_ips = make(map[uuid.UUID]map[string]string)
	for nodeid, node := range nodes {
		host_addrs := node.NetworkInfo.IPv4
		var m = make(map[string]string)
		for _, host_addr := range host_addrs {
			if ok, _ := utils.IsIPInSubnet(host_addr, networks.Cluster); ok {
				m["cluster"] = host_addr
			}
			if ok, _ := utils.IsIPInSubnet(host_addr, networks.Public); ok {
				m["public"] = host_addr
			}
			if m["cluster"] != "" && m["public"] != "" {
				node_ips[nodeid] = m
				break
			}
		}
		if _, ok := node_ips[nodeid]; !ok {
			node_ips[nodeid] = map[string]string{
				"public":  node.ManagementIP4,
				"cluster": node.ManagementIP4,
			}
		}
	}
	return node_ips, nil
}

func startAndPersistMons(clusterId uuid.UUID, mons []string, ctxt string) (bool, error) {
	if ok, err := salt_backend.StartMon(mons, ctxt); err != nil || !ok {
		return false, err
	}

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	for _, mon := range mons {
		if err := coll.Update(
			bson.M{"hostname": mon},
			bson.M{"$set": bson.M{
				"clusterid":   clusterId,
				"options.mon": "Y"}}); err != nil {
			return false, err
		}
		logger.Get().Info(fmt.Sprintf("%s-Added mon node: %s", ctxt, mon))
	}
	return true, nil
}

func addOSDs(clusterId uuid.UUID, clusterName string, nodes map[uuid.UUID]models.Node, requestNodes []models.ClusterNode, t *task.Task, ctxt string) ([]backend.OSD, []backend.OSD) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	updatedStorageDisksMap := make(map[uuid.UUID][]models.Disk)
	var (
		failedOSDs    []backend.OSD
		succeededOSDs []backend.OSD
		slus          = make(map[string]models.StorageLogicalUnit)
	)
	for _, requestNode := range requestNodes {
		if util.StringInSlice(models.NODE_TYPE_OSD, requestNode.NodeType) {
			var updatedStorageDisks []models.Disk
			uuid, err := uuid.Parse(requestNode.NodeId)
			if err != nil {
				logger.Get().Error(
					"%s-Error parsing node id: %s while add OSD for cluster: %s. error: %v",
					ctxt,
					requestNode.NodeId,
					clusterName,
					err)
				t.UpdateStatus(fmt.Sprintf("Failed to add OSD(s) from node: %v", requestNode.NodeId))
				continue
			}
			storageNode := nodes[*uuid]
			for _, storageDisk := range storageNode.StorageDisks {
				for _, device := range requestNode.Devices {
					if storageDisk.Name == device.Name && !storageDisk.Used {
						if device.FSType == "" {
							device.FSType = models.DEFAULT_FS_TYPE
						}
						var osd = backend.OSD{
							Node:       storageNode.Hostname,
							PublicIP4:  storageNode.PublicIP4,
							ClusterIP4: storageNode.ClusterIP4,
							Device:     device.Name,
							FSType:     device.FSType,
						}
						osds, err := salt_backend.AddOSD(clusterName, osd, ctxt)
						if err != nil {
							failedOSDs = append(failedOSDs, osd)
							break
						}
						osdName := osds[storageNode.Hostname][0]
						var options = make(map[string]interface{})
						options["node"] = osd.Node
						options["publicip4"] = osd.PublicIP4
						options["clusterip4"] = osd.ClusterIP4
						options["device"] = osd.Device
						options["fstype"] = osd.FSType
						slu := models.StorageLogicalUnit{
							Name:              osdName,
							Type:              models.CEPH_OSD,
							ClusterId:         clusterId,
							NodeId:            storageNode.NodeId,
							StorageDeviceId:   storageDisk.DiskId,
							StorageProfile:    storageDisk.StorageProfile,
							StorageDeviceSize: storageDisk.Size,
							Options:           options,
							Status:            models.SLU_STATUS_UNKNOWN,
							State:             bigfinmodels.OSD_STATE_IN,
							AlmStatus:         models.ALARM_STATUS_CLEARED,
						}
						if ok, err := persistOSD(slu, t, ctxt); err != nil || !ok {
							logger.Get().Error("%s-Error persising %s for cluster: %s. error: %v", ctxt, slu.Name, clusterName, err)
							failedOSDs = append(failedOSDs, osd)
							break
						}
						slus[osdName] = slu
						succeededOSDs = append(succeededOSDs, osd)
						storageDisk.Used = true
					}
					updatedStorageDisks = append(updatedStorageDisks, storageDisk)
				}
			}
			updatedStorageDisksMap[storageNode.NodeId] = updatedStorageDisks
		}
	}

	// Update the storage disks as used
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	for nodeid, updatedStorageDisks := range updatedStorageDisksMap {
		if err := coll.Update(
			bson.M{"nodeid": nodeid},
			bson.M{"$set": bson.M{
				"clusterid":    clusterId,
				"storagedisks": updatedStorageDisks}}); err != nil {
			logger.Get().Error(
				"%s-Error updating disks for node: %v post add OSDs for cluster: %s. error: %v",
				ctxt,
				nodeid,
				clusterName,
				err)
		}
	}
	t.UpdateStatus("Syncing the OSD status")
	/*
	 TODO: Sleep will be removed once the events are vailable
	 from calamari on OSD status change. Immeadiately after the
	 the creation the OSD sttaus set to out and down, so wait for
	 sometime to get the right status
	*/
	time.Sleep(60 * time.Second)
	for count := 0; count < 3; count++ {
		if err := syncOsdDetails(clusterId, slus, ctxt); err != nil || len(slus) > 0 {
			logger.Get().Warning(
				"%s-Error syncing the OSD status. error: %v",
				ctxt,
				err)
			time.Sleep(10 * time.Second)
		} else {
			break
		}
	}
	return failedOSDs, succeededOSDs
}

func persistOSD(slu models.StorageLogicalUnit, t *task.Task, ctxt string) (bool, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
	if err := coll.Insert(slu); err != nil {
		return false, err
	}
	logger.Get().Info(
		fmt.Sprintf("%s-Added %s (%s %s) for cluster: %v",
			ctxt,
			slu.Name,
			slu.Options["node"],
			slu.Options["device"],
			slu.ClusterId))
	t.UpdateStatus(
		"Added %s (%s %s)",
		slu.Name,
		slu.Options["node"],
		slu.Options["device"])

	return true, nil
}

func (s *CephProvider) ExpandCluster(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext

	cluster_id_str := req.RpcRequestVars["cluster-id"]
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("%s-Error parsing the cluster id: %s. error: %v", ctxt, cluster_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s. error: %v", cluster_id_str, err))
		return err
	}

	var new_nodes []models.ClusterNode
	if err := json.Unmarshal(req.RpcRequestData, &new_nodes); err != nil {
		logger.Get().Error("%s-Unbale to parse the request. error: %v", ctxt, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request. error: %v", err))
		return err
	}

	// Get corresponding nodes from DB
	nodes, err := util.GetNodes(new_nodes)
	if err != nil {
		logger.Get().Error("%s-Error getting the nodes from DB for cluster: %v. error: %v", ctxt, *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error getting the nodes from DB. error: %v", err))
		return err
	}

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	var cluster models.Cluster
	if err := coll.Find(bson.M{"clusterid": *cluster_id}).One(&cluster); err != nil {
		logger.Get().Error("%s-Error getting cluster details for %v. error: %v", ctxt, *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error getting cluster details. error: %v", err))
		return nil
	}

	// Get the cluster and public IPs for nodes
	node_ips, err := nodeIPs(cluster.Networks, nodes)
	if err != nil {
		logger.Get().Error("%s-Node IP does not fall in provided subnets for cluster: %v. error: %v", ctxt, *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Node IP does fall in provided subnets. error: %v", err))
		return nil
	}

	var mons []backend.Mon
	for _, new_node := range new_nodes {
		if util.StringInSlice("MON", new_node.NodeType) {
			var mon backend.Mon
			nodeid, _ := uuid.Parse(new_node.NodeId)
			mon.Node = nodes[*nodeid].Hostname
			mon.PublicIP4 = node_ips[*nodeid]["public"]
			mon.ClusterIP4 = node_ips[*nodeid]["cluster"]
			mons = append(mons, mon)
		}
	}

	// If mon node already exists for the cluster, error out
	for _, new_node := range new_nodes {
		if util.StringInSlice("MON", new_node.NodeType) {
			coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
			nodeid, err := uuid.Parse(new_node.NodeId)
			if err != nil {
				logger.Get().Error("%s-Error parsing the node id: %s while expand cluster: %v. error: %v", new_node.NodeId, ctxt, *cluster_id, err)
				*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the node id: %s", new_node.NodeId))
				return err
			}
			var monNode models.Node
			// No need to check for error here as in case of error, node instance would not be populated
			// and the same already being checked below
			_ = coll.Find(bson.M{"clusterid": *cluster_id, "nodeid": *nodeid}).One(&monNode)
			if monNode.Hostname != "" {
				logger.Get().Error("%s-Mon %v already exists for cluster: %v", ctxt, *nodeid, *cluster_id)
				*resp = utils.WriteResponse(http.StatusInternalServerError, "The mon node already available")
				return errors.New(fmt.Sprintf("Mon %v already exists for cluster: %v", *nodeid, *cluster_id))
			}
		}
	}

	asyncTask := func(t *task.Task) {
		sessionCopy := db.GetDatastore().Copy()
		defer sessionCopy.Close()
		for {
			select {
			case <-t.StopCh:
				return
			default:
				t.UpdateStatus("Started ceph provider task for cluster expansion: %v", t.ID)
				var failedMons, succeededMons []string
				if len(mons) > 0 {
					t.UpdateStatus("Adding mons")
					for _, mon := range mons {
						if ret_val, err := salt_backend.AddMon(
							cluster.Name,
							[]backend.Mon{mon},
							ctxt); err != nil || !ret_val {
							failedMons = append(failedMons, mon.Node)
						} else {
							succeededMons = append(succeededMons, mon.Node)
							t.UpdateStatus(fmt.Sprintf("Added mon node: %s", mon.Node))
						}
					}
				}
				if len(failedMons) > 0 {
					t.UpdateStatus(fmt.Sprintf("Failed to add mon(s) %v", failedMons))
				}

				t.UpdateStatus("Updating node details for cluster")
				// Update nodes details
				coll1 := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
				for _, node := range nodes {
					if err := coll1.Update(
						bson.M{"nodeid": node.NodeId},
						bson.M{"$set": bson.M{
							"clusterip4": node_ips[node.NodeId]["cluster"],
							"publicip4":  node_ips[node.NodeId]["public"]}}); err != nil {
						logger.Get().Error(
							"%s-Error updating the details for node: %s. error: %v",
							ctxt,
							node.Hostname,
							err)
						t.UpdateStatus(fmt.Sprintf("Failed to update details of node: %s", node.Hostname))
					}
				}
				// Start and persist the mons
				t.UpdateStatus("Starting and persisting mons")
				ret_val, err := startAndPersistMons(*cluster_id, succeededMons, ctxt)
				if !ret_val || err != nil {
					logger.Get().Error(
						"%s-Error starting/persisting mons. error: %v",
						ctxt,
						err)
					t.UpdateStatus("Failed to start/persist mons")
				}

				// Add OSDs
				t.UpdateStatus("Getting updated nodes for OSD creation")
				updated_nodes, err := util.GetNodes(new_nodes)
				if err != nil {
					utils.FailTask(fmt.Sprintf(
						"Error getting updated nodes while expand cluster: %v",
						*cluster_id),
						fmt.Errorf("%s-%v", ctxt, err),
						t)
					return
				}
				t.UpdateStatus("Adding OSDs")
				failedOSDs, succeededOSDs := addOSDs(
					*cluster_id,
					cluster.Name,
					updated_nodes,
					new_nodes,
					t,
					ctxt)
				if len(succeededOSDs) == 0 {
					utils.FailTask(
						fmt.Sprintf("Failed to add all OSDs while expand cluster: %v", *cluster_id),
						fmt.Errorf("%s-%v", ctxt, err),
						t)
					return
				}
				if len(failedOSDs) != 0 {
					var osds []string
					for _, osd := range failedOSDs {
						osds = append(osds, fmt.Sprintf("%s:%s", osd.Node, osd.Device))
					}
					t.UpdateStatus(fmt.Sprintf("OSD addition failed for %v", osds))
				}

				t.UpdateStatus("Recalculating pgnum/pgpnum")
				if ok := RecalculatePgnum(ctxt, *cluster_id, t); !ok {
					logger.Get().Warning(
						"%s-Could not re-calculate pgnum/pgpnum for cluster: %v",
						ctxt,
						*cluster_id)
				}
				t.UpdateStatus("Success")
				t.Done(models.TASK_STATUS_SUCCESS)
				return
			}
		}
	}

	if taskId, err := bigfin_task.GetTaskManager().Run(
		bigfin_conf.ProviderName,
		"CEPH-ExpandCluster",
		asyncTask,
		nil,
		nil,
		nil); err != nil {
		logger.Get().Error("%s-Task creation failed for exoand cluster: %v. error: %v", ctxt, *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, "Task creation failed for cluster expansion")
		return err
	} else {
		*resp = utils.WriteAsyncResponse(taskId, fmt.Sprintf("Task Created for expand cluster: %v", *cluster_id), []byte{})
	}

	return nil
}

func (s *CephProvider) GetClusterStatus(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext

	cluster_id_str := req.RpcRequestVars["cluster-id"]
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("%s-Error parsing the cluster id: %s. error: %v", ctxt, cluster_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s", cluster_id_str))
		return err
	}
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	// Get cluster details
	var cluster models.Cluster
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Find(bson.M{"clusterid": *cluster_id}).One(&cluster); err != nil {
		logger.Get().Error("%s-Error fetching details of cluster: %v. error: %v", ctxt, *cluster_id, err)
		return err
	}

	status, err := cluster_status(*cluster_id, cluster.Name, ctxt)
	if err != nil {
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("error: %v", err))
	} else {
		intStatus := int(status)
		*resp = utils.WriteResponseWithData(http.StatusOK, "", []byte(strconv.Itoa(intStatus)))
	}
	return nil
}

func (s *CephProvider) UpdateStorageLogicalUnitParams(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext

	cluster_id_str, ok := req.RpcRequestVars["cluster-id"]
	if !ok {
		logger.Get().Error("%s- Cluster-id is not provided along with request")
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Cluster-id is not provided along with request"))
		return errors.New("Cluster-id is not provided along with request")
	}
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("%s-Error parsing the cluster id: %s. error: %v", ctxt, cluster_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s", cluster_id_str))
		return err
	}
	slu_id_str, ok := req.RpcRequestVars["slu-id"]
	if !ok {
		logger.Get().Error("%s- slu-id is not provided along with request")
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("slu-id is not provided along with request"))
		return errors.New("slu-id is not provided along with request")
	}
	slu_id, err := uuid.Parse(slu_id_str)
	if err != nil {
		logger.Get().Error("%s-Error parsing the cluster id: %s. error: %v", ctxt, cluster_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s", cluster_id_str))
		return err
	}

	var upreq map[string]interface{}
	if err := json.Unmarshal(req.RpcRequestData, &upreq); err != nil {
		logger.Get().Error(fmt.Sprintf("%s-Unbale to parse the update slu params request for %s. error: %v", ctxt, slu_id_str, err))
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unable to parse the request. error: %v", err))
		return err
	}

	//make sure we dealing with only the supported parameters
	osdData := make(map[string]interface{})

	if in, ok := upreq["in"]; ok {
		osdData["in"] = in
	}
	if up, ok := upreq["up"]; ok {
		osdData["up"] = up
	}

	if len(osdData) == 0 {
		logger.Get().Error("%s-No valid data provided to update", ctxt)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("No valid data provided to update"))
		return err
	}

	// Get a random mon node
	monnode, err := GetRandomMon(*cluster_id)
	if err != nil {
		logger.Get().Error("%s-Error getting a mon node in cluster: %s. error: %v", ctxt, *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error getting a mon node in cluster: %s. error: %v", *cluster_id, err))
		return err
	}

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
	var slu models.StorageLogicalUnit
	if err := coll.Find(bson.M{"sluid": *slu_id}).One(&slu); err != nil {
		logger.Get().Error("%s-Error fetching details of slu: %v. error: %v", ctxt, *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error fetching details of slu: %v. error: %v", *cluster_id, err))
		return err
	}
	osdId := strings.Split(slu.Name, ".")[1]

	asyncTask := func(t *task.Task) {
		for {
			select {
			case <-t.StopCh:
				return
			default:
				t.UpdateStatus("Started updating osd params: %v", t.ID)
				ok, err := cephapi_backend.UpdateOSD(monnode.Hostname, *cluster_id, osdId, osdData, ctxt)
				if err != nil || !ok {
					utils.FailTask(fmt.Sprintf("Could not update osd params for slu: %s of cluster: %v", slu_id, cluster_id), err, t)
					return
				}
				//Now update the latest status from calamari
				fetchedOSD, err := cephapi_backend.GetOSD(monnode.Hostname, *cluster_id, osdId, ctxt)
				if err != nil {
					utils.FailTask(fmt.Sprintf("Error getting OSD details for cluster: %v.", cluster_id), err, t)
					return
				}
				status := mapOsdStatus(fetchedOSD.Up, fetchedOSD.In)
				state := mapOsdState(fetchedOSD.In)
				slu.Options["in"] = strconv.FormatBool(fetchedOSD.In)
				slu.Options["up"] = strconv.FormatBool(fetchedOSD.Up)
				slu.State = state
				slu.Status = status

				sessionCopy := db.GetDatastore().Copy()
				defer sessionCopy.Close()
				coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)

				if err := coll.Update(bson.M{"sluid": fetchedOSD.Uuid, "clusterid": cluster_id}, slu); err != nil {
					utils.FailTask(fmt.Sprintf("Error updating the details for slu: %s.", slu.Name), err, t)
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
		"CEPH-UpdateOSD",
		asyncTask,
		nil,
		nil,
		nil); err != nil {
		logger.Get().Error("Task creation failed for update OSD %s on cluster: %v. error: %v", *slu_id, *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, "Task creation failed for update OSD")
		return err
	} else {
		*resp = utils.WriteAsyncResponse(taskId, fmt.Sprintf("Task Created for update OSD %s on cluster: %v", *slu_id, *cluster_id), []byte{})
	}
	return nil
}

func (s *CephProvider) SyncStorageLogicalUnitParams(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext

	cluster_id_str, ok := req.RpcRequestVars["cluster-id"]
	if !ok {
		logger.Get().Error("%s- Cluster-id is not provided along with request")
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Cluster-id is not provided along with request"))
		return errors.New("Cluster-id is not provided along with request")
	}
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("%s-Error parsing the cluster id: %s. error: %v", ctxt, cluster_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s", cluster_id_str))
		return err
	}

	if err := SyncOsdStatus(*cluster_id, ctxt); err != nil {
		logger.Get().Error("%s-Error syncing the OSD status. Err: %v", ctxt, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error syncing the OSD status. Err: %v", err))
		return err
	}
	*resp = utils.WriteResponse(http.StatusOK, "")
	return nil

}

func cluster_status(clusterId uuid.UUID, clusterName string, ctxt string) (models.ClusterStatus, error) {
	// Pick a random mon from the list
	monnode, err := GetRandomMon(clusterId)
	if err != nil {
		logger.Get().Error("%s-Error getting a mon from cluster: %s. error: %v", ctxt, clusterName, err)
		return models.CLUSTER_STATUS_UNKNOWN, errors.New(fmt.Sprintf("Error getting a mon. error: %v", err))
	}

	// Get the cluser status
	status, err := salt_backend.GetClusterStatus(monnode.Hostname, clusterId, clusterName, ctxt)
	if err != nil {
		logger.Get().Error("%s-Could not get up status of cluster: %v. error: %v", ctxt, clusterName, err)
		return models.CLUSTER_STATUS_UNKNOWN, err
	}
	if val, ok := cluster_status_map[status]; ok {
		return val, nil
	} else {
		return models.CLUSTER_STATUS_UNKNOWN, nil
	}
}

func RecalculatePgnum(ctxt string, clusterId uuid.UUID, t *task.Task) bool {
	// Get storage pools
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	var storages []models.Storage
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
	if err := coll.Find(bson.M{"clusterid": clusterId}).All(&storages); err != nil {
		utils.FailTask(fmt.Sprintf("Error getting storage pools for cluster: %v", clusterId), fmt.Errorf("%s-%v", ctxt, err), t)
		return false
	}

	t.UpdateStatus("Getting a mon from cluster")
	monnode, err := GetRandomMon(clusterId)
	if err != nil {
		utils.FailTask(fmt.Sprintf("Error getting mon node for cluster: %v", clusterId), fmt.Errorf("%s-%v", ctxt, err), t)
		return false
	}

	for _, storage := range storages {
		if storage.Name == "rbd" {
			continue
		}
		var pgNum uint
		if storage.Type == models.STORAGE_TYPE_ERASURE_CODED {
			pgNum = DerivePgNum(clusterId, storage.Size, ec_pool_sizes[storage.Options["ecprofile"]])
		} else {
			pgNum = DerivePgNum(clusterId, storage.Size, storage.Replicas)
		}
		currentPgNum, err := strconv.Atoi(storage.Options["pgnum"])
		if err != nil {
			utils.FailTask(fmt.Sprintf("Error getting details of pool: %s for cluster: %v", storage.Name, clusterId), fmt.Errorf("%s-%v", ctxt, err), t)
			return false
		}
		if pgNum == uint(currentPgNum) {
			logger.Get().Info("No change in PgNum .. Continuing ..")
			continue
		}
		id, err := strconv.Atoi(storage.Options["id"])
		if err != nil {
			utils.FailTask(fmt.Sprintf("Error getting details of pool: %s for cluster: %v", storage.Name, clusterId), fmt.Errorf("%s-%v", ctxt, err), t)
			return false
		}
		// Update the PG Num for the cluster
		t.UpdateStatus(fmt.Sprintf("Updating the pgnum and pgpnum for pool %s", storage.Name))
		poolData := map[string]interface{}{
			"pg_num":  int(pgNum),
			"pgp_num": int(pgNum),
		}
		ok, err := cephapi_backend.UpdatePool(monnode.Hostname, clusterId, id, poolData, ctxt)
		if err != nil || !ok {
			t.UpdateStatus(fmt.Sprintf("Could not update pgnum/pgnum for pool: %s of cluster: %v", storage.Name, clusterId))
		}
	}
	return true
}

func syncOsdDetails(clusterId uuid.UUID, slus map[string]models.StorageLogicalUnit, ctxt string) error {
	// Get a random mon node
	monnode, err := GetRandomMon(clusterId)
	if err != nil {
		logger.Get().Error(
			"%s-Error getting a mon node in cluster: %s. error: %v",
			ctxt,
			clusterId,
			err)
		return err
	}

	fetchedOSDs, err := cephapi_backend.GetOSDs(monnode.Hostname, clusterId, ctxt)
	if err != nil {
		logger.Get().Error(
			"%s-Error getting OSD details for cluster: %v. error: %v",
			ctxt,
			clusterId,
			err)
		return err
	}
	pgSummary, err := cephapi_backend.GetPGSummary(monnode.Hostname, clusterId, ctxt)
	if err != nil {
		logger.Get().Error("%s-Error getting pg summary for cluster: %v. error: %v", ctxt, clusterId, err)
		return err
	}

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
	for _, slu := range slus {
		for _, fetchedOSD := range fetchedOSDs {
			id, err := strconv.Atoi(strings.Split(slu.Name, ".")[1])
			if err != nil {
				logger.Get().Error(
					"%s-Error getting OSD id from name: %s. error: %v",
					ctxt,
					slu.Name,
					err)
				return err
			}
			if fetchedOSD.Id == id {
				status := mapOsdStatus(fetchedOSD.Up, fetchedOSD.In)
				state := mapOsdState(fetchedOSD.In)
				slu.Options["in"] = strconv.FormatBool(fetchedOSD.In)
				slu.Options["up"] = strconv.FormatBool(fetchedOSD.Up)
				slu.Options["pgsummary"] = pgSummary.ByOSD[strconv.Itoa(fetchedOSD.Id)]
				slu.State = state
				slu.Status = status
				slu.SluId = fetchedOSD.Uuid
				if err := coll.Update(
					bson.M{"name": slu.Name, "clusterid": clusterId},
					slu); err != nil {
					logger.Get().Error(
						"%s-Error updating the details for slu: %s. error: %v",
						ctxt,
						slu.Name,
						err)
					return err
				}
				delete(slus, slu.Name)
			}
		}
	}
	return nil
}

func SyncOsdStatus(clusterId uuid.UUID, ctxt string) error {

	// Get a random mon node
	monnode, err := GetRandomMon(clusterId)
	if err != nil {
		logger.Get().Error("%s-Error getting a mon node in cluster: %s. error: %v", ctxt, clusterId, err)
		return err
	}

	fetchedOSDs, err := cephapi_backend.GetOSDs(monnode.Hostname, clusterId, ctxt)
	if err != nil {
		logger.Get().Error("%s-Error getting OSD details for cluster: %v. error: %v", ctxt, clusterId, err)
		return err
	}
	pgSummary, err := cephapi_backend.GetPGSummary(monnode.Hostname, clusterId, ctxt)
	if err != nil {
		logger.Get().Error("%s-Error getting pg summary for cluster: %v. error: %v", ctxt, clusterId, err)
		return err
	}

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
	var slu models.StorageLogicalUnit
	for _, fetchedOSD := range fetchedOSDs {

		if err := coll.Find(bson.M{"sluid": fetchedOSD.Uuid}).One(&slu); err != nil {
			if err != mgo.ErrNotFound {
				logger.Get().Error("%s-Error fetching details of slu: %v. error: %v", ctxt, clusterId, err)
				return err
			}
			continue
		}

		status := mapOsdStatus(fetchedOSD.Up, fetchedOSD.In)
		state := mapOsdState(fetchedOSD.In)
		slu.Options["in"] = strconv.FormatBool(fetchedOSD.In)
		slu.Options["up"] = strconv.FormatBool(fetchedOSD.Up)
		slu.Options["pgsummary"] = pgSummary.ByOSD[strconv.Itoa(fetchedOSD.Id)]
		slu.State = state
		slu.Status = status
		if err := coll.Update(
			bson.M{"sluid": fetchedOSD.Uuid, "clusterid": clusterId},
			bson.M{"$set": bson.M{"status": status, "state": state, "options": slu.Options}}); err != nil {
			if err != mgo.ErrNotFound {
				logger.Get().Error("%s-Error updating the status for slu: %s. error: %v", ctxt, fetchedOSD.Uuid.String(), err)
				return err
			}
		}
	}

	return nil
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

func updateCrushMap(ctxt string, mon string, clusterId uuid.UUID) error {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
	scoll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_PROFILE)

	var sProfiles []models.StorageProfile
	ruleSets := make(map[string]interface{})

	//Get the stoarge profiles
	if err := scoll.Find(nil).All(&sProfiles); err != nil {
		logger.Get().Error("Error getting the storageprofiles error: %v", err)
		return err
	}
	for _, sprof := range sProfiles {
		//Get the OSDs per storageprofiles
		var slus []models.StorageLogicalUnit
		if err := coll.Find(bson.M{"storageprofile": sprof.Name}).All(&slus); err != nil {
			logger.Get().Error("Error getting the slus for cluster: %s. error: %v", clusterId, err)
			continue
		}
		if len(slus) == 0 {
			continue
		}
		//create crush nodes
		cNode := backend.CrushNodeRequest{BucketType: "host", Name: sprof.Name}
		var pos int
		for _, slu := range slus {
			id, _ := strconv.Atoi(strings.Split(slu.Name, `.`)[1])
			item := backend.CrushItem{Id: id, Pos: pos}
			cNode.Items = append(cNode.Items, item)
			pos = pos + 1
		}
		cNodeId, err := cephapi_backend.CreateCrushNode(mon, clusterId, cNode, ctxt)
		if err != nil {
			logger.Get().Error("Failed to create Crush node for cluster: %s. error: %v", clusterId, err)
			continue
		}

		//create crush rule
		ruleSetId := RULEOFFSET + (0 - cNodeId)
		cRule := backend.CrushRuleRequest{Name: sprof.Name, RuleSet: ruleSetId, Type: "replicated", MinSize: 1, MaxSize: 10}
		step_take := make(map[string]interface{})
		step_take["item_name"] = cNode.Name
		step_take["item"] = cNodeId
		step_take["take"] = "op"
		cRule.Steps = append(cRule.Steps, step_take)
		leaf := make(map[string]interface{})
		leaf["num"] = 0
		leaf["type"] = "host"
		leaf["op"] = "chooseleaf_firstn"
		cRule.Steps = append(cRule.Steps, leaf)
		emit := make(map[string]interface{})
		emit["op"] = "emit"
		cRule.Steps = append(cRule.Steps, emit)
		if err := cephapi_backend.CreateCrushRule(mon, clusterId, cRule, ctxt); err != nil {
			logger.Get().Error("Failed to create Crush rule for cluster: %s. error: %v", clusterId, err)
			continue
		}
		ruleSets[sprof.Name] = ruleSetId
	}
	//update the cluster with this rulesets
	cluster, err := getCluster(clusterId)
	if err != nil {
		logger.Get().Error("Failed to get details of cluster: %s. error: %v", clusterId, err)
		return err
	}
	cluster.Options["rulesetmap"] = ruleSets
	ccoll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := ccoll.Update(
		bson.M{"clusterid": clusterId}, bson.M{"$set": bson.M{"options": cluster.Options}}); err != nil {
		logger.Get().Error("%s-Error updating the cluster: %s. error: %v", ctxt, clusterId, err)
		return err

	}
	return nil
}
