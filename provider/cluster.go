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
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/skyring-common/conf"
	"github.com/skyrings/skyring-common/db"
	"github.com/skyrings/skyring-common/models"
	"github.com/skyrings/skyring-common/monitoring"
	"github.com/skyrings/skyring-common/tools/logger"
	"github.com/skyrings/skyring-common/tools/task"
	"github.com/skyrings/skyring-common/tools/uuid"
	"gopkg.in/mgo.v2/bson"
	"net/http"
	"strconv"
	"time"

	bigfin_task "github.com/skyrings/bigfin/tools/task"
)

var (
	cluster_status_map = map[string]string{
		"HEALTH_OK":   models.STATUS_OK,
		"HEALTH_WARN": models.STATUS_WARN,
		"HEALTH_ERR":  models.STATUS_ERR,
	}
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
	nodes, err := getNodes(request.Nodes)
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
		if utils.StringInSlice("MON", req_node.NodeType) {
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
		for {
			select {
			case <-t.StopCh:
				return
			default:
				t.UpdateStatus("Started ceph provider task for cluster creation: %v", t.ID)
				sessionCopy := db.GetDatastore().Copy()
				defer sessionCopy.Close()

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

				cluster.MonitoringInterval = request.MonitoringInterval
				if cluster.MonitoringInterval == 0 {
					cluster.MonitoringInterval = monitoring.DefaultClusterMonitoringInterval
				}
				coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
				if err := coll.Insert(cluster); err != nil {
					utils.FailTask(fmt.Sprintf("%s-Error persisting the cluster %s", ctxt, request.Name), err, t)
					return
				}

				ret_val, err := salt_backend.CreateCluster(request.Name, *cluster_uuid, []backend.Mon{mons[0]}, ctxt)
				if err != nil {
					utils.FailTask(fmt.Sprintf("%s-Cluster creation failed for %s", ctxt, request.Name), err, t)
					setClusterState(*cluster_uuid, models.CLUSTER_STATE_FAILED, ctxt)
					return
				}

				if ret_val {
					// Add other mons
					t.UpdateStatus("Adding mons")
					if len(mons) > 1 {
						for _, mon := range mons[1:] {
							if ret_val, err := salt_backend.AddMon(request.Name, []backend.Mon{mon}, ctxt); err != nil || !ret_val {
								utils.FailTask(fmt.Sprintf("%s-Error adding mons while create cluster %s", ctxt, request.Name), err, t)
								setClusterState(*cluster_uuid, models.CLUSTER_STATE_FAILED, ctxt)
								return
							}
						}
					}

					t.UpdateStatus("Updating node details for cluster")
					// Update nodes details
					for _, node := range nodes {
						coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
						if err := coll.Update(
							bson.M{"nodeid": node.NodeId},
							bson.M{"$set": bson.M{
								"clusterid":  *cluster_uuid,
								"clusterip4": node_ips[node.NodeId]["cluster"],
								"publicip4":  node_ips[node.NodeId]["public"]}}); err != nil {
							utils.FailTask(fmt.Sprintf("%s-Failed to update nodes details post create cluster %s", ctxt, request.Name), err, t)
							setClusterState(*cluster_uuid, models.CLUSTER_STATE_FAILED, ctxt)
							return
						}
					}

					// Start and persist the mons
					t.UpdateStatus("Starting and creating mons")
					ret_val, err = startAndPersistMons(*cluster_uuid, mons, ctxt)
					if !ret_val {
						utils.FailTask(fmt.Sprintf("%s-Error start/persist mons while create cluster %s", ctxt, request.Name), err, t)
						setClusterState(*cluster_uuid, models.CLUSTER_STATE_FAILED, ctxt)
						return
					}

					// Add OSDs
					t.UpdateStatus("Getting updated nodes list for OSD creation")
					updated_nodes, err := getNodes(request.Nodes)
					if err != nil {
						utils.FailTask(fmt.Sprintf("%s-Error getting updated nodes list post create cluster %s", ctxt, request.Name), err, t)
						setClusterState(*cluster_uuid, models.CLUSTER_STATE_FAILED, ctxt)
						return
					}
					t.UpdateStatus("Adding OSDs")
					failedOSDs, succeededOSDs := addOSDs(*cluster_uuid, request.Name, updated_nodes, request.Nodes, t, ctxt)
					if len(succeededOSDs) == 0 {
						utils.FailTask(fmt.Sprintf("%s-Failed adding all OSDs while create cluster %s", ctxt, request.Name), err, t)
						setClusterState(*cluster_uuid, models.CLUSTER_STATE_FAILED, ctxt)
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
					status, err := cluster_status(*cluster_uuid, request.Name, ctxt)
					clusterStatus := models.CLUSTER_STATUS_UNKNOWN
					switch status {
					case models.STATUS_OK:
						clusterStatus = models.CLUSTER_STATUS_OK
					case models.STATUS_WARN:
						clusterStatus = models.CLUSTER_STATUS_WARN
					case models.STATUS_ERR:
						clusterStatus = models.CLUSTER_STATUS_ERROR
					}
					if err := coll.Update(bson.M{"clusterid": *cluster_uuid}, bson.M{"$set": bson.M{"status": clusterStatus, "state": models.CLUSTER_STATE_ACTIVE}}); err != nil {
						t.UpdateStatus("Error updating the cluster status")
						return
					}
					t.UpdateStatus("Success")
					t.Done(models.TASK_STATUS_SUCCESS)
					return
				}
			}
		}
	}
	if taskId, err := bigfin_task.GetTaskManager().Run("CEPH-CreateCluster", asyncTask, 300*time.Second, nil, nil, nil); err != nil {
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

func startAndPersistMons(clusterId uuid.UUID, mons []backend.Mon, ctxt string) (bool, error) {
	var nodenames []string
	for _, mon := range mons {
		nodenames = append(nodenames, mon.Node)
	}
	if ok, err := salt_backend.StartMon(nodenames, ctxt); err != nil || !ok {
		return false, err
	}

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	for _, mon := range mons {
		coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
		if err := coll.Update(bson.M{"hostname": mon.Node}, bson.M{"$set": bson.M{"options.mon": "Y"}}); err != nil {
			return false, err
		}
		logger.Get().Info(fmt.Sprintf("%s-Mon added %s", ctxt, mon.Node))
	}
	return true, nil
}

func addOSDs(clusterId uuid.UUID, clusterName string, nodes map[uuid.UUID]models.Node, requestNodes []models.ClusterNode, t *task.Task, ctxt string) ([]backend.OSD, []backend.OSD) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	updatedStorageDisksMap := make(map[uuid.UUID][]models.Disk)
	var failedOSDs []backend.OSD
	var succeededOSDs []backend.OSD
	for _, requestNode := range requestNodes {
		if utils.StringInSlice("OSD", requestNode.NodeType) {
			var updatedStorageDisks []models.Disk
			uuid, err := uuid.Parse(requestNode.NodeId)
			if err != nil {
				logger.Get().Error("%s-Error parsing node id: %s while add OSD for cluster: %s. error: %v", ctxt, requestNode.NodeId, clusterName, err)
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
						var options = make(map[string]string)
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
						}
						if ok, err := persistOSD(slu, t, ctxt); err != nil || !ok {
							logger.Get().Error("%s-Error persisting %s for cluster: %s. error: %v", ctxt, slu.Name, clusterName, err)
							failedOSDs = append(failedOSDs, osd)
							break
						}
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
	for nodeid, updatedStorageDisks := range updatedStorageDisksMap {
		coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
		if err := coll.Update(
			bson.M{"nodeid": nodeid},
			bson.M{"$set": bson.M{"storagedisks": updatedStorageDisks}}); err != nil {
			logger.Get().Error("%s-Error updating disks for node: %v post add OSDs for cluster: %s. error: %v", ctxt, nodeid, clusterName, err)
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
	logger.Get().Info(fmt.Sprintf("%s-Added %s (%s %s) for cluster: %v", ctxt, slu.Name, slu.Options["node"], slu.Options["device"], slu.ClusterId))
	t.UpdateStatus("Added %s (%s %s)", slu.Name, slu.Options["node"], slu.Options["device"])

	return true, nil
}

func getNodes(clusterNodes []models.ClusterNode) (map[uuid.UUID]models.Node, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	var nodes = make(map[uuid.UUID]models.Node)
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	for _, clusterNode := range clusterNodes {
		uuid, err := uuid.Parse(clusterNode.NodeId)
		if err != nil {
			return nodes, errors.New(fmt.Sprintf("Error parsing node id: %v", clusterNode.NodeId))
		}
		var node models.Node
		if err := coll.Find(bson.M{"nodeid": *uuid}).One(&node); err != nil {
			return nodes, err
		}
		nodes[node.NodeId] = node
	}
	return nodes, nil
}

func (s *CephProvider) ExpandCluster(req models.RpcRequest, resp *models.RpcResponse) error {
	cluster_id_str := req.RpcRequestVars["cluster-id"]
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("Error parsing the cluster id: %s. error: %v", cluster_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s. error: %v", cluster_id_str, err))
		return err
	}

	var new_nodes []models.ClusterNode
	if err := json.Unmarshal(req.RpcRequestData, &new_nodes); err != nil {
		logger.Get().Error("Unbale to parse the request. error: %v", err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request. error: %v", err))
		return err
	}

	// Get corresponding nodes from DB
	nodes, err := getNodes(new_nodes)
	if err != nil {
		logger.Get().Error("Error getting the nodes from DB for cluster: %v. error: %v", *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error getting the nodes from DB. error: %v", err))
		return err
	}

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	var cluster models.Cluster
	if err := coll.Find(bson.M{"clusterid": *cluster_id}).One(&cluster); err != nil {
		logger.Get().Error("Error getting cluster details for %v. error: %v", *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error getting cluster details. error: %v", err))
		return nil
	}

	// Get the cluster and public IPs for nodes
	node_ips, err := nodeIPs(cluster.Networks, nodes)
	if err != nil {
		logger.Get().Error("Node IP does not fall in provided subnets for cluster: %v. error: %v", *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Node IP does fall in provided subnets. error: %v", err))
		return nil
	}

	var mons []backend.Mon
	for _, new_node := range new_nodes {
		if utils.StringInSlice("MON", new_node.NodeType) {
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
		if utils.StringInSlice("MON", new_node.NodeType) {
			coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
			nodeid, err := uuid.Parse(new_node.NodeId)
			if err != nil {
				logger.Get().Error("Error parsing the node id: %s while expand cluster: %v. error: %v", new_node.NodeId, *cluster_id, err)
				*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the node id: %s", new_node.NodeId))
				return err
			}
			var monNode models.Node
			// No need to check for error here as in case of error, node instance would not be populated
			// and the same already being checked below
			_ = coll.Find(bson.M{"clusterid": *cluster_id, "nodeid": *nodeid}).One(&monNode)
			if monNode.Hostname != "" {
				logger.Get().Error("Mon %v already exists for cluster: %v", *nodeid, *cluster_id)
				*resp = utils.WriteResponse(http.StatusInternalServerError, "The mon node already available")
				return errors.New(fmt.Sprintf("Mon %v already exists for cluster: %v", *nodeid, *cluster_id))
			}
		}
	}

	asyncTask := func(t *task.Task) {
		for {
			select {
			case <-t.StopCh:
				return
			default:
				t.UpdateStatus("Started ceph provider task for cluster expansion: %v", t.ID)
				// Update nodes details
				sessionCopy := db.GetDatastore().Copy()
				defer sessionCopy.Close()
				coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
				for _, node := range nodes {
					if err := coll.Update(
						bson.M{"nodeid": node.NodeId},
						bson.M{"$set": bson.M{
							"clusterid":  *cluster_id,
							"clusterip4": node_ips[node.NodeId]["cluster"],
							"publicip4":  node_ips[node.NodeId]["public"]}}); err != nil {
						utils.FailTask(fmt.Sprintf("Error updating node %v post cluster exoansion for cluster: %v", node.NodeId, *cluster_id), err, t)
						return
					}
				}

				if len(mons) > 0 {
					t.UpdateStatus("Adding mons")
					for _, mon := range mons {
						if ret_val, err := salt_backend.AddMon(cluster.Name, []backend.Mon{mon}, ""); err != nil || !ret_val {
							utils.FailTask(fmt.Sprintf("Error adding mons while expand cluster: %v", *cluster_id), err, t)
							return
						}
					}
				}
				t.UpdateStatus("Starting and persisting the mons")
				// Start and persist the mons
				ret_val, err := startAndPersistMons(*cluster_id, mons, "")
				if err != nil || !ret_val {
					utils.FailTask(fmt.Sprintf("Error start/persist mons while expand cluster: %v", *cluster_id), err, t)
					return
				}

				// Add OSDs
				t.UpdateStatus("Getting updated nodes for OSD creation")
				updated_nodes, err := getNodes(new_nodes)
				if err != nil {
					utils.FailTask(fmt.Sprintf("Error getting updated nodes while expand cluster: %v", *cluster_id), err, t)
					return
				}
				t.UpdateStatus("Adding OSDs")
				failedOSDs, succeededOSDs := addOSDs(*cluster_id, cluster.Name, updated_nodes, new_nodes, t, "")
				if len(succeededOSDs) == 0 {
					utils.FailTask(fmt.Sprintf("Failed to add all OSDs while expand cluster: %v", *cluster_id), err, t)
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
				if ok := RecalculatePgnum(*cluster_id, t); !ok {
					logger.Get().Warning("Could not re-calculate pgnum/pgpnum for cluster: %v", *cluster_id)
				}
				t.UpdateStatus("Success")
				t.Done(models.TASK_STATUS_SUCCESS)
				return
			}
		}
	}

	if taskId, err := bigfin_task.GetTaskManager().Run("CEPH-ExpandCluster", asyncTask, 300*time.Second, nil, nil, nil); err != nil {
		logger.Get().Error("Task creation failed for exoand cluster: %v. error: %v", *cluster_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, "Task creation failed for cluster expansion")
		return err
	} else {
		*resp = utils.WriteAsyncResponse(taskId, fmt.Sprintf("Task Created for expand cluster: %v", *cluster_id), []byte{})
	}

	return nil
}

func (s *CephProvider) GetClusterStatus(req models.RpcRequest, resp *models.RpcResponse) error {
	cluster_id_str := req.RpcRequestVars["cluster-id"]
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("Error parsing the cluster id: %s. error: %v", cluster_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s", cluster_id_str))
		return err
	}
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	// Get cluster details
	var cluster models.Cluster
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Find(bson.M{"clusterid": *cluster_id}).One(&cluster); err != nil {
		logger.Get().Error("Error fetching details of cluster: %v. error: %v", *cluster_id, err)
		return err
	}

	status, err := cluster_status(*cluster_id, cluster.Name, "")
	if err != nil {
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("error: %v", err))
	} else {
		*resp = utils.WriteResponse(http.StatusOK, status)
	}
	return nil
}

func cluster_status(clusterId uuid.UUID, clusterName string, ctxt string) (status string, err error) {
	// Pick a random mon from the list
	monnode, err := GetRandomMon(clusterId)
	if err != nil {
		logger.Get().Error("%s-Error getting a mon from cluster: %s. error: %v", ctxt, clusterName, err)
		return "", errors.New(fmt.Sprintf("Error getting a mon. error: %v", err))
	}

	// Get the cluser status
	status, err = salt_backend.GetClusterStatus(monnode.Hostname, clusterName)
	if err != nil {
		logger.Get().Error("%s-Could not get up status of cluster: %v. error: %v", ctxt, clusterName, err)
		return "", err
	}
	return cluster_status_map[status], nil
}

func RecalculatePgnum(clusterId uuid.UUID, t *task.Task) bool {
	// Get storage pools
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	var storages []models.Storage
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
	if err := coll.Find(bson.M{"clusterid": clusterId}).All(&storages); err != nil {
		utils.FailTask(fmt.Sprintf("Error getting storage pools for cluster: %v", clusterId), err, t)
		return false
	}

	t.UpdateStatus("Getting a mon from cluster")
	monnode, err := GetRandomMon(clusterId)
	if err != nil {
		utils.FailTask(fmt.Sprintf("Error getting mon node for cluster: %v", clusterId), err, t)
		return false
	}

	for _, storage := range storages {
		if storage.Name == "rbd" {
			continue
		}
		pgNum := DerivePgNum(clusterId, storage.Size, storage.Replicas)
		id, err := strconv.Atoi(storage.Options["id"])
		if err != nil {
			utils.FailTask(fmt.Sprintf("Error getting details of pool: %s for cluster: %v", storage.Name, clusterId), err, t)
			return false
		}
		// Update the PG Num for the cluster
		t.UpdateStatus(fmt.Sprintf("Updating the pgnum and pgpnum for pool %s", storage.Name))
		poolData := map[string]interface{}{
			"pg_num":  int(pgNum),
			"pgp_num": int(pgNum),
		}
		ok, err := cephapi_backend.UpdatePool(monnode.Hostname, clusterId, id, poolData)
		if err != nil || !ok {
			t.UpdateStatus(fmt.Sprintf("Could not update pgnum/pgnum for pool: %s of cluster: %v", storage.Name, clusterId))
		}
	}
	return true
}
