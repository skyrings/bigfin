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
	"github.com/skyrings/bigfin/tools/logger"
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/skyring/conf"
	"github.com/skyrings/skyring/db"
	"github.com/skyrings/skyring/models"
	"github.com/skyrings/skyring/tools/task"
	"github.com/skyrings/skyring/tools/uuid"
	"gopkg.in/mgo.v2/bson"
	"net/http"

	bigfin_task "github.com/skyrings/bigfin/tools/task"
	skyring_backend "github.com/skyrings/skyring/backend"
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

	if err := json.Unmarshal(req.RpcRequestData, &request); err != nil {
		logger.Get().Error("Unbale to parse the request %v", err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request %v", err))
		return err
	}

	// Get corresponding nodes from DB
	nodes, err := getNodes(request.Nodes)
	if err != nil {
		logger.Get().Error("Error getting nodes from DB %v", err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error getting nodes from DB %v", err))
		return err
	}
	logger.Get().Info(fmt.Sprintf("%v", nodes))

	// Get the cluster and public IPs for nodes
	node_ips, err := nodeIPs(request.Networks, nodes)
	if err != nil {
		logger.Get().Error("Node IP does fall in provided subnets: %v", err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Node IP does fall in provided subnets: %v", err))
		return nil
	}

	// Invoke the cluster create backend
	cluster_uuid, err := uuid.New()
	if err != nil {
		logger.Get().Error("Error creating cluster id: %v", err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error creating cluster id: %v", err))
		return nil
	}
	logger.Get().Info((*cluster_uuid).String())
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
		logger.Get().Error("No mons mentioned in the node list")
		*resp = utils.WriteResponse(http.StatusInternalServerError, "No mons mentioned in the node list")
		return errors.New("ceph_provider: No mons mentioned in the node list")
	}

	asyncTask := func(t *task.Task) {
		t.UpdateStatus("Started ceph provider task for cluster creation: %v", t.ID)
		ret_val, err := salt_backend.CreateCluster(request.Name, *cluster_uuid, mons)
		logger.Get().Info(request.Name)
		//_, err := salt_backend.CreateCluster(request.Name, *cluster_uuid, mons)
		if err != nil {
			utils.FailTask("Cluster creation failed", err, t)
			return
		}

		if ret_val {
			t.UpdateStatus("Updating node details for cluster")
			// Update nodes details
			sessionCopy := db.GetDatastore().Copy()
			defer sessionCopy.Close()
			for _, node := range nodes {
				coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
				if err := coll.Update(
					bson.M{"nodeid": node.NodeId},
					bson.M{"$set": bson.M{
						"clusterid":  *cluster_uuid,
						"clusterip4": node_ips[node.NodeId]["cluster"],
						"publicip4":  node_ips[node.NodeId]["public"]}}); err != nil {
					utils.FailTask("Failed to update nodes details post cluster create", err, t)
					return
				}
			}

			// Start and persist the mons
			t.UpdateStatus("Starting and creating mons")
			ret_val, err := startAndPersistMons(*cluster_uuid, mons)
			if !ret_val {
				utils.FailTask("Error start/persist mons", err, t)
				return
			}

			// Add OSDs
			t.UpdateStatus("Getting updated nodes list for OSD creation")
			updated_nodes, err := getNodes(request.Nodes)
			if err != nil {
				utils.FailTask("Error getting updated nodes list post cluster create", err, t)
				return
			}
			t.UpdateStatus("Adding OSDs")
			ret_val, err = addOSDs(*cluster_uuid, request.Name, updated_nodes, request.Nodes)
			if err != nil || !ret_val {
				utils.FailTask("Error adding OSDs", err, t)
				return
			}

			// Add cluster to DB
			t.UpdateStatus("Persisting cluster details")
			var cluster models.Cluster
			cluster.ClusterId = *cluster_uuid
			cluster.Name = request.Name
			cluster.CompatVersion = request.CompatVersion
			cluster.Type = request.Type
			cluster.WorkLoad = request.WorkLoad
			status, err := cluster_status(*cluster_uuid, request.Name)
			switch status {
			case models.STATUS_OK:
				cluster.Status = models.CLUSTER_STATUS_OK
			case models.STATUS_WARN:
				cluster.Status = models.CLUSTER_STATUS_WARN
			case models.STATUS_ERR:
				cluster.Status = models.CLUSTER_STATUS_ERROR
			default:
				cluster.Status = models.CLUSTER_STATUS_OK
			}
			cluster.Tags = request.Tags
			cluster.Options = request.Options
			cluster.Networks = request.Networks
			cluster.OpenStackServices = request.OpenStackServices
			cluster.Enabled = true
			coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
			if err := coll.Insert(cluster); err != nil {
				utils.FailTask("Error persisting the cluster", err, t)
				return
			}
			t.UpdateStatus("Success")
			t.Done(models.TASK_STATUS_SUCCESS)
		}
	}
	if taskId, err := bigfin_task.GetTaskManager().Run("CEPH-CreateCluster", asyncTask, nil, nil, nil); err != nil {
		*resp = utils.WriteResponse(http.StatusInternalServerError, "Task creation failed for create cluster")
		return err
	} else {
		*resp = utils.WriteAsyncResponse(taskId, "Task Created", []byte{})
	}

	return nil
}

func nodeIPs(networks models.ClusterNetworks, nodes map[uuid.UUID]models.Node) (map[uuid.UUID]map[string]string, error) {
	var node_ips = make(map[uuid.UUID]map[string]string)
	for nodeid, node := range nodes {
		host_addrs := node.NetworkInfo.Ipv4
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
	}
	return node_ips, nil
}

func startAndPersistMons(clusterId uuid.UUID, mons []backend.Mon) (bool, error) {
	var nodenames []string
	for _, mon := range mons {
		nodenames = append(nodenames, mon.Node)
	}
	if ok, err := salt_backend.StartMon(nodenames); err != nil || !ok {
		return false, err
	}

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	for _, mon := range mons {
		coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
		if err := coll.Update(bson.M{"hostname": mon.Node}, bson.M{"$set": bson.M{"options.mon": "Y"}}); err != nil {
			return false, err
		}
		logger.Get().Info(fmt.Sprintf("Mon added %s", mon.Node))
	}
	return true, nil
}

func addOSDs(clusterId uuid.UUID, clusterName string, nodes map[uuid.UUID]models.Node, requestNodes []models.ClusterNode) (bool, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	updatedStorageDisksMap := make(map[uuid.UUID][]skyring_backend.Disk)
	for _, requestNode := range requestNodes {
		if utils.StringInSlice("OSD", requestNode.NodeType) {
			var updatedStorageDisks []skyring_backend.Disk
			uuid, err := uuid.Parse(requestNode.NodeId)
			if err != nil {
				logger.Get().Error("Error parsing node id: %s", requestNode.NodeId)
				return false, errors.New(fmt.Sprintf("Error parsing node id: %v", requestNode.NodeId))
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
						if ret_val, err := salt_backend.AddOSD(clusterName, osd); err != nil || !ret_val {
							logger.Get().Error("Error adding OSD: %v. error: %v", osd, err)
							return ret_val, err
						}
						if ret_val, err := persistOSD(
							clusterId,
							storageNode.NodeId,
							storageDisk.FSUUID,
							storageDisk.Size,
							osd); err != nil || !ret_val {
							logger.Get().Error("Error persisting OSD: %v. error: %v", osd, err)
							return ret_val, err
						}
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
			logger.Get().Error("Error updating disks for node: %v. error: %v", nodeid, err)
			return false, err
		}
	}

	return true, nil
}

func persistOSD(clusterId uuid.UUID, nodeId uuid.UUID, diskId uuid.UUID, diskSize uint64, osd backend.OSD) (bool, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)

	id, err := uuid.New()
	if err != nil {
		return false, errors.New("Error creating SLU id")
	}
	var slu models.StorageLogicalUnit
	slu.SluId = *id
	slu.Name = fmt.Sprintf("osd-%v", *id)
	slu.Type = models.CEPH_OSD
	slu.ClusterId = clusterId
	slu.NodeId = nodeId
	slu.StorageDeviceId = diskId
	slu.StorageDeviceSize = diskSize
	var options = make(map[string]string)
	options["node"] = osd.Node
	options["publicip4"] = osd.PublicIP4
	options["clusterip4"] = osd.ClusterIP4
	options["device"] = osd.Device
	options["fstype"] = osd.FSType
	slu.Options = options

	if err := coll.Insert(slu); err != nil {
		return false, err
	}
	logger.Get().Info(fmt.Sprintf("OSD added %s %s", osd.Node, osd.Device))

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
		logger.Get().Error("Error parsing the cluster id: %s", cluster_id_str)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s", cluster_id_str))
		return err
	}

	var new_nodes []models.ClusterNode
	if err := json.Unmarshal(req.RpcRequestData, &new_nodes); err != nil {
		logger.Get().Error("Unbale to parse the request %v", err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request %v", err))
		return err
	}

	// Get corresponding nodes from DB
	nodes, err := getNodes(new_nodes)
	if err != nil {
		logger.Get().Error("Error getting the nodes from DB: %v", err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error getting the nodes from DB: %v", err))
		return err
	}

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	var cluster models.Cluster
	if err := coll.Find(bson.M{"clusterid": *cluster_id}).One(&cluster); err != nil {
		logger.Get().Error("Error getting cluster details %v", err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error getting cluster details %v", err))
		return nil
	}

	// Get the cluster and public IPs for nodes
	node_ips, err := nodeIPs(cluster.Networks, nodes)
	if err != nil {
		logger.Get().Error("Node IP does fall in provided subnets: %v", err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Node IP does fall in provided subnets: %v", err))
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
		coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
		nodeid, err := uuid.Parse(new_node.NodeId)
		if err != nil {
			logger.Get().Error("Error parsing the node id: %s", new_node.NodeId)
			*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the node id: %s", new_node.NodeId))
			return err
		}
		var monNode models.Node
		// No need to check for error here as in case of error, slu instance would not be populated
		// and the same already eing checked below
		_ = coll.Find(bson.M{"clusterid": *cluster_id, "nodeid": *nodeid}).One(&monNode)
		if monNode.Hostname != "" {
			logger.Get().Error("Mon already exists: %v", *nodeid)
			*resp = utils.WriteResponse(http.StatusInternalServerError, "The mon node already available")
			return errors.New(fmt.Sprintf("Mon already exists: %v", *nodeid))
		}
	}

	asyncTask := func(t *task.Task) {
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
				utils.FailTask("Error updating node details post cluster creation", err, t)
				return
			}
		}

		if len(mons) > 0 {
			t.UpdateStatus("Adding mons")
			// Add mon
			ret_val, err := salt_backend.AddMon(cluster.Name, mons)
			if err != nil {
				utils.FailTask("Error adding mons", err, t)
				return
			}
			if ret_val {
				t.UpdateStatus("Starting and persisting the mons")
				// Start and persist the mons
				ret_val, err := startAndPersistMons(*cluster_id, mons)
				if err != nil || !ret_val {
					utils.FailTask("Error start/persist mons", err, t)
					return
				}
			}
		}

		// Add OSDs
		t.UpdateStatus("Getting updated nodes for OSD creation")
		updated_nodes, err := getNodes(new_nodes)
		if err != nil {
			utils.FailTask("Error getting updated nodes post cluster create", err, t)
			return
		}
		t.UpdateStatus("Adding OSDs")
		ret_val, err := addOSDs(*cluster_id, cluster.Name, updated_nodes, new_nodes)
		if err != nil || !ret_val {
			utils.FailTask("Error adding OSDs", err, t)
			return
		}
		t.UpdateStatus("Success")
		t.Done(models.TASK_STATUS_SUCCESS)
	}

	if taskId, err := bigfin_task.GetTaskManager().Run("CEPH-ExpandCluster", asyncTask, nil, nil, nil); err != nil {
		*resp = utils.WriteResponse(http.StatusInternalServerError, "Task creation failed for cluster expansion")
		return err
	} else {
		*resp = utils.WriteAsyncResponse(taskId, "Task Created", []byte{})
	}

	return nil
}

func (s *CephProvider) GetClusterStatus(req models.RpcRequest, resp *models.RpcResponse) error {
	cluster_id_str := req.RpcRequestVars["cluster-id"]
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("Error parsing the cluster id: %s", cluster_id_str)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s", cluster_id_str))
		return err
	}
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	// Get cluster details
	var cluster models.Cluster
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Find(bson.M{"clusterid": *cluster_id}).One(&cluster); err != nil {
		logger.Get().Error("Error fetching cluster: %v. error: %v", *cluster_id, err)
		return err
	}

	status, err := cluster_status(*cluster_id, cluster.Name)
	if err != nil {
		*resp = utils.WriteResponse(http.StatusInternalServerError, "")
	} else {
		*resp = utils.WriteResponse(http.StatusOK, status)
	}
	return nil
}

func cluster_status(clusterId uuid.UUID, clusterName string) (status string, err error) {
	// Pick a random mon from the list
	monnode, err := GetRandomMon(clusterId)
	if err != nil {
		logger.Get().Error("Error getting a mon. error: %v", err)
		return "", errors.New(fmt.Sprintf("Error getting a mon. error: %v", err))
	}

	// Get the cluser status
	status, err = salt_backend.GetClusterStatus(monnode.Hostname, clusterName)
	if err != nil {
		logger.Get().Error("Could not get up status of cluster: %v. error: %v", clusterName, err)
		return "", err
	}
	return cluster_status_map[status], nil
}
