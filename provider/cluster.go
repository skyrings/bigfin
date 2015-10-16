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
	"github.com/skyrings/bigfin/backend/salt"
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/skyring/conf"
	"github.com/skyrings/skyring/db"
	"github.com/skyrings/skyring/models"
	"github.com/skyrings/skyring/tools/uuid"
	"gopkg.in/mgo.v2/bson"
	"net/http"

	skyring_backend "github.com/skyrings/skyring/backend"
)

var (
	salt_backend = salt.New()
)

type CephProvider struct{}

func (s *CephProvider) CreateCluster(req models.RpcRequest, resp *models.RpcResponse) error {
	var request models.AddClusterRequest

	if err := json.Unmarshal(req.RpcRequestData, &request); err != nil {
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request %v", err))
		return err
	}

	// Get corresponding nodes from DB
	nodes, _ := getNodes(request.Nodes)

	// Get the cluster and public IPs for nodes
	node_ips, err := nodeIPs(request.Networks, nodes)
	if err != nil {
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Node IP does fall in provided subnets: %v", err))
		return nil
	}

	// Invoke the cluster create backend
	cluster_uuid, _ := uuid.New()
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
		*resp = utils.WriteResponse(http.StatusInternalServerError, "No mons mentioned in the node list")
		return errors.New("ceph_provider: No mons mentioned in the node list")
	}

	ret_val, err := salt_backend.CreateCluster(request.Name, *cluster_uuid, mons)
	if err != nil {
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error while cluster creation %v", err))
		return err
	}

	if ret_val {
		// Update nodes details
		sessionCopy := db.GetDatastore().Copy()
		defer sessionCopy.Close()
		for _, node := range nodes {
			coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
			if err := coll.Update(
				bson.M{"nodeid": node.NodeId},
				bson.M{"$set": bson.M{
					"clusterid":   *cluster_uuid,
					"cluster_ip4": node_ips[node.NodeId]["cluster"],
					"public_ip4":  node_ips[node.NodeId]["public"]}}); err != nil {
				*resp = utils.WriteResponse(http.StatusInternalServerError, err.Error())
				return err
			}
		}

		// Start and persist the mons
		ret_val, err := startAndPersistMons(*cluster_uuid, mons)
		if !ret_val {
			*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Cluster created but failed to start mon %v", err))
			return nil
		}

		// Add OSDs
		updated_nodes, _ := getNodes(request.Nodes)
		ret_val, _ = addOSDs(*cluster_uuid, request.Name, updated_nodes, request.Nodes)
		if !ret_val {
			*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Cluster created but add OSDs failed %v", err))
			return nil
		}

		// Add cluster to DB
		var cluster models.Cluster
		cluster.ClusterId = *cluster_uuid
		cluster.Name = request.Name
		cluster.CompatVersion = request.CompatVersion
		cluster.Type = request.Type
		cluster.WorkLoad = request.WorkLoad
		cluster.Status = models.CLUSTER_STATUS_ACTIVE_AND_AVAILABLE
		cluster.Tags = request.Tags
		cluster.Options = request.Options
		cluster.Networks = request.Networks
		cluster.OpenStackServices = request.OpenStackServices
		cluster.Enabled = true
		coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
		if err := coll.Insert(cluster); err != nil {
			*resp = utils.WriteResponse(http.StatusInternalServerError, err.Error())
			return err
		}

		// Send response
		*resp = utils.WriteResponse(http.StatusOK, "Done")
		return nil
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
	_, err := salt_backend.StartMon(nodenames)

	if err != nil {
		return false, err
	}

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	for _, mon := range mons {
		coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
		var node models.Node
		if err := coll.Find(bson.M{"hostname": mon.Node}).One(&node); err != nil {
			return false, err
		}

		id, _ := uuid.New()
		var slu models.StorageLogicalUnit
		slu.SluId = *id
		slu.Name = fmt.Sprintf("mon-%v", *id)
		slu.Type = models.CEPH_MON
		slu.ClusterId = clusterId
		slu.NodeId = node.NodeId
		var options = make(map[string]string)
		options["node"] = mon.Node
		options["publicip4"] = mon.PublicIP4
		options["clusterip4"] = mon.ClusterIP4
		slu.Options = options

		coll = sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
		if err := coll.Insert(slu); err != nil {
			return false, err
		}
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
			uuid, _ := uuid.Parse(requestNode.NodeId)
			storageNode := nodes[*uuid]
			for _, storageDisk := range storageNode.StorageDisks {
				for _, device := range requestNode.Devices {
					if storageDisk.Name == device.Name && !storageDisk.Used {
						if storageDisk.FSType == "" {
							storageDisk.FSType = models.DEFAULT_FS_TYPE
						}
						var osd = backend.OSD{
							Node:       storageNode.Hostname,
							PublicIP4:  storageNode.PublicIP4,
							ClusterIP4: storageNode.ClusterIP4,
							Device:     device.Name,
							FSType:     storageDisk.FSType,
						}
						if ret_val, err := salt_backend.AddOSD(clusterName, osd); err != nil {
							return ret_val, err
						}
						if ret_val, err := persistOSD(clusterId, storageNode.NodeId, storageDisk.FSUUID, osd); err != nil {
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
			bson.M{"$set": bson.M{"storage_disks": updatedStorageDisks}}); err != nil {
			return false, err
		}
	}

	return true, nil
}

func persistOSD(clusterId uuid.UUID, nodeId uuid.UUID, diskId uuid.UUID, osd backend.OSD) (bool, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)

	id, _ := uuid.New()
	var slu models.StorageLogicalUnit
	slu.SluId = *id
	slu.Name = fmt.Sprintf("osd-%v", *id)
	slu.Type = models.CEPH_OSD
	slu.ClusterId = clusterId
	slu.NodeId = nodeId
	slu.StorageDeviceId = diskId
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

	return true, nil
}

func getNodes(clusterNodes []models.ClusterNode) (map[uuid.UUID]models.Node, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	var nodes = make(map[uuid.UUID]models.Node)
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	for _, clusterNode := range clusterNodes {
		uuid, _ := uuid.Parse(clusterNode.NodeId)
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
	cluster_id, _ := uuid.Parse(cluster_id_str)

	var new_nodes []models.ClusterNode
	if err := json.Unmarshal(req.RpcRequestData, &new_nodes); err != nil {
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request %v", err))
		return err
	}

	// Get corresponding nodes from DB
	nodes, _ := getNodes(new_nodes)

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	var cluster models.Cluster
	if err := coll.Find(bson.M{"clusterid": *cluster_id}).One(&cluster); err != nil {
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error getting cluster details %v", err))
		return nil
	}

	// Get the cluster and public IPs for nodes
	node_ips, err := nodeIPs(cluster.Networks, nodes)
	if err != nil {
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

	// Update nodes details
	for _, node := range nodes {
		coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
		if err := coll.Update(
			bson.M{"nodeid": node.NodeId},
			bson.M{"$set": bson.M{
				"clusterid":   *cluster_id,
				"cluster_ip4": node_ips[node.NodeId]["cluster"],
				"public_ip4":  node_ips[node.NodeId]["public"]}}); err != nil {
			*resp = utils.WriteResponse(http.StatusInternalServerError, err.Error())
			return err
		}
	}

	if len(mons) > 0 {
		// If mon node already exists, error out
		for _, new_node := range new_nodes {
			coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
			nodeid, _ := uuid.Parse(new_node.NodeId)
			var slu models.StorageLogicalUnit
			_ = coll.Find(bson.M{"clusterid": *cluster_id, "nodeid": *nodeid, "type": models.CEPH_MON}).One(&slu)
			if !slu.SluId.IsZero() {
				*resp = utils.WriteResponse(http.StatusInternalServerError, "The mon node already available")
				return nil
			}
		}

		// Add mon
		ret_val, err := salt_backend.AddMon(cluster.Name, mons)
		if err != nil {
			*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Failed to add mon %v", err))
			return nil
		}
		if ret_val {
			// Start and persist the mons
			ret_val, err := startAndPersistMons(*cluster_id, mons)
			if !ret_val {
				*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Cluster created but failed to start mon %v", err))
				return nil
			}
		}
	}

	// Add OSDs
	updated_nodes, _ := getNodes(new_nodes)
	ret_val, err := addOSDs(*cluster_id, cluster.Name, updated_nodes, new_nodes)
	if !ret_val {
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Add OSDs failed %v", err))
		return nil
	}

	*resp = utils.WriteResponse(http.StatusOK, "Done")
	return nil
}
