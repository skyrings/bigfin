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
	"log"
	"net"
	"net/http"
)

const (
	CLUSTER_STATUS_UP   = "up"
	CLUSTER_STATUS_DOWN = "down"
)

var (
	salt_backend = salt.New()
)

func (s *CephProvider) CreateCluster(req models.RpcRequest, resp *models.RpcResponse) error {
	var request models.StorageCluster

	if err := json.Unmarshal(req.RpcRequestData, &request); err != nil {
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request %v", err))
		return err
	}

	// Get the cluster and public IPs for nodes
	node_ips := nodeIPs(request.Networks, request.Nodes)

	// Invoke the cluster create backend
	cluster_uuid, _ := uuid.New()
	request.ClusterId = *cluster_uuid
	var mons []backend.Mon
	for _, node := range request.Nodes {
		if node.Options["mon"] == "Y" || node.Options["mon"] == "y" {
			var mon backend.Mon
			mon.Node = node.Hostname
			mon.PublicIP4 = node_ips[node.Hostname]["public"]
			mon.ClusterIP4 = node_ips[node.Hostname]["cluster"]
			mons = append(mons, mon)
		}
	}
	if len(mons) == 0 {
		*resp = utils.WriteResponse(http.StatusInternalServerError, "No mons mentioned in the node list")
		return errors.New("ceph_provider: No mons mentioned in the node list")
	}

	ret_val, err := salt_backend.CreateCluster(request.ClusterName, request.ClusterId, mons)
	if err != nil {
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error while cluster creation %v", err))
		return err
	}

	if ret_val {
		// Update nodes details
		sessionCopy := db.GetDatastore().Copy()
		defer sessionCopy.Close()
		for _, node := range request.Nodes {
			coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
			if err := coll.Update(
				bson.M{"hostname": node.Hostname},
				bson.M{"$set": bson.M{
					"clusterid": request.ClusterId,
					"administrativestatus": models.USED,
					"clusterip": node_ips[node.Hostname]["cluster"],
					"publicaddressipv4": node_ips[node.Hostname]["cluster"]}}); err != nil {
				*resp = utils.WriteResponse(http.StatusInternalServerError, err.Error())
				return err
			}
		}

		// Start mons
		ret_val, err = startMon(mons)
		if !ret_val {
			*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Cluster created but failed to start mon %v", err))
			return nil
		}
		log.Println("Mons started")

		// Add OSDs
		ret_val, _ = addOSDs(request.ClusterId, request.ClusterName, request.Nodes)
		if !ret_val {
			*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Cluster created but add OSDs failed %v", err))
			return nil
		}
		log.Println("OSDs added")

		// Add mons
		ret_val, _ = addMonsToDB(request.ClusterId, mons)
		if !ret_val {
			*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Cluster created but add mons persist failed %v", err))
			return nil
		}
		log.Println("Mons added")

		// Add cluster to DB
		request.ClusterStatus = CLUSTER_STATUS_UP
		request.AdministrativeStatus = models.CLUSTER_STATUS_ACTIVE_AND_AVAILABLE
		coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
		if err := coll.Insert(request); err != nil {
			*resp = utils.WriteResponse(http.StatusInternalServerError, err.Error())
			return err
		}

		// Send response
		*resp = utils.WriteResponse(http.StatusOK, "Done")
		return nil
	}
	return nil
}

func nodeIPs(networks models.ClusterNetworks, nodes []models.ClusterNode) map[string]map[string]string {
	var node_ips = make(map[string]map[string]string)
	for _, node := range nodes {
		host_addrs, err := net.LookupHost(node.Hostname)
		var m = make(map[string]string)
		if err == nil {
			for _, host_addr := range host_addrs {
				if ok, _ := utils.IsIPInSubnet(host_addr, networks.Cluster); ok {
					m["cluster"] = host_addr
				}
				if ok, _ := utils.IsIPInSubnet(host_addr, networks.Public); ok {
					m["public"] = host_addr
				}
				node_ips[node.Hostname] = m
			}
		}
		if node_ips[node.Hostname]["cluster"] == "" {
			m["cluster"] = host_addrs[0]
		}
		if node_ips[node.Hostname]["public"] == "" {
			m["public"] = host_addrs[0]
		}
		node_ips[node.Hostname] = m
	}
	return node_ips
}

func startMon(mons []backend.Mon) (bool, error) {
	var nodenames []string
	for _, mon := range mons {
		nodenames = append(nodenames, mon.Node)
	}
	return salt_backend.StartMon(nodenames)
}

func addOSDs(clusterId uuid.UUID, clusterName string, nodes []models.ClusterNode) (bool, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	var osds []backend.OSD
	updatedStorageDisksMap := make(map[string][]models.StorageDisk)
	for _, node := range nodes {
		// Get the node details from DB for disk details
		var storageNode models.StorageNode
		var updatedStorageDisks []models.StorageDisk
		coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
		if err := coll.Find(bson.M{"hostname": node.Hostname}).One(&storageNode); err != nil {
			return false, err
		}
		for _, storageDisk := range storageNode.StorageDisks {
			for _, devname := range node.Disks {
				if storageDisk.Disk.Name == devname && storageDisk.AdministrativeStatus == models.FREE {
					var osd = backend.OSD{
						Node: node.Hostname,
						PublicIP4: storageNode.PublicAddressIpv4,
						ClusterIP4: storageNode.ClusterIp,
						Device: devname,
						FSType: storageDisk.Disk.FSType,
					}
					osds = append(osds, osd)
					storageDisk.AdministrativeStatus = models.USED
				}
				updatedStorageDisks = append(updatedStorageDisks, storageDisk)
			}
		}
		updatedStorageDisksMap[node.Hostname] = updatedStorageDisks
	}

	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(COLL_NAME_OSDS)
	for _, osd := range osds {
		log.Println(fmt.Sprintf("Adding osd... %v", osd))
		if ret_val, err := salt_backend.AddOSD(clusterName, osd); err != nil {
			return ret_val, err
		}

		// Persist OSD details to DB
		osdid, _ := uuid.New()
		var cephosd = CephOSD{OSDId: *osdid, ClusterId: clusterId, OSD: osd}
		if err := coll.Insert(cephosd); err != nil {
			log.Println("Save osd failed")
			return false, err
		}
		log.Println("OSD Added")
	}

	// Update the storage disks as used
	for hostname, updatedStorageDisks := range updatedStorageDisksMap {
		coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
		if err := coll.Update(
			bson.M{"hostname": hostname},
			bson.M{"$set": bson.M{"storagedisks": updatedStorageDisks}}); err != nil {
			return false, err
		}
	}

	return true, nil
}

func addMonsToDB(clusterId uuid.UUID, mons []backend.Mon) (bool, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	for _, mon := range mons {
		var cephmon CephMon
		monid, _ := uuid.New()
		cephmon.MonId = *monid
		cephmon.ClusterId = clusterId
		cephmon.Mon = mon
		coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(COLL_NAME_MONS)
		if err := coll.Insert(cephmon); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (s *CephProvider) RemoveStorage(req models.RpcRequest, resp *models.RpcResponse) error {
	cluster_id := req.RpcRequestVars["cluster-id"]
	uuid, _ := uuid.Parse(cluster_id)

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	// Remove the OSDs
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(COLL_NAME_OSDS)
	if _, err := coll.RemoveAll(bson.M{"clusterid": *uuid}); err != nil {
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error removing OSD %v", err))
		return nil
	}
	log.Println("Removed OSDs")

	// Remove Mons
	coll = sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(COLL_NAME_MONS)
	if _, err := coll.RemoveAll(bson.M{"clusterid": *uuid}); err != nil {
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error removing Mon %v", err))
		return nil
	}
	log.Println("Removed Mons")

	// TODO: Remove the pools

	*resp = utils.WriteResponse(http.StatusOK, "Done")
	return nil
}

func (s *CephProvider) ExpandCluster(req models.RpcRequest, resp *models.RpcResponse) error {
	cluster_id := req.RpcRequestVars["cluster-id"]
	uuid, _ := uuid.Parse(cluster_id)
	log.Println(*uuid)

	var request models.ExpandClusterRequest
	if err := json.Unmarshal(req.RpcRequestData, &request); err != nil {
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request %v", err))
		return err
	}
	log.Println(request)

	// Get the cluster and public IPs for nodes
	node_ips := nodeIPs(request.Networks, request.Nodes)
	log.Println(node_ips)

	var mons []backend.Mon
	for _, node := range request.Nodes {
		if node.Options["mon"] == "Y" || node.Options["mon"] == "y" {
			var mon backend.Mon
			mon.Node = node.Hostname
			mon.PublicIP4 = node_ips[node.Hostname]["public"]
			mon.ClusterIP4 = node_ips[node.Hostname]["cluster"]
			mons = append(mons, mon)
		}
	}
	log.Println(mons)

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	// Update nodes details
	for _, node := range request.Nodes {
		coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
		if err := coll.Update(
			bson.M{"hostname": node.Hostname},
			bson.M{"$set": bson.M{
				"clusterid": *uuid,
				"administrativestatus": models.USED,
				"clusterip": node_ips[node.Hostname]["cluster"],
				"publicaddressipv4": node_ips[node.Hostname]["cluster"]}}); err != nil {
			*resp = utils.WriteResponse(http.StatusInternalServerError, err.Error())
			return err
		}
	}
	log.Println("Nodes updated")
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	var cluster models.StorageCluster
	if err := coll.Find(bson.M{"clusterid": *uuid}).One(&cluster); err != nil {
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error getting cluster details %v", err))
		return nil
	}
	log.Println(cluster)

	if len(mons) > 0 {
		// Add mon
		ret_val, err := salt_backend.AddMon(cluster.ClusterName, mons)
		if err != nil {
			*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Failed to add mon %v", err))
			return nil
		}
		if ret_val {
			// Start mons
			ret_val, err := startMon(mons)
			if !ret_val {
				*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Failed to start mon %v", err))
				return nil
			}
			log.Println("Mons started")

			// Add mons
			ret_val, _ = addMonsToDB(*uuid, mons)
			if !ret_val {
				*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Add mons persist failed %v", err))
				return nil
			}
			log.Println("Mons added")
		}
	}

	// Add OSDs
	ret_val, err := addOSDs(*uuid, cluster.ClusterName, request.Nodes)
	if !ret_val {
		*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Add OSDs failed %v", err))
		return nil
	}
	log.Println("OSDs added")

	*resp = utils.WriteResponse(http.StatusOK, "Done")
	return nil
}
