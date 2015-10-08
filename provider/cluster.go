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
	"github.com/skyrings/bigfin/backend"
	"github.com/skyrings/bigfin/backend/salt"
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/skyring/conf"
	"github.com/skyrings/skyring/db"
	"github.com/skyrings/skyring/models"
	"github.com/skyrings/skyring/tools/uuid"
	"gopkg.in/mgo.v2/bson"
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
		*resp = utils.WriteResponse(http.StatusBadRequest, "Unbale to parse the request")
		return err
	}

	// Get the cluster and public IPs for nodes
	var node_ips = make(map[string]map[string]string)
	cluster_subnet := request.Networks.Cluster
	public_subnet := request.Networks.Public
	for _, node := range request.Nodes {
		host_addrs, err := net.LookupHost(node.Hostname)
		var m = make(map[string]string)
		if err == nil {
			for _, host_addr := range host_addrs {
				if ok, _ := utils.IsIPInSubnet(host_addr, cluster_subnet); ok {
					m["cluster"] = host_addr
				}
				if ok, _ := utils.IsIPInSubnet(host_addr, public_subnet); ok {
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
		*resp = utils.WriteResponse(http.StatusInternalServerError, "Error while cluster creation")
		return err
	}

	if ret_val {
		// If success, persist the details in DB
		request.ClusterStatus = CLUSTER_STATUS_UP
		sessionCopy := db.GetDatastore().Copy()
		defer sessionCopy.Close()

		coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
		if err := coll.Insert(request); err != nil {
			*resp = utils.WriteResponse(http.StatusInternalServerError, err.Error())
			return err
		}

		for _, node := range request.Nodes {
			coll = sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
			if err := coll.Update(
				bson.M{"hostname": node.Hostname},
				bson.M{"$set": bson.M{
					"clusterid": request.ClusterId,
					"managedstate": models.NODE_STATE_USED,
					"clusterip": node_ips[node.Hostname]["cluster"],
					"publicaddressipv4": node_ips[node.Hostname]["cluster"]}}); err != nil {
				*resp = utils.WriteResponse(http.StatusInternalServerError, err.Error())
				return err
			}
		}

		// Send response
		*resp = utils.WriteResponse(http.StatusOK, "Done")
		return nil
	}
	return nil
}
