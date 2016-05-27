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
	"github.com/skyrings/skyring-common/monitoring"
	"github.com/skyrings/skyring-common/tools/logger"
	"github.com/skyrings/skyring-common/tools/task"
	"github.com/skyrings/skyring-common/tools/uuid"
	"gopkg.in/mgo.v2/bson"
	"net/http"
	"strconv"
	"strings"
	"time"

	bigfin_models "github.com/skyrings/bigfin/bigfinmodels"
	bigfin_conf "github.com/skyrings/bigfin/conf"
	bigfin_task "github.com/skyrings/bigfin/tools/task"
	skyring_util "github.com/skyrings/skyring-common/utils"
)

func (s *CephProvider) GetClusterNodesForImport(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext
	bootstrapNode := req.RpcRequestVars["bootstrapnode"]
	var clusterForImport models.ClusterForImport

	out, err := cephapi_backend.GetCluster(bootstrapNode, ctxt)
	if err != nil {
		logger.Get().Error(
			"%s-Error getting cluster details. error: %v",
			ctxt,
			err)
		*resp = utils.WriteResponse(
			http.StatusInternalServerError,
			"Error getting cluster details")
		return err
	}
	clusterForImport.ClusterName = out.Name
	clusterForImport.ClusterId = out.Id
	clusterForImport.Compatible = true

	nodes, err := cephapi_backend.GetClusterNodes(bootstrapNode, out.Id, ctxt)
	if err != nil {
		logger.Get().Error(
			"%s-Error getting nodes participating in the cluster: %v",
			ctxt,
			out.Id)
		*resp = utils.WriteResponse(
			http.StatusInternalServerError,
			"Error getting nodes participating in the cluster")
		return err
	}
	var clusterNodes []models.NodeForImport
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	var fetchedNode models.Node
	for _, node := range nodes {
		clusterNode := models.NodeForImport{
			Name: node.FQDN,
		}
		var nodeType []string
		for _, service := range node.Services {
			switch service.Type {
			case bigfin_models.NODE_SERVICE_MON:
				if ok := skyring_util.StringInSlice(bigfin_models.NODE_SERVICE_MON, nodeType); !ok {
					nodeType = append(nodeType, bigfin_models.NODE_SERVICE_MON)
				}
			case bigfin_models.NODE_SERVICE_OSD:
				if ok := skyring_util.StringInSlice(bigfin_models.NODE_SERVICE_OSD, nodeType); !ok {
					nodeType = append(nodeType, bigfin_models.NODE_SERVICE_OSD)
				}
			}
		}
		clusterNode.Type = nodeType
		if strings.HasPrefix(bootstrapNode, node.FQDN) {
			if node.CephVersion != "" && node.CephVersion != "null" {
				nodeVerStr := fmt.Sprintf(
					"%s.%s",
					strings.Split(node.CephVersion, ".")[0],
					strings.Split(node.CephVersion, ".")[1])
				nodeCephVersion, _ := strconv.ParseFloat(nodeVerStr, 64)
				clusterForImport.Compatible = (nodeCephVersion >= bigfin_conf.ProviderConfig.Provider.CompatVersion)
				clusterForImport.Version = nodeVerStr
			}
		}
		if err := coll.Find(
			bson.M{"hostname": bson.M{
				"$regex":   node.FQDN,
				"$options": "$i"}}).One(&fetchedNode); err != nil {
			clusterNode.Found = false
		} else {
			clusterNode.Found = true
			clusterNode.Name = fetchedNode.Hostname
		}
		clusterNodes = append(clusterNodes, clusterNode)
	}

	clusterForImport.Nodes = clusterNodes
	result, err := json.Marshal(clusterForImport)
	if err != nil {
		logger.Get().Error(
			"%s-Error forming the output for import cluster nodes. error: %v",
			ctxt,
			err)
		*resp = utils.WriteResponse(
			http.StatusInternalServerError,
			fmt.Sprintf(
				"Error forming the output. error: %v",
				err))
		return err
	}
	*resp = utils.WriteResponseWithData(http.StatusOK, "", result)
	return nil
}

func (s *CephProvider) ImportCluster(req models.RpcRequest, resp *models.RpcResponse) error {
	var request models.ImportClusterRequest
	ctxt := req.RpcRequestContext

	if err := json.Unmarshal(req.RpcRequestData, &request); err != nil {
		logger.Get().Error(
			fmt.Sprintf("%s-Unbale to parse the import cluster request. error: %v",
				ctxt,
				err))
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request. error: %v", err))
		return err
	}

	asyncTask := func(t *task.Task) {
		// Get cluster details and populate
		t.UpdateStatus("Updating cluster generic dtails (name/id)")
		cluster_uuid, clusterName, err := PopulateClusterDetails(request.BootstrapNode, ctxt)
		if err != nil {
			utils.FailTask(
				"Failed to fetch and populate cluster details",
				err,
				t)
			return
		}
		setClusterState(*cluster_uuid, models.CLUSTER_STATE_CREATING, ctxt)

		// Get the cluster network details and populate
		t.UpdateStatus("Updating cluster network details")
		if err := PopulateClusterNetworkDetails(request.BootstrapNode, *cluster_uuid, ctxt); err != nil {
			utils.FailTask(
				fmt.Sprintf(
					"Error fetching and populating network details for cluster: %s",
					clusterName),
				err,
				t)
			setClusterState(*cluster_uuid, models.CLUSTER_STATE_FAILED, ctxt)
			return
		}
		// Get and populate cluster status details
		t.UpdateStatus("Updating cluster status")
		if err := PopulateClusterStatus(request.BootstrapNode, *cluster_uuid, ctxt); err != nil {
			utils.FailTask(
				fmt.Sprintf(
					"Failed to fetch and populate status details for cluster: %s",
					clusterName),
				err,
				t)
			setClusterState(*cluster_uuid, models.CLUSTER_STATE_FAILED, ctxt)
			return
		}
		// Get and update nodes of the cluster
		t.UpdateStatus("Updating cluster nodes")
		failedNodes, err := PopulateClusterNodes(request.BootstrapNode, *cluster_uuid, request.Nodes, ctxt)
		if err != nil {
			utils.FailTask(
				fmt.Sprintf(
					"Failed populating node details for cluster: %s",
					clusterName),
				err,
				t)
			setClusterState(*cluster_uuid, models.CLUSTER_STATE_FAILED, ctxt)
			return
		}
		if len(failedNodes) > 0 {
			t.UpdateStatus("Failed to updated details of nodes: %v", failedNodes)
		}
		// Get and update storage pools
		t.UpdateStatus("Updating cluster storages")
		if err := PopulateStoragePools(request.BootstrapNode, *cluster_uuid, ctxt); err != nil {
			t.UpdateStatus("Failed populating storage pools details")
			logger.Get().Warning(
				"%s-Failed populating storage pools details for cluster: %s. error: %v",
				ctxt,
				clusterName,
				err)
		}
		// Get and update OSDs
		t.UpdateStatus("Updating cluster SLUs")
		if err := PopulateClusterOSDs(request.BootstrapNode, *cluster_uuid, ctxt); err != nil {
			t.UpdateStatus("Failed populating OSD details")
			logger.Get().Warning(
				"%s-Failed populating OSD details for cluster: %s. error: %v",
				ctxt,
				clusterName,
				err)
		}
		// Get and update block devices
		/*t.UpdateStatus("Updating cluster block devices")
		if err := PopulateBlockDevices(request.BootstrapNode, *cluster_uuid, ctxt); err != nil {
			t.UpdateStatus("Failed populating block devices details")
			logger.Get().Warning(
				"%s-Failed populating block devices details for cluster: %s. error: %v",
				ctxt,
				clusterName,
				err)
		}*/
		setClusterState(*cluster_uuid, models.CLUSTER_STATE_ACTIVE, ctxt)

		// Create default EC profiles
		t.UpdateStatus("Creating default EC profiles")
		if ok, err := CreateDefaultECProfiles(
			ctxt,
			request.BootstrapNode,
			*cluster_uuid); !ok || err != nil {
			logger.Get().Error(
				"%s-Error creating default EC profiles for cluster: %s. error: %v",
				ctxt,
				clusterName,
				err)
			t.UpdateStatus("Could not create default EC profile")
		}

		//Update the CRUSH MAP
		/*t.UpdateStatus("Updating the CRUSH Map")
		if err := updateCrushMap(
			ctxt,
			request.BootstrapNode,
			*cluster_uuid); err != nil {
			logger.Get().Error(
				"%s-Error updating the Crush map for cluster: %s. error: %v",
				ctxt,
				clusterName,
				err)
			t.UpdateStatus("Failed to update Crush map")
			t.Done(models.TASK_STATUS_SUCCESS)
			return
		}*/

		// Update the notification details
		t.UpdateStatus("Creating notification subscription configuartions")
		var bigfin_notifications []models.NotificationSubscription
		for _, notification := range bigfin_models.NOTIFICATIONS_SUPPORTED {
			bigfin_notifications = append(bigfin_notifications, models.NotificationSubscription{
				Name:    notification,
				Enabled: false,
			})
		}
		for _, notification := range models.NOTIFICATIONS_SUPPORTED {
			bigfin_notifications = append(bigfin_notifications, models.NotificationSubscription{
				Name:    notification,
				Enabled: false,
			})
		}
		sessionCopy := db.GetDatastore().Copy()
		defer sessionCopy.Close()
		notifSubsColl := sessionCopy.
			DB(conf.SystemConfig.DBConfig.Database).
			C(models.COLL_NAME_CLUSTER_NOTIFICATION_SUBSCRIPTIONS)
		if err := notifSubsColl.Insert(
			models.ClusterNotificationSubscription{
				ClusterId:     *cluster_uuid,
				Notifications: bigfin_notifications}); err != nil {
			logger.Get().Error(
				"%s-Error persisting the default notification subscriptions on cluster %s. error %v",
				ctxt,
				clusterName,
				err)
			t.UpdateStatus("Could not create notification subscription configuartions")
		}

		t.UpdateStatus("Success")
		t.Done(models.TASK_STATUS_SUCCESS)
		return
	}
	if taskId, err := bigfin_task.GetTaskManager().Run(
		bigfin_conf.ProviderName,
		fmt.Sprintf("%s-Import Cluster", bigfin_conf.ProviderName),
		asyncTask,
		nil,
		nil,
		nil); err != nil {
		logger.Get().Error(
			"%s - Task creation failed for import cluster. error: %v",
			ctxt,
			err)
		*resp = utils.WriteResponse(
			http.StatusInternalServerError,
			"Task creation failed for import cluster")
		return err
	} else {
		*resp = utils.WriteAsyncResponse(
			taskId,
			"Task Created for import cluster",
			[]byte{})
	}
	return nil
}

func PopulateClusterDetails(bootstrapNode string, ctxt string) (*uuid.UUID, string, error) {
	out, err := cephapi_backend.GetCluster(bootstrapNode, ctxt)
	if err != nil {
		return nil, "", fmt.Errorf("%s-Error getting cluster details. error: %v", ctxt, err)
	}

	var cluster models.Cluster = models.Cluster{
		ClusterId: out.Id,
		Name:      out.Name,
		Type:      bigfin_conf.ProviderName,
	}
	cluster.MonitoringInterval = monitoring.DefaultClusterMonitoringInterval
	cluster.Monitoring = models.MonitoringState{
		Plugins:    utils.GetProviderSpecificDefaultThresholdValues(),
		StaleNodes: []string{},
	}
	cluster.CompatVersion = fmt.Sprintf("%f", bigfin_conf.ProviderConfig.Provider.CompatVersion)
	cluster.AutoExpand = true

	// Persist the cluster details
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Insert(cluster); err != nil {
		return nil, "", fmt.Errorf("%s-Error persisting the cluster. error: %v", ctxt, err)
	}

	return &out.Id, out.Name, nil
}

func PopulateClusterNetworkDetails(bootstrapNode string, clusterId uuid.UUID, ctxt string) error {
	out, err := cephapi_backend.GetClusterNetworks(bootstrapNode, clusterId, ctxt)
	if err != nil {
		return fmt.Errorf(
			"%s-Error getting network details of cluster: %v. error: %v",
			ctxt,
			clusterId,
			err)
	}

	// Update the cluster network details
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Update(
		bson.M{"clusterid": clusterId},
		bson.M{"$set": bson.M{
			"networks.public":  out.Public,
			"networks.cluster": out.Cluster}}); err != nil {
		return fmt.Errorf(
			"%s-Error updating network details for cluster: %v. error: %v",
			ctxt,
			clusterId,
			err)
	}
	return nil
}

func PopulateClusterStatus(bootstrapNode string, clusterId uuid.UUID, ctxt string) error {
	out, err := cephapi_backend.GetClusterStatus(bootstrapNode, clusterId, "", ctxt)
	if err != nil {
		return fmt.Errorf(
			"%s-Error getting status of cluster: %v. error: %v",
			ctxt,
			clusterId,
			err)
	}
	var status models.ClusterStatus
	if val, ok := cluster_status_map[out]; ok {
		status = val
	} else {
		status = models.CLUSTER_STATUS_UNKNOWN
	}

	// Update the status of the cluster
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Update(
		bson.M{"clusterid": clusterId},
		bson.M{"$set": bson.M{"status": status}}); err != nil {
		return fmt.Errorf(
			"%s-Failed updating the status of cluster: %v. error: %v",
			ctxt,
			clusterId,
			err)
	}
	return nil
}

func PopulateClusterNodes(bootstrapNode string, clusterId uuid.UUID, nodes []string, ctxt string) ([]string, error) {
	nodes = append(nodes, bootstrapNode)
	failedNodes, err := PopulateNodeNetworkDetails(clusterId, nodes, ctxt)
	if err != nil {
		return failedNodes, fmt.Errorf(
			"%s-Error populating node network details for cluster: %v. error: %v",
			ctxt,
			clusterId,
			err)
	}
	if err := syncStorageNodes(bootstrapNode, clusterId, ctxt); err != nil {
		return failedNodes, fmt.Errorf(
			"%s-Error fetching and populating storages node for cluster: %v. error: %v",
			ctxt,
			clusterId,
			err)
	}
	return failedNodes, nil
}

func PopulateNodeNetworkDetails(clusterId uuid.UUID, nodes []string, ctxt string) ([]string, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	coll1 := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)

	var cluster models.Cluster
	if err := coll.Find(bson.M{"clusterid": clusterId}).One(&cluster); err != nil {
		return []string{}, fmt.Errorf("Error fetching the cluster details. error: %v", clusterId)
	}

	var fetchedNode models.Node
	var failedNodes []string
	var updates bson.M = make(map[string]interface{})
	for _, node := range nodes {
		if err := coll1.Find(bson.M{"hostname": node}).One(&fetchedNode); err != nil {
			logger.Get().Error(
				"%s-Error getting details of node: %s. error: %v",
				ctxt,
				fetchedNode.Hostname,
				err)
			failedNodes = append(failedNodes, fetchedNode.Hostname)
			continue
		}
		if fetchedNode.State == models.NODE_STATE_INITIALIZING {
			for count := 0; count < 30; count++ {
				time.Sleep(10 * time.Second)
				if err := coll1.Find(bson.M{"hostname": node}).One(&fetchedNode); err != nil {
					logger.Get().Error(
						"%s-Error getting details of node: %s. error: %v",
						ctxt,
						fetchedNode.Hostname,
						err)
					failedNodes = append(failedNodes, fetchedNode.Hostname)
					continue
				}
				if fetchedNode.State == models.NODE_STATE_ACTIVE {
					break
				}
			}

		}
		if fetchedNode.State == models.NODE_STATE_INITIALIZING {
			logger.Get().Error(
				"Node %s still in initializing state. Continuing to other",
				node)
			failedNodes = append(failedNodes, fetchedNode.Hostname)
			continue
		}
		updates["clusterid"] = clusterId
		for _, host_addr := range fetchedNode.NetworkInfo.IPv4 {
			if ok, _ := utils.IsIPInSubnet(host_addr, cluster.Networks.Public); ok {
				updates["publicip4"] = host_addr
			}
			if ok, _ := utils.IsIPInSubnet(host_addr, cluster.Networks.Cluster); ok {
				updates["clusterip4"] = host_addr
			}
		}
		if updates["publicip4"] == "" {
			updates["publicip4"] = fetchedNode.ManagementIP4
		}
		if updates["clusterip4"] == "" {
			updates["clusterip4"] = fetchedNode.ManagementIP4
		}
		if err := coll1.Update(
			bson.M{"hostname": fetchedNode.Hostname},
			bson.M{"$set": updates}); err != nil {
			failedNodes = append(failedNodes, fetchedNode.Hostname)
		}
	}
	return failedNodes, nil
}

func PopulateStoragePools(bootstrapNode string, clusterId uuid.UUID, ctxt string) error {
	if err := syncStoragePools(bootstrapNode, clusterId, ctxt); err != nil {
		return fmt.Errorf(
			"%s-Error fetching and populating storage pools for cluster: %s. error: %v",
			ctxt,
			clusterId,
			err)
	}
	return nil
}

func PopulateClusterOSDs(bootstrapNode string, clusterId uuid.UUID, ctxt string) error {
	if err := syncOsds(bootstrapNode, clusterId, ctxt); err != nil {
		return fmt.Errorf(
			"%s-Error fetching and populating OSDs for cluster: %v. error: %v",
			ctxt,
			clusterId,
			err)
	}
	return nil
}

func PopulateBlockDevices(bootstrapNode string, clusterId uuid.UUID, ctxt string) error {
	if err := syncRBDs(bootstrapNode, clusterId, ctxt); err != nil {
		return fmt.Errorf(
			"%s-Error fetching and populating RBDs for cluster: %v. error: %v",
			ctxt,
			clusterId,
			err)
	}
	return nil
}
