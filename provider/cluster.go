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
	RULEOFFSET          = 10000
	MINSIZE             = 1
	MAXSIZE             = 10
	MON                 = "MON"
	OSD                 = "OSD"
	JOURNALSIZE         = 5120
	MAX_JOURNALS_ON_SSD = 6
)

type JournalDetail struct {
	JournalDisk string  `json:"journaldisk"`
	SSD         bool    `json:"type"`
	Size        uint64  `json:"size"`
	JournalDisk string  `json:"journaldisk"`
	SSD         bool    `json:"type"`
	Size        uint64  `json:"size"`
	Reweight    float64 `json:"reweight"`
	OsdJournal  string  `json:"osd_journal"`
}

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

	// Validate the cluster nodes
	if err := validateClusterNodes(nodes, ctxt); err != nil {
		logger.Get().Error("%s-%v", ctxt, err)
		*resp = utils.WriteResponse(
			http.StatusBadRequest,
			fmt.Sprintf("Error validating nodes. error: %v", err))
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

	if request.JournalSize == "" {
		request.JournalSize = fmt.Sprintf("%dMB", JOURNALSIZE)
	}

	nodeRoleMapFromRequest := make(map[string][]string)
	var flag bool

	for _, req_node := range request.Nodes {
		if util.StringInSlice("MON", req_node.NodeType) {
			flag = true
		}
		nodeRoleMapFromRequest[req_node.NodeId] = req_node.NodeType
	}
	if !flag {
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
				cluster.CompatVersion = fmt.Sprintf("%f", bigfin_conf.ProviderConfig.Provider.CompatVersion)
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
				cluster.JournalSize = request.JournalSize
				cluster.Monitoring = models.MonitoringState{
					Plugins:    utils.GetProviderSpecificDefaultThresholdValues(),
					StaleNodes: []string{},
				}
				var bigfin_notifications []models.NotificationSubscription
				for _, notification := range bigfinmodels.NOTIFICATIONS_SUPPORTED {
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
				notifSubsColl := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_CLUSTER_NOTIFICATION_SUBSCRIPTIONS)
				if err := notifSubsColl.Insert(models.ClusterNotificationSubscription{ClusterId: *cluster_uuid, Notifications: bigfin_notifications}); err != nil {
					logger.Get().Error("%s-Error persisting the default notification subscriptions on cluster %s.Error %v", ctxt, request.Name, err)
					return
				}
				cluster.MonitoringInterval = request.MonitoringInterval
				if cluster.MonitoringInterval == 0 {
					cluster.MonitoringInterval = monitoring.DefaultClusterMonitoringInterval
				}
				coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
				if err := coll.Insert(cluster); err != nil {
					utils.FailTask(fmt.Sprintf("%s-Error persisting the cluster %s", ctxt, request.Name), err, t)
					return
				}

				nodecoll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
				for _, node := range nodes {
					if err := nodecoll.Update(
						bson.M{"nodeid": node.NodeId},
						bson.M{"$set": bson.M{
							"clusterip4": node_ips[node.NodeId]["cluster"],
							"publicip4":  node_ips[node.NodeId]["public"],
							"roles":      nodeRoleMapFromRequest[node.NodeId.String()]}}); err != nil {

						logger.Get().Error(
							"%s-Error updating the details for node: %s. error: %v",
							ctxt,
							node.Hostname,
							err)
						t.UpdateStatus(fmt.Sprintf("Failed to update details of node: %s", node.Hostname))
					}
				}

				if err := CreateClusterUsingInstaller(cluster_uuid, request, nodes, node_ips, t, ctxt); err != nil {

					utils.FailTask(fmt.Sprintf("%s-Cluster creation failed for %s", ctxt, request.Name), err, t)
					setClusterState(*cluster_uuid, models.CLUSTER_STATE_FAILED, ctxt)
					return
				}

				// Update the cluster status at the last
				t.UpdateStatus("Updating the status of the cluster")
				clusterStatus, err := cluster_status(*cluster_uuid, request.Name, ctxt)
				if err := coll.Update(
					bson.M{"clusterid": *cluster_uuid},
					bson.M{"$set": bson.M{
						"status": clusterStatus,
						"state":  models.CLUSTER_STATE_ACTIVE}}); err != nil {
					t.UpdateStatus("Error updating the cluster status")
				}

				// Delete the default created pool "rbd"
				t.UpdateStatus("Removing default created pool \"rbd\"")
				monnode, err := GetCalamariMonNode(*cluster_uuid, ctxt)
				if err != nil {
					logger.Get().Error("%s-Could not get random mon", ctxt)
					t.UpdateStatus("Could not get the Monitor for configuration")
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
				/*t.UpdateStatus("Updating the CRUSH Map")
				if err := updateCrushMap(ctxt, monnode.Hostname, *cluster_uuid); err != nil {
					logger.Get().Error("%s-Error updating the Crush map for cluster: %s. error: %v", ctxt, request.Name, err)
					t.UpdateStatus("Failed to update Crush map")
				}*/
				t.UpdateStatus("Success")
				t.Done(models.TASK_STATUS_SUCCESS)
				return

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

func CreateClusterUsingSalt(cluster_uuid *uuid.UUID, request models.AddClusterRequest,
	nodes map[uuid.UUID]models.Node, node_ips map[uuid.UUID]map[string]string,
	t *task.Task, ctxt string) error {

	var mons []backend.Mon
	for _, req_node := range request.Nodes {
		if util.StringInSlice(MON, req_node.NodeType) {
			var mon backend.Mon
			nodeid, _ := uuid.Parse(req_node.NodeId)
			mon.Node = nodes[*nodeid].Hostname
			mon.PublicIP4 = node_ips[*nodeid]["public"]
			mon.ClusterIP4 = node_ips[*nodeid]["cluster"]
			mons = append(mons, mon)
		}
	}
	t.UpdateStatus("Creating cluster with mon: %s", mons[0].Node)

	if ret_val, err := salt_backend.CreateCluster(request.Name, *cluster_uuid, []backend.Mon{mons[0]}, ctxt); !ret_val || err != nil {
		return err
	}

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

	// Start and persist the mons
	t.UpdateStatus("Starting and persisting mons")
	ret_val, err := startAndPersistMons(*cluster_uuid, succeededMons, ctxt)
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
		logger.Get().Error(
			"%s-Error getting updated nodes list post create cluster %s. error: %v",
			ctxt,
			request.Name,
			err)
		return err
	}
	failedOSDs, succeededOSDs := addOSDs(
		*cluster_uuid,
		request.Name,
		updated_nodes,
		request.Nodes,
		t,
		ctxt)

	if len(failedOSDs) != 0 {
		var osds []string
		for _, osd := range failedOSDs {
			osds = append(osds, fmt.Sprintf("%s:%s", osd.Node, osd.Device))
		}
		t.UpdateStatus(fmt.Sprintf("OSD addition failed for %v", osds))
		if len(succeededOSDs) == 0 {
			t.UpdateStatus("Failed adding all OSDs")
			logger.Get().Error(
				"%s-Failed adding all OSDs while create cluster %s. error: %v",
				ctxt,
				request.Name,
				err)
		}
	}
	return nil
}

func validateClusterNodes(nodes map[uuid.UUID]models.Node, ctxt string) error {
	var failedNodes []string
	for _, node := range nodes {
		if ok := salt_backend.ParticipatesInCluster(node.Hostname, ctxt); ok {
			failedNodes = append(failedNodes, node.Hostname)
		}
	}
	if len(failedNodes) > 0 {
		return fmt.Errorf(
			"Nodes %v already participating in a cluster."+
				" New Cluster cannot be created using the node",
			failedNodes)
	}
	return nil
}

func CreateClusterUsingInstaller(cluster_uuid *uuid.UUID, request models.AddClusterRequest,
	nodes map[uuid.UUID]models.Node, node_ips map[uuid.UUID]map[string]string,
	t *task.Task, ctxt string) error {

	var (
		clusterMons               []map[string]interface{}
		failedMons, succeededMons []string
	)
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	t.UpdateStatus("Configuring the mons")
	var calamariInitialized bool
	for _, req_node := range request.Nodes {
		if util.StringInSlice(MON, req_node.NodeType) {
			mon := make(map[string]interface{})
			nodeid, err := uuid.Parse(req_node.NodeId)
			if err != nil {
				logger.Get().Error("%s-Failed to parse uuid for node %v. error: %v", ctxt, req_node.NodeId, err)
				t.UpdateStatus(fmt.Sprintf("Failed to add MON node: %v", req_node.NodeId))
				continue
			}
			// By default dont start calamari on any mon.
			// Later on only one it sould be started
			if !calamariInitialized {
				mon["calamari"] = true
			} else {
				mon["calamari"] = false
			}
			mon["host"] = nodes[*nodeid].Hostname
			mon["address"] = node_ips[*nodeid]["cluster"]
			mon["fsid"] = cluster_uuid.String()
			mon["monitor_secret"] = "AQA7P8dWAAAAABAAH/tbiZQn/40Z8pr959UmEA=="
			mon["cluster_name"] = request.Name
			mon["cluster_network"] = request.Networks.Cluster
			mon["public_network"] = request.Networks.Public
			mon["redhat_storage"] = conf.SystemConfig.Provisioners[bigfin_conf.ProviderName].RedhatStorage
			if len(clusterMons) > 0 {
				mon["monitors"] = clusterMons
			}

			if err := installer_backend.Configure(ctxt, t, MON, mon); err != nil {
				failedMons = append(failedMons, mon["host"].(string))
				logger.Get().Error("%s-Failed to add MON %v. error: %v", ctxt, mon["host"].(string), err)
			} else {
				t.UpdateStatus(fmt.Sprintf("Added mon node: %s", mon["host"].(string)))
				succeededMons = append(succeededMons, mon["host"].(string))

				if !calamariInitialized {
					t.UpdateStatus("Starting calamari on: %s", nodes[*nodeid].Hostname)
					if err := salt_backend.StartCalamari(nodes[*nodeid].Hostname, ctxt); err != nil {
						t.UpdateStatus("Failed to start calamari on node: %s", mon)
						logger.Get().Warning(
							"%s-Could not start calamari on mon: %s. error: %v",
							ctxt,
							mon,
							err)
					} else {
						t.UpdateStatus("Started calamari on node: %s", nodes[*nodeid].Hostname)
						if err := coll.Update(
							bson.M{"hostname": nodes[*nodeid].Hostname},
							bson.M{"$set": bson.M{
								"options.calamari": "Y"}}); err != nil {
						}
						calamariInitialized = true
					}
				} else {
					if err := coll.Update(
						bson.M{"hostname": nodes[*nodeid].Hostname},
						bson.M{"$set": bson.M{
							"options.calamari": "N"}}); err != nil {
					}
				}
				clusterMon := make(map[string]interface{})
				clusterMon["host"] = nodes[*nodeid].Hostname
				clusterMon["address"] = node_ips[*nodeid]["cluster"]
				clusterMons = append(clusterMons, clusterMon)
			}

		}
	}

	if len(failedMons) > 0 {
		t.UpdateStatus(fmt.Sprintf("Failed to add mon(s) %v", failedMons))
		if len(succeededMons) == 0 {
			return errors.New("Cluster creation failed. All mons failed")
		}
	}

	t.UpdateStatus("Persisting mons")
	//var calamariStarted bool
	for _, mon := range succeededMons {
		if err := coll.Update(
			bson.M{"hostname": mon},
			bson.M{"$set": bson.M{
				"clusterid":   *cluster_uuid,
				"options.mon": "Y"}}); err != nil {
			return err
		}
		logger.Get().Info(fmt.Sprintf("%s-Added mon node: %s", ctxt, mon))
		/*if !calamariStarted {
			t.UpdateStatus("Starting calamari on: %s", mon)
			if err := salt_backend.StartCalamari(mon, ctxt); err != nil {
				t.UpdateStatus("Failed to start calamari on node: %s", mon)
				logger.Get().Warning(
					"%s-Could not start calamari on mon: %s. error: %v",
					ctxt,
					mon,
					err)
				continue
			} else {
				t.UpdateStatus("Started calamari on node: %s", mon)
				calamariStarted = true
				if err := coll.Update(
					bson.M{"hostname": mon},
					bson.M{"$set": bson.M{
						"options.calamari": "Y"}}); err != nil {
					return err
				}
			}
		}*/
	}
	if !calamariInitialized {
		t.UpdateStatus("Could not start calamari on any mons")
		return fmt.Errorf("Could not start calamari on any mons")
	}

	var osdPresent bool
	for _, req_node := range request.Nodes {
		if util.StringInSlice(OSD, req_node.NodeType) {
			osdPresent = true
			break
		}
	}
	if osdPresent {
		failedOSDs, succeededOSDs, err := configureOSDs(*cluster_uuid, request, clusterMons, t, false, ctxt)
		if err != nil {
			return err
		}
		if len(failedOSDs) != 0 {
			t.UpdateStatus(fmt.Sprintf("OSD addition failed for %v", failedOSDs))
			if len(succeededOSDs) == 0 {
				t.UpdateStatus("Failed adding all OSDs")
				logger.Get().Error(
					"%s-Failed adding all OSDs while create cluster %s. error: %v",
					ctxt,
					request.Name,
					err)
			}
		}
	}
	return nil
}

func configureOSDs(clusterId uuid.UUID, request models.AddClusterRequest,
	clusterMons []map[string]interface{}, t *task.Task,
	expand bool, ctxt string) ([]string, []string, error) {

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	var (
		failedOSDs    []string
		succeededOSDs []string
		slus          = make(map[string]models.StorageLogicalUnit)
	)
	// In case of expand cluster journal size to be taken from DB
	if request.JournalSize == "" {
		var cluster models.Cluster
		collClusters := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
		if err := collClusters.Find(bson.M{"clusterid": clusterId}).One(&cluster); err != nil {
			logger.Get().Error(
				"%s-Error getting cluster details for %v. error: %v",
				ctxt,
				clusterId,
				err)
			return failedOSDs, succeededOSDs, err
		}
		request.JournalSize = cluster.JournalSize
	}
	jSize := utils.SizeFromStr(request.JournalSize)

	nodes, err := util.GetNodes(request.Nodes)
	if err != nil {
		logger.Get().Error(
			"%s-Error getting updated nodes list for cluster %s. error: %v",
			ctxt,
			request.Name,
			err)
		return failedOSDs, succeededOSDs, err
	}
	t.UpdateStatus("Configuring the OSDs")
	var successNodeIds []uuid.UUID
	for _, requestNode := range request.Nodes {
		if !util.StringInSlice(models.NODE_TYPE_OSD, requestNode.NodeType) {
			continue
		}
		uuid, err := uuid.Parse(requestNode.NodeId)
		if err != nil {
			logger.Get().Error(
				"%s-Error parsing node id: %s while add OSD for cluster: %s. error: %v",
				ctxt,
				requestNode.NodeId,
				request.Name,
				err)
			t.UpdateStatus(fmt.Sprintf("Failed to add OSD(s) from node: %v", requestNode.NodeId))
			continue
		}

		devices := make(map[string]models.Disk)
		storageNode := nodes[*uuid]
		// Form a map of storage node disks
		var nodeDisksMap map[string]models.Disk = make(map[string]models.Disk)
		for _, storageDisk := range storageNode.StorageDisks {
			nodeDisksMap[storageDisk.Name] = storageDisk
		}
		var consumedCount int
		var disksMap map[string]JournalDetail = make(map[string]JournalDetail)
		if expand {
			disksMap, consumedCount = mapPendingSSDJournalDisks(*uuid, requestNode, jSize, t, ctxt)
			// Still left with unused rotational disks
			if consumedCount < len(requestNode.Devices) {
				for diskName, storageDisk := range nodeDisksMap {
					if storageDisk.Type == "disk" &&
						storageDisk.Used == false &&
						diskName != requestNode.Devices[consumedCount].Name {
						var journal = JournalDetail{
							JournalDisk: diskName,
							SSD:         false,
							Size:        storageDisk.Size,
						}
						disksMap[requestNode.Devices[consumedCount].Name] = journal
						delete(nodeDisksMap, diskName)
						delete(nodeDisksMap, requestNode.Devices[consumedCount].Name)
						consumedCount++
					}
					if consumedCount >= len(requestNode.Devices) {
						break
					}
				}
			}
		}

		// Prepare left out disks for journal mapping
		for diskName, storageDisk := range nodeDisksMap {
			for idx := consumedCount; idx < len(requestNode.Devices); idx++ {
				if diskName == requestNode.Devices[idx].Name {
					if storageDisk.Used {
						logger.Get().Warning(
							"%s-Used Disk :%v. skipping",
							ctxt,
							diskName)
						break
					}
					devices[requestNode.Devices[idx].Name] = storageDisk
				} else {
					continue
				}
			}
		}

		// Get the journal mapping for the disks
		diskWithJournalMapped := getDiskWithJournalMapped(devices, request.JournalSize)
		for disk, journal := range disksMap {
			diskWithJournalMapped[disk] = journal
		}
		for disk, journal := range diskWithJournalMapped {
			var osdDet = make(map[string]string)
			osdDet[disk] = journal.JournalDisk
			osd := make(map[string]interface{})
			osd["devices"] = osdDet
			osd["fsid"] = clusterId.String()
			osd["host"] = storageNode.Hostname
			// Convert the journal size to MB and set
			osd["journal_size"] = jSize / 1024
			osd["cluster_name"] = request.Name
			osd["cluster_network"] = request.Networks.Cluster
			osd["public_network"] = request.Networks.Public
			osd["redhat_storage"] = conf.SystemConfig.Provisioners[bigfin_conf.ProviderName].RedhatStorage
			osd["monitors"] = clusterMons

			if err := installer_backend.Configure(ctxt, t, OSD, osd); err != nil {
				failedOSDs = append(failedOSDs, fmt.Sprintf("%v:%v", osd["host"].(string), osd["devices"]))
				logger.Get().Error("%s-Failed to add OSD: %v on Host: %v. error: %v", ctxt, osd["devices"], osd["host"].(string), err)
				continue
			}
			var options = make(map[string]interface{})
			options["node"] = osd["host"].(string)
			options["publicip4"] = storageNode.PublicIP4
			options["clusterip4"] = storageNode.ClusterIP4
			options["device"] = disk
			options["journal"] = journal
			//
			//	TODO: OSD Names needs to be taken care by sync calls
			//
			slu := models.StorageLogicalUnit{
				Type:              models.CEPH_OSD,
				ClusterId:         clusterId,
				NodeId:            storageNode.NodeId,
				StorageDeviceId:   devices[disk].DiskId,
				StorageProfile:    devices[disk].StorageProfile,
				StorageDeviceSize: devices[disk].Size,
				Options:           options,
				Status:            models.SLU_STATUS_UNKNOWN,
				State:             bigfinmodels.OSD_STATE_IN,
				AlmStatus:         models.ALARM_STATUS_CLEARED,
			}
			if ok, err := persistOSD(slu, t, ctxt); err != nil || !ok {
				logger.Get().Error("%s-Error persising %s for cluster: %s. error: %v", ctxt, slu.Name, request.Name, err)
				failedOSDs = append(failedOSDs, fmt.Sprintf("%s:%s", osd["host"].(string), osd["devices"]))
				continue
			}
			slus[fmt.Sprintf("%s:%s", slu.NodeId.String(), slu.Options["device"])] = slu
			succeededOSDs = append(succeededOSDs, fmt.Sprintf("%v:%v", osd["host"].(string), osd["devices"]))
			successNodeIds = append(successNodeIds, *uuid)
		}
	}

	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	for _, nodeid := range successNodeIds {
		if err := coll.Update(
			bson.M{"nodeid": nodeid},
			bson.M{"$set": bson.M{
				"clusterid": clusterId}}); err != nil {
			logger.Get().Error(
				"%s-Error updating clusterId for node: %v post add OSDs for cluster: %s. error: %v",
				ctxt,
				nodeid,
				clusterId.String(),
				err)
		}

	}
	if len(slus) > 0 {
		t.UpdateStatus("Syncing the OSD status")
		//
		//TODO: Sleep will be removed once the events are vailable
		//from calamari on OSD status change. Immeadiately after the
		//the creation the OSD sttaus set to out and down, so wait for
		//sometime to get the right status
		//
		time.Sleep(60 * time.Second)
		for count := 0; count < 3; count++ {
			if err := syncOsdDetails(clusterId, slus, ctxt); err != nil {
				logger.Get().Warning(
					"%s-Error syncing the OSD status. error: %v",
					ctxt,
					err)
				time.Sleep(10 * time.Second)
			} else {
				break
			}
		}
	}
	return failedOSDs, succeededOSDs, nil
}

func mapPendingSSDJournalDisks(
	nodeId uuid.UUID,
	requestNode models.ClusterNode,
	jSize uint64,
	t *task.Task,
	ctxt string) (map[string]JournalDetail, int) {

	var consumedCount int
	var disksMap map[string]JournalDetail = make(map[string]JournalDetail)

	// Get the journal disks from the node
	existingJournals, err := getJournalDisks(nodeId)
	if err != nil {
		logger.Get().Error(
			"%s-Error assessing existing journal disks for mapping of node: %s. error: %v",
			ctxt,
			requestNode.NodeId,
			err)
		t.UpdateStatus(
			"Failed to assess existing journal disks for node: %s",
			requestNode.NodeId)
	}

	// Map to existing journal disk if space available
	for journal, count := range existingJournals {
		if journal.SSD && count < 6 && (journal.Size-jSize*uint64(count)) > jSize {
			for idx := consumedCount; idx < len(requestNode.Devices); idx++ {
				disksMap[requestNode.Devices[idx].Name] = journal
				count++
				consumedCount++
				if count == 6 || (journal.Size-jSize*uint64(count)) < jSize {
					break
				}
			}
		}
	}
	return disksMap, consumedCount
}

func getJournalDisks(nodeId uuid.UUID) (map[JournalDetail]uint, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
	var slus []models.StorageLogicalUnit
	if err := coll.Find(bson.M{"nodeid": nodeId}).All(&slus); err != nil {
		return map[JournalDetail]uint{}, err
	}
	var retVal map[JournalDetail]uint = make(map[JournalDetail]uint)
	for _, slu := range slus {
		if _, ok := slu.Options["journal"]; ok {
			journalDet := slu.Options["journal"].(map[string]interface{})
			var journal = JournalDetail{
				JournalDisk: journalDet["journaldisk"].(string),
				SSD:         journalDet["ssd"].(bool),
				Size:        uint64(journalDet["size"].(int64)),
			}
			retVal[journal] += 1
		}
	}
	return retVal, nil
}

/**
 * This fuction works based on a simple algorithm which maps
 * smaller sized disks as journla for bigger sized disks.
 * To achieve this, it first sorts the list of disks based on
 * size in descending order and then start the mapping of disks
 * with their journal disks.
 *
 * There are below listed four scenarios to be handled
 *
 * CASE-1: All the disks are rotational
 * RESTRICTION: In this case one disk can be used as journal
 * for one disk only
 * LOGIC: Here the logic is very simple. If the no of disks is
 * event valid possible no of OSDs would be (no of disks / 2)
 * else if the no of disks is  odd, the valid no of OSDs would be
 * (no of disks - 1) / 2. Now in sorted list of disks on descending
 * size, first half set of disks get mapped to second half set of
 * disks to use them as journals.
 *
 * CASE-2: All the disks are SSDs
 * RESTRICTION: In this case one disk can be used as journal for
 * upto 6 disks
 * LOGIC: Here in sorted list of disks on descending size, it starts
 * from first disk and starts mapping the last entry in the list as
 * journal. Once 6 disks are mapped to last entry in list, next set
 * of disks start mapping to one but last disk in list as their
 * journal. This continues till all the disks are mapped to their
 * journals. Also while mapping disks to their journal, if no more
 * space available on journal disk, it moves mapping to the next
 * higher sized disk from last.
 * The below diagram explains the logic well
 *
 *   ----------------------------------------------------
 *   |    -----------------------------------------------
 *   |    |    ------------------------------------------
 *   |    |    |    -------------------------------------
 *   |    |    |    |    --------------------------------
 *   |    |    |    |    |    ---------------------------
 *   |    |    |    |    |    |    ----------------    |
 *   |    |    |    |    |    |    |    -----------    |
 *   |    |    |    |    |    |    |    |    ------    |
 *   |    |    |    |    |    |    |    |    |    |    |
 *  ---  ---  ---  ---  ---  ---  ---  ---  ---  ---  ----
 *  |0|  |1|  |2|  |3|  |4|  |5|  |6|  |7|  |8|  |9|  |10|
 *  ---  ---  ---  ---  ---  ---  ---  ---  ---  ---  ----
 *
 * CASE-3: Few disks are rotational and few are SSDs
 * RESTRICTION: First SSDs should be used as journals and a maximum
 * of 6 disks can use one SSD as journal.
 * LOGIC: In this case, first it segregates the list of SSDs and
 * rotational disks. Then starts mapping SSD disk as journal for
 * rotational disk. Once already 6 disks marked to use an SSD as
 * journal or no space available in the selected SSD, it moves to
 * next SSD to use as journal.
 * After this mapping done, we might end up in a situation where
 * more rotational or SSDs disks are left
 * 	SUB CASE-3a: More rotational disks left
 *	LOGIC: Logic in case-1 is repeated for the left out disks
 *
 *	SUB CASE-3b: More SSDs are left
 *	LOGIC: Logic in case-2 is repeated for the left out disks
 */
func getDiskWithJournalMapped(disks map[string]models.Disk, journalSize string) map[string]JournalDetail {
	jSize := utils.SizeFromStr(journalSize)

	var mappedDisks = make(map[string]JournalDetail)
	var ssdCount, rotationalCount, osdCount int

	for _, disk := range disks {
		if disk.SSD {
			ssdCount++
		} else {
			rotationalCount++
		}
	}

	// All the disks are rotational
	var validCount int
	if rotationalCount == len(disks) {
		var disksForSort []models.Disk
		for _, disk := range disks {
			disksForSort = append(disksForSort, disk)
		}
		sortedDisks := SortDisksOnSize(disksForSort)
		if len(disks)%2 == 0 {
			validCount = len(disks) / 2
		} else {
			validCount = (len(disks) - 1) / 2
		}

		for idx := 0; idx < validCount; idx++ {
			var journal = JournalDetail{
				JournalDisk: sortedDisks[(len(disks)-idx)-1].DevName,
				SSD:         false,
				Size:        sortedDisks[(len(disks)-idx)-1].Size,
			}
			mappedDisks[sortedDisks[idx].DevName] = journal
		}
		return mappedDisks
	}

	// All the disks are ssd
	var journalDiskIdx, mappedDiskCountForJournal int
	var ssdDiskSize uint64
	if ssdCount == len(disks) {
		var disksForSort []models.Disk
		for _, disk := range disks {
			disksForSort = append(disksForSort, disk)
		}
		sortedDisks := SortDisksOnSize(disksForSort)
		for idx := 0; idx <= (len(sortedDisks)-journalDiskIdx)-2; idx++ {
			ssdDiskSize = sortedDisks[(len(sortedDisks)-journalDiskIdx)-1].Size - jSize
			mappedDiskCountForJournal++
			osdCount++
			var journal = JournalDetail{
				JournalDisk: sortedDisks[(len(sortedDisks)-journalDiskIdx)-1].DevName,
				SSD:         true,
				Size:        sortedDisks[(len(sortedDisks)-journalDiskIdx)-1].Size,
			}
			mappedDisks[sortedDisks[idx].DevName] = journal
			if mappedDiskCountForJournal == MAX_JOURNALS_ON_SSD || ssdDiskSize < jSize {
				mappedDiskCountForJournal = 0
				journalDiskIdx++
			}
		}
		return mappedDisks
	}

	// Few of the disks are SSD and few rotational
	var ssdDisks, rotationalDisks []models.Disk
	for _, disk := range disks {
		if disk.SSD {
			ssdDisks = append(ssdDisks, disk)
		} else {
			rotationalDisks = append(rotationalDisks, disk)
		}
	}
	journalDiskIdx = 0
	mappedDiskCountForJournal = 0
	for _, disk := range rotationalDisks {
		if journalDiskIdx < len(ssdDisks) {
			ssdDiskSize = ssdDisks[journalDiskIdx].Size - jSize
			mappedDiskCountForJournal++
			osdCount++
			var journal = JournalDetail{
				JournalDisk: ssdDisks[journalDiskIdx].DevName,
				SSD:         true,
				Size:        ssdDisks[journalDiskIdx].Size,
			}
			mappedDisks[disk.DevName] = journal
			if mappedDiskCountForJournal == MAX_JOURNALS_ON_SSD || ssdDiskSize < jSize {
				mappedDiskCountForJournal = 0
				journalDiskIdx++
			}
		} else {
			break
		}
	}

	// If still ssd disks pending, map them among themselves
	// There should not be any rotational disks left by this time
	if journalDiskIdx < len(ssdDisks) {
		var pendingDisks []models.Disk
		for idx := journalDiskIdx; idx < len(ssdDisks); idx++ {
			pendingDisks = append(pendingDisks, ssdDisks[idx])
		}
		sortedDisks := SortDisksOnSize(pendingDisks)

		journalDiskIdx = 0
		mappedDiskCountForJournal = 0
		for idx1 := 0; idx1 <= (len(sortedDisks)-journalDiskIdx)-2; idx1++ {
			ssdDiskSize = sortedDisks[(len(sortedDisks)-journalDiskIdx)-1].Size - jSize
			mappedDiskCountForJournal++
			osdCount++
			var journal = JournalDetail{
				JournalDisk: sortedDisks[(len(sortedDisks)-journalDiskIdx)-1].DevName,
				SSD:         true,
				Size:        sortedDisks[(len(sortedDisks)-journalDiskIdx)-1].Size,
			}
			mappedDisks[sortedDisks[idx1].DevName] = journal
			if mappedDiskCountForJournal == MAX_JOURNALS_ON_SSD || ssdDiskSize < jSize {
				mappedDiskCountForJournal = 0
				journalDiskIdx++
			}
		}
		return mappedDisks
	}

	// If still rotational disks pending, map them among themselves
	if osdCount < len(rotationalDisks) {
		pendingDisksCount := len(rotationalDisks) - osdCount
		if pendingDisksCount%2 == 0 {
			validCount = pendingDisksCount / 2
		} else {
			validCount = (pendingDisksCount - 1) / 2
		}
		var pendingDisks []models.Disk
		for idx := osdCount - 1; idx < len(rotationalDisks); idx++ {
			pendingDisks = append(pendingDisks, rotationalDisks[idx])
		}
		sortedDisks := SortDisksOnSize(pendingDisks)
		for idx2 := 0; idx2 < validCount; idx2++ {
			var journal = JournalDetail{
				JournalDisk: sortedDisks[(len(pendingDisks)-idx2)-1].DevName,
				SSD:         false,
				Size:        sortedDisks[(len(pendingDisks)-idx2)-1].Size,
			}
			mappedDisks[sortedDisks[idx2].DevName] = journal
		}
		return mappedDisks
	}

	return mappedDisks
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
	if len(slus) > 0 {
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
	}
	return failedOSDs, succeededOSDs
}

func persistOSD(slu models.StorageLogicalUnit, t *task.Task, ctxt string) (bool, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
	var existing_slu models.StorageLogicalUnit
	if err := coll.Find(bson.M{"nodeid": slu.NodeId,
		"clusterid":      slu.ClusterId,
		"options.device": slu.Options["device"]}).One(&existing_slu); err != nil {
		if err.Error() == mgo.ErrNotFound.Error() {
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
		} else {
			return false, err
		}
	}
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
				var request models.AddClusterRequest
				request.Name = cluster.Name
				request.Nodes = new_nodes
				if err := ExpandClusterUsingInstaller(cluster_id, request, nodes, node_ips, t, ctxt); err != nil {
					utils.FailTask(
						fmt.Sprintf("Error expanding the cluster:%v", *cluster_id),
						fmt.Errorf("%s-%v", ctxt, err),
						t)
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

func ExpandClusterUsingSalt(cluster_uuid *uuid.UUID, request models.AddClusterRequest,
	nodes map[uuid.UUID]models.Node, node_ips map[uuid.UUID]map[string]string,
	t *task.Task, ctxt string) error {

	var mons []backend.Mon

	nodeRoleMapFromRequest := make(map[string][]string)

	for _, req_node := range request.Nodes {
		if util.StringInSlice(MON, req_node.NodeType) {
			var mon backend.Mon
			nodeid, _ := uuid.Parse(req_node.NodeId)
			mon.Node = nodes[*nodeid].Hostname
			mon.PublicIP4 = node_ips[*nodeid]["public"]
			mon.ClusterIP4 = node_ips[*nodeid]["cluster"]
			mons = append(mons, mon)
		}
		nodeRoleMapFromRequest[req_node.NodeId] = req_node.NodeType
	}

	var failedMons, succeededMons []string

	// Add other mons
	if len(mons) > 0 {
		t.UpdateStatus("Adding mons")
		for _, mon := range mons {
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
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll_snodes := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	for _, node := range nodes {
		if err := coll_snodes.Update(
			bson.M{"nodeid": node.NodeId},
			bson.M{"$set": bson.M{
				"clusterip4": node_ips[node.NodeId]["cluster"],
				"publicip4":  node_ips[node.NodeId]["public"],
				"roles":      nodeRoleMapFromRequest[node.NodeId.String()]}}); err != nil {
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
	ret_val, err := startAndPersistMons(*cluster_uuid, succeededMons, ctxt)
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
		logger.Get().Error(
			"%s-Error getting updated nodes list post create cluster %s. error: %v",
			ctxt,
			request.Name,
			err)
		return err
	}
	failedOSDs, succeededOSDs := addOSDs(
		*cluster_uuid,
		request.Name,
		updated_nodes,
		request.Nodes,
		t,
		ctxt)

	if len(failedOSDs) != 0 {
		var osds []string
		for _, osd := range failedOSDs {
			osds = append(osds, fmt.Sprintf("%s:%s", osd.Node, osd.Device))
		}
		t.UpdateStatus(fmt.Sprintf("OSD addition failed for %v", osds))
		if len(succeededOSDs) == 0 {
			t.UpdateStatus("Failed adding all OSDs")
			logger.Get().Error(
				"%s-Failed adding all OSDs while create cluster %s. error: %v",
				ctxt,
				request.Name,
				err)
		}
		if len(succeededOSDs) > 0 {
			t.UpdateStatus("Recalculating pgnum/pgpnum")
			if ok := RecalculatePgnum(ctxt, *cluster_uuid, t); !ok {
				logger.Get().Warning(
					"%s-Could not re-calculate pgnum/pgpnum for cluster: %v",
					ctxt,
					*cluster_uuid)
			}
			//Update the CRUSH MAP
			t.UpdateStatus("Updating the CRUSH Map")
			if err := UpdateCrushNodeItems(ctxt, *cluster_uuid); err != nil {
				logger.Get().Error("%s-Error updating the Crush map for cluster: %v. error: %v", ctxt, *cluster_uuid, err)
			}
		}
	}
	return nil
}

func ExpandClusterUsingInstaller(cluster_uuid *uuid.UUID, request models.AddClusterRequest,
	nodes map[uuid.UUID]models.Node, node_ips map[uuid.UUID]map[string]string,
	t *task.Task, ctxt string) error {

	var (
		clusterMons               []map[string]interface{}
		failedMons, succeededMons []string
		monReqNodes, osdReqNodes  []models.ClusterNode
	)

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	//Get the Mons of the cluster
	coll_nodes := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	var monNodes models.Nodes
	if err := coll_nodes.Find(bson.M{"clusterid": *cluster_uuid, "options.mon": "Y"}).All(&monNodes); err != nil {
		logger.Get().Error("%s-Error getting mons while expand cluster: %v. error: %v", ctxt, *cluster_uuid, err)
		return err
	}
	for _, mon := range monNodes {
		clusterMon := make(map[string]interface{})
		clusterMon["host"] = mon.Hostname
		clusterMon["address"] = mon.ClusterIP4
		clusterMons = append(clusterMons, clusterMon)
	}
	nodeRoleMapFromRequest := make(map[string][]string)
	for _, req_node := range request.Nodes {
		if util.StringInSlice(MON, req_node.NodeType) {
			monReqNodes = append(monReqNodes, req_node)
		} else if util.StringInSlice(OSD, req_node.NodeType) {
			osdReqNodes = append(osdReqNodes, req_node)
		}
		nodeRoleMapFromRequest[req_node.NodeId] = req_node.NodeType
	}

	if len(monReqNodes) > 0 {
		t.UpdateStatus("Configuring the mons")

		for _, req_node := range request.Nodes {
			if util.StringInSlice(MON, req_node.NodeType) {
				mon := make(map[string]interface{})
				nodeid, err := uuid.Parse(req_node.NodeId)
				if err != nil {
					logger.Get().Error("%s-Failed to parse uuid for node %v. error: %v", ctxt, req_node.NodeId, err)
					t.UpdateStatus(fmt.Sprintf("Failed to add MON node: %v", req_node.NodeId))
					continue
				}
				mon["calamari"] = false
				mon["host"] = nodes[*nodeid].Hostname
				mon["address"] = node_ips[*nodeid]["cluster"]
				mon["fsid"] = cluster_uuid.String()
				mon["monitor_secret"] = "AQA7P8dWAAAAABAAH/tbiZQn/40Z8pr959UmEA=="
				mon["cluster_name"] = request.Name
				mon["cluster_network"] = request.Networks.Cluster
				mon["public_network"] = request.Networks.Public
				mon["redhat_storage"] = conf.SystemConfig.Provisioners[bigfin_conf.ProviderName].RedhatStorage
				if len(clusterMons) > 0 {
					mon["monitors"] = clusterMons
				}

				if err := installer_backend.Configure(ctxt, t, MON, mon); err != nil {
					failedMons = append(failedMons, mon["host"].(string))
					logger.Get().Error("%s-Failed to add MON %v. error: %v", ctxt, mon["host"].(string), err)
				} else {
					t.UpdateStatus(fmt.Sprintf("Added mon node: %s", mon["host"].(string)))
					succeededMons = append(succeededMons, mon["host"].(string))

					clusterMon := make(map[string]interface{})
					clusterMon["host"] = nodes[*nodeid].Hostname
					clusterMon["address"] = node_ips[*nodeid]["cluster"]
					clusterMons = append(clusterMons, clusterMon)
				}

			}
		}

		if len(failedMons) > 0 {
			t.UpdateStatus(fmt.Sprintf("Failed to add mon(s) %v", failedMons))
		}

		t.UpdateStatus("Persisting mons")
		for _, mon := range succeededMons {
			if err := coll_nodes.Update(
				bson.M{"hostname": mon},
				bson.M{"$set": bson.M{
					"clusterid":        *cluster_uuid,
					"options.mon":      "Y",
					"options.calamari": "N"}}); err != nil {
				return err
			}
			logger.Get().Info(fmt.Sprintf("%s-Added mon node: %s", ctxt, mon))
		}
	}

	t.UpdateStatus("Updating node details for cluster")
	// Update nodes details
	for _, node := range nodes {
		if err := coll_nodes.Update(
			bson.M{"nodeid": node.NodeId},
			bson.M{"$set": bson.M{
				"clusterip4": node_ips[node.NodeId]["cluster"],
				"publicip4":  node_ips[node.NodeId]["public"],
				"roles":      nodeRoleMapFromRequest[node.NodeId.String()]}}); err != nil {
			logger.Get().Error(
				"%s-Error updating the details for node: %s. error: %v",
				ctxt,
				node.Hostname,
				err)
			t.UpdateStatus(fmt.Sprintf("Failed to update details of node: %s", node.Hostname))
		}
	}

	if len(osdReqNodes) > 0 {
		failedOSDs, succeededOSDs, err := configureOSDs(*cluster_uuid, request, clusterMons, t, true, ctxt)
		if err != nil {
			return err
		}
		if len(failedOSDs) != 0 {
			t.UpdateStatus(fmt.Sprintf("OSD addition failed for %v", failedOSDs))
		}
		if len(succeededOSDs) > 0 {
			t.UpdateStatus("Recalculating pgnum/pgpnum")
			if ok := RecalculatePgnum(ctxt, *cluster_uuid, t); !ok {
				logger.Get().Warning(
					"%s-Could not re-calculate pgnum/pgpnum for cluster: %v",
					ctxt,
					*cluster_uuid)
				t.UpdateStatus("Recalculating pgnum/pgpnum failed")
			}
			//Update the CRUSH MAP
			/*t.UpdateStatus("Updating the CRUSH Map")
			if err := UpdateCrushNodeItems(ctxt, *cluster_uuid); err != nil {
				logger.Get().Error("%s-Error updating the Crush map for cluster: %v. error: %v", ctxt, *cluster_uuid, err)
				t.UpdateStatus("Updating the CRUSH Map failed")
			}*/
		}
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
	monnode, err := GetCalamariMonNode(*cluster_id, ctxt)
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
		sessionCopy := db.GetDatastore().Copy()
		defer sessionCopy.Close()
		for {
			select {
			case <-t.StopCh:
				return
			default:
				t.UpdateStatus("Started updating osd params: %v", t.ID)
				ok, err := cephapi_backend.UpdateOSD(monnode.Hostname, *cluster_id, osdId, osdData, ctxt)
				if err != nil || !ok {
					utils.FailTask(fmt.Sprintf("Could not update osd params for slu: %s of cluster: %v", slu_id, cluster_id), fmt.Errorf("%s-%v", ctxt, err), t)
					return
				}
				//Now update the latest status from calamari
				fetchedOSD, err := cephapi_backend.GetOSD(monnode.Hostname, *cluster_id, osdId, ctxt)
				if err != nil {
					utils.FailTask(fmt.Sprintf("Error getting OSD details for cluster: %v.", cluster_id), fmt.Errorf("%s-%v", ctxt, err), t)
					return
				}
				status := mapOsdStatus(fetchedOSD.Up, fetchedOSD.In)
				state := mapOsdState(fetchedOSD.In)
				slu.Options["in"] = strconv.FormatBool(fetchedOSD.In)
				slu.Options["up"] = strconv.FormatBool(fetchedOSD.Up)
				slu.State = state
				slu.Status = status

				coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)

				if err := coll.Update(bson.M{"sluid": fetchedOSD.Uuid, "clusterid": cluster_id}, slu); err != nil {
					utils.FailTask(fmt.Sprintf("Error updating the details for slu: %s.", slu.Name), fmt.Errorf("%s-%v", ctxt, err), t)
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

func (s *CephProvider) GetClusterConfig(req models.RpcRequest, resp *models.RpcResponse) error {
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
			"%s-Error getting a mon from cluster: %v. error: %v",
			ctxt,
			*cluster_id,
			err)
		*resp = utils.WriteResponse(
			http.StatusInternalServerError,
			fmt.Sprintf(
				"Error getting a mon from cluster: %v",
				*cluster_id))
		return err
	}
	configs, err := cephapi_backend.GetClusterConfig(monnode.Hostname, *cluster_id, ctxt)
	if err != nil {
		logger.Get().Error(
			"%s-Error getting config details of cluster: %v. error: %v",
			ctxt,
			*cluster_id,
			err)
		*resp = utils.WriteResponse(
			http.StatusInternalServerError,
			fmt.Sprintf(
				"Error getting config details of cluster: %v",
				*cluster_id))
		return err
	}
	result, err := json.Marshal(configs)
	if err != nil {
		logger.Get().Error(
			"%s-Error forming the output for config details of cluster: %s. error: %v",
			ctxt,
			*cluster_id,
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

func cluster_status(clusterId uuid.UUID, clusterName string, ctxt string) (models.ClusterStatus, error) {
	// Pick a random mon from the list
	monnode, err := GetCalamariMonNode(clusterId, ctxt)
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
		logger.Get().Error("%s-Error getting storage pools for cluster: %s. error: %v", ctxt, clusterId, err)
		return false
	}

	t.UpdateStatus("Getting a mon from cluster")
	monnode, err := GetCalamariMonNode(clusterId, ctxt)
	if err != nil {
		logger.Get().Error("%s-Error getting a mon from cluster: %s. error: %v", ctxt, clusterId, err)
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
		currentPgNum, err := strconv.Atoi(storage.Options["pg_num"])
		if err != nil {
			logger.Get().Error("%s-Error getting pgnum of pool: %s for cluster: %v. error: %v", ctxt, storage.Name, clusterId, err)
			continue
		}
		if pgNum == uint(currentPgNum) {
			logger.Get().Info("No change in PgNum .. Continuing ..")
			continue
		}
		id, err := strconv.Atoi(storage.Options["id"])
		if err != nil {
			logger.Get().Error("%s-Error getting id of pool: %s for cluster: %v. error: %v", ctxt, storage.Name, clusterId, err)
			continue
		}
		// Update the PG Num for the cluster
		t.UpdateStatus(fmt.Sprintf("Updating the pgnum and pgpnum for pool %s", storage.Name))
		poolData := map[string]interface{}{
			"pg_num":  int(pgNum),
			"pgp_num": int(pgNum),
		}
		ok, err := cephapi_backend.UpdatePool(monnode.Hostname, clusterId, id, poolData, ctxt)
		if err != nil || !ok {
			logger.Get().Error("%s-Error updating pgnum/pgnum of pool: %s for cluster: %v. error: %v", ctxt, storage.Name, clusterId, err)
			t.UpdateStatus(fmt.Sprintf("Could not update pgnum/pgnum for pool: %s of cluster: %v", storage.Name, clusterId))
		}
	}
	return true
}

func syncOsdDetails(clusterId uuid.UUID, slus map[string]models.StorageLogicalUnit, ctxt string) error {
	// Get a random mon node
	monnode, err := GetCalamariMonNode(clusterId, ctxt)
	if err != nil {
		logger.Get().Error(
			"%s-Error getting a mon node in cluster: %s. error: %v",
			ctxt,
			clusterId,
			err)
		return err
	}

	osds, err := cephapi_backend.GetOSDs(monnode.Hostname, clusterId, ctxt)
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
	coll_slu := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
	coll_nodes := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)

	for _, osd := range osds {
		// Get the node details for SLU
		var node models.Node
		if err := coll_nodes.Find(
			bson.M{"hostname": bson.M{
				"$regex":   osd.Server,
				"$options": "$i"}}).One(&node); err != nil {

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

		if slu, ok := slus[fmt.Sprintf("%s:%s", node.NodeId.String(), deviceDetails.DevName)]; ok {
			status := mapOsdStatus(osd.Up, osd.In)
			state := mapOsdState(osd.In)
			slu.Options["in"] = strconv.FormatBool(osd.In)
			slu.Options["up"] = strconv.FormatBool(osd.Up)
			slu.Options["pgsummary"] = pgSummary.ByOSD[strconv.Itoa(osd.Id)]
			slu.State = state
			slu.Status = status
			slu.SluId = osd.Uuid
			slu.Name = fmt.Sprintf("osd.%d", osd.Id)
			slu.Options["journal.osdjournal"] = fetchedOSD.OsdJournal
			slu.Options["journal.reweight"] = fetchedOSD.Reweight

			if err := coll_slu.Update(
				bson.M{
					"nodeid":         node.NodeId,
					"clusterid":      clusterId,
					"options.device": deviceDetails.DevName}, slu); err != nil {
				logger.Get().Error("%s-Error updating the slu: %s. error: %v", ctxt, osd.Uuid.String(), err)
				continue
			}
			logger.Get().Info("%s-Updated the slu: osd.%d on cluster: %v", ctxt, osd.Id, clusterId)
		}
	}
	return nil
}

func SyncOsdStatus(clusterId uuid.UUID, ctxt string) error {

	// Get a random mon node
	monnode, err := GetCalamariMonNode(clusterId, ctxt)
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
		slu.Options["journal.osdjournal"] = fetchedOSD.OsdJournal
		slu.Options["journal.reweight"] = fetchedOSD.Reweight
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
		cNode := backend.CrushNodeRequest{BucketType: "root", Name: sprof.Name}
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
		cRule := backend.CrushRuleRequest{Name: sprof.Name, RuleSet: ruleSetId, Type: "replicated", MinSize: MINSIZE, MaxSize: MAXSIZE}
		step_take := make(map[string]interface{})
		step_take["item_name"] = cNode.Name
		step_take["item"] = cNodeId
		step_take["op"] = "take"
		cRule.Steps = append(cRule.Steps, step_take)
		leaf := make(map[string]interface{})
		leaf["num"] = 0
		leaf["type"] = "osd"
		leaf["op"] = "chooseleaf_firstn"
		cRule.Steps = append(cRule.Steps, leaf)
		emit := make(map[string]interface{})
		emit["op"] = "emit"
		cRule.Steps = append(cRule.Steps, emit)
		cRuleId, err := cephapi_backend.CreateCrushRule(mon, clusterId, cRule, ctxt)
		if err != nil {
			logger.Get().Error("Failed to create Crush rule for cluster: %s. error: %v", clusterId, err)
			continue
		}
		ruleInfo := bigfinmodels.CrushInfo{RuleSetId: cRuleId, CrushNodeId: cNodeId}
		ruleSets[sprof.Name] = ruleInfo
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

func UpdateCrushNodeItems(ctxt string, clusterId uuid.UUID) error {

	cluster, err := getCluster(clusterId)
	if err != nil {
		logger.Get().Error("Failed to get details of cluster: %s. error: %v", clusterId, err)
		return err
	}

	monnode, err := GetCalamariMonNode(clusterId, ctxt)
	if err != nil {
		logger.Get().Error("%s-Could not get random mon. Err:%v", ctxt, err)
		return err
	}
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
	scoll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_PROFILE)

	var sProfiles []models.StorageProfile
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
		var (
			pos   int
			items []backend.CrushItem
		)
		for _, slu := range slus {
			id, _ := strconv.Atoi(strings.Split(slu.Name, `.`)[1])
			item := backend.CrushItem{Id: id, Pos: pos}
			items = append(items, item)
			pos = pos + 1
		}

		rulesetmapval, ok := cluster.Options["rulesetmap"]
		if !ok {

			logger.Get().Error("Error getting the ruleset for cluster: %s", cluster.Name)
			return nil

		}
		rulesetmap := rulesetmapval.(map[string]interface{})
		rulesetval, ok := rulesetmap[sprof.Name]
		if !ok {
			logger.Get().Error("Error getting the ruleset for cluster: %s", cluster.Name)
			return nil
		}
		ruleset := rulesetval.(map[string]interface{})
		params := map[string]interface{}{
			"items": items,
		}
		_, err := cephapi_backend.PatchCrushNode(monnode.Hostname, clusterId, ruleset["crushnodeid"].(int), params, ctxt)
		if err != nil {
			logger.Get().Error("Failed to update Crush node for cluster: %s. error: %v", clusterId, err)
			continue
		}

	}
	return nil
}
