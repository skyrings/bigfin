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
	"github.com/skyrings/skyring-common/tools/logger"
	"github.com/skyrings/skyring-common/tools/task"
	"github.com/skyrings/skyring-common/tools/uuid"
	"gopkg.in/mgo.v2/bson"
	"net/http"

	bigfin_conf "github.com/skyrings/bigfin/conf"
	bigfin_task "github.com/skyrings/bigfin/tools/task"
)

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
		if err := PopulateClusterNodes(request.BootstrapNode, *cluster_uuid, ctxt); err != nil {
			utils.FailTask(
				fmt.Sprintf(
					"Failed populating node details for cluster: %s",
					clusterName),
				err,
				t)
			setClusterState(*cluster_uuid, models.CLUSTER_STATE_FAILED, ctxt)
			return
		}
		// Get and update storage pools
		if err := PopulateStoragePools(request.BootstrapNode, *cluster_uuid, ctxt); err != nil {
			utils.FailTask(
				fmt.Sprintf(
					"Failed populating storage pools details for cluster: %s",
					clusterName),
				err,
				t)
			setClusterState(*cluster_uuid, models.CLUSTER_STATE_FAILED, ctxt)
			return
		}
		// Get and update OSDs
		if err := PopulateClusterOSDs(request.BootstrapNode, *cluster_uuid, ctxt); err != nil {
			utils.FailTask(
				fmt.Sprintf(
					"Failed populating OSD details for cluster: %s",
					clusterName),
				err,
				t)
			setClusterState(*cluster_uuid, models.CLUSTER_STATE_FAILED, ctxt)
			return
		}
		// Get and update block devices
		if err := PopulateBlockDevices(request.BootstrapNode, *cluster_uuid, ctxt); err != nil {
			utils.FailTask(
				fmt.Sprintf(
					"Failed populating block devices details for cluster: %s",
					clusterName),
				err,
				t)
			setClusterState(*cluster_uuid, models.CLUSTER_STATE_FAILED, ctxt)
			return
		}
		setClusterState(*cluster_uuid, models.CLUSTER_STATE_ACTIVE, ctxt)
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

func PopulateClusterNodes(bootstrapNode string, clusterId uuid.UUID, ctxt string) error {
	if err := syncStorageNodes(bootstrapNode, clusterId, ctxt); err != nil {
		return fmt.Errorf(
			"%s-Error fetching and populating storages node for cluster: %v. error: %v",
			ctxt,
			clusterId,
			err)
	}
	return nil
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
	if err := syncOsds(clusterId, ctxt); err != nil {
		return fmt.Errorf(
			"%s-Error fetching and populating OSDs for cluster: %v. error: %v",
			ctxt,
			clusterId,
			err)
	}
	return nil
}

func PopulateBlockDevices(bootstrapNode string, clusterId uuid.UUID, ctxt string) error {
	if err := syncRBDs(clusterId, ctxt); err != nil {
		return fmt.Errorf(
			"%s-Error fetching and populating RBDs for cluster: %v. error: %v",
			ctxt,
			clusterId,
			err)
	}
	return nil
}
