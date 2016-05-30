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
	"github.com/skyrings/skyring-common/models"
	"github.com/skyrings/skyring-common/tools/logger"
	"github.com/skyrings/skyring-common/tools/uuid"
	"github.com/skyrings/skyring-common/utils"
	"net/http"
)

func (s *CephProvider) GetDiskHierarchy(req models.RpcRequest, resp *models.RpcResponse) error {
	var request models.DiskHierarchyRequest
	ctxt := req.RpcRequestContext

	if err := json.Unmarshal(req.RpcRequestData, &request); err != nil {
		logger.Get().Error(fmt.Sprintf("%s-Unbale to parse the disk hierarchy request. error: %v", ctxt, err))
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request. error: %v", err))
		return err
	}

	if request.JournalSize == "" {
		request.JournalSize = fmt.Sprintf("%dMB", JOURNALSIZE)
	}
	nodes, err := util.GetNodes(request.ClusterNodes)
	if err != nil {
		logger.Get().Error(
			"%s-Error getting nodes list from DB for cluster %s. error: %v",
			ctxt,
			request.ClusterName,
			err)
		return err
	}
	hierarchy := make(map[string]map[string]string)
	var storageSize uint64
	for _, requestNode := range request.ClusterNodes {
		disksMap := make(map[string]string)
		uuid, err := uuid.Parse(requestNode.NodeId)
		if err != nil {
			logger.Get().Error(
				"%s-Error parsing node id: %s for cluster: %s. error: %v",
				ctxt,
				requestNode.NodeId,
				request.ClusterName,
				err)
			continue
		}

		devices := make(map[string]models.Disk)
		storageNode := nodes[*uuid]
		// Form a map of storage node disks
		var nodeDisksMap map[string]models.Disk = make(map[string]models.Disk)
		for _, storageDisk := range storageNode.StorageDisks {
			nodeDisksMap[storageDisk.Name] = storageDisk
		}

		for diskName, storageDisk := range nodeDisksMap {
			for idx := 0; idx < len(requestNode.Devices); idx++ {
				if diskName == requestNode.Devices[idx].Name {
					devices[requestNode.Devices[idx].Name] = storageDisk
				}
			}
		}

		diskWithJournalMapped := getDiskWithJournalMapped(devices, request.JournalSize)
		for disk, journal := range diskWithJournalMapped {
			disksMap[disk] = journal.JournalDisk
			for _, storageDisk := range storageNode.StorageDisks {
				if storageDisk.Name == disk {
					storageSize += storageDisk.Size
				}
			}
		}
		hierarchy[requestNode.NodeId] = disksMap
	}

	retVal := models.DiskHierarchyDetails{
		ClusterName: request.ClusterName,
		Hierarchy:   hierarchy,
		StorageSize: storageSize,
	}
	result, err := json.Marshal(retVal)
	if err != nil {
		logger.Get().Error(
			"%s-Error forming the output for get disk hierarchy of cluster: %s. error: %v",
			ctxt,
			request.ClusterName,
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
