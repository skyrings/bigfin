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
	"github.com/skyrings/bigfin/tools/logger"
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/skyring/conf"
	"github.com/skyrings/skyring/db"
	"github.com/skyrings/skyring/event"
	"github.com/skyrings/skyring/models"
	"gopkg.in/mgo.v2/bson"
	"net/http"
	"path/filepath"
	"strings"
)

var (
	handlermap = map[string]interface{}{
		"skyring/calamari/ceph/calamari/started":        calamari_server_start_handler,
		"skyring/calamari/ceph/server/added":            ceph_server_add_handler,
		"skyring/calamari/ceph/server/reboot":           ceph_server_reboot_handler,
		"skyring/calamari/ceph/server/package/changed":  ceph_server_package_change_handler,
		"skyring/calamari/ceph/server/lateReporting":    ceph_server_late_reporting_handler,
		"skyring/calamari/ceph/server/regainedContact":  ceph_server_contact_regained_handler,
		"skyring/calamari/ceph/cluster/lateReporting":   ceph_cluster_late_reporting_handler,
		"skyring/calamari/ceph/cluster/regainedContact": ceph_cluster_contact_regained_handler,
		"skyring/calamari/ceph/osd/propertyChanged":     ceph_osd_property_changed_handler,
		"skyring/calamari/ceph/mon/propertyChanged":     ceph_mon_property_changed_handler,
		"skyring/calamari/ceph/cluster/health/changed":  ceph_cluster_health_changed,
	}
	cluster_status_in_enum = map[string]int{
		"HEALTH_OK":   models.CLUSTER_STATUS_OK,
		"HEALTH_WARN": models.CLUSTER_STATUS_WARN,
		"HEALTH_ERR":  models.CLUSTER_STATUS_ERROR,
	}
)

func calamari_server_start_handler(event models.Event) error {
	return nil
}

func ceph_server_add_handler(event models.Event) error {
	return nil
}

func ceph_server_reboot_handler(event models.Event) error {
	return nil
}

func ceph_server_package_change_handler(event models.Event) error {
	return nil
}

func ceph_server_late_reporting_handler(event models.Event) error {
	return nil
}

func ceph_server_contact_regained_handler(event models.Event) error {
	return nil
}

func ceph_cluster_late_reporting_handler(event models.Event) error {
	return nil
}

func ceph_cluster_contact_regained_handler(event models.Event) error {
	return nil
}

func ceph_osd_property_changed_handler(event models.Event) error {
	return nil
}

func ceph_mon_property_changed_handler(event models.Event) error {
	return nil
}

func update_cluster_status(clusterStatus int, event models.Event) error {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Update(bson.M{"clusterid": event.ClusterId}, bson.M{"$set": bson.M{"status": clusterStatus}}); err != nil {
		logger.Get().Error("Error updating the cluster status: %v", err)
		return err
	}
	return nil
}

func ceph_cluster_health_changed(event models.Event) error {
	status := strings.SplitAfter(event.Message, " ")[len(strings.SplitAfter(event.Message, " "))-1]
	if err := update_cluster_status(cluster_status_in_enum[status], event); err != nil {
		return err
	}
	return nil
}

func (s *CephProvider) ProcessEvent(req models.RpcRequest, resp *models.RpcResponse) error {
	var e models.Event

	if err := json.Unmarshal(req.RpcRequestData, &e); err != nil {
		logger.Get().Error("Unbale to parse the request %v", err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request %v", err))
		return err
	}

	for tag, handler := range handlermap {
		if match, err := filepath.Match(tag, e.Tag); err == nil {
			if match {
				if err := handler.(func(models.Event) error)(e); err != nil {
					*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Event Handling Failed for event: %s", err))
					logger.Get().Error("Event Handling Failed for event: %v", err)
					return err
				}
				if err := event.Persist_event(e); err != nil {
					*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Could not persist the event to DB: %s", err))
					logger.Get().Error("Could not persist the event to DB: %v", err)
					return err
				} else {
					*resp = utils.WriteResponse(http.StatusOK, "")
					return nil
				}
			}
		} else {
			*resp = utils.WriteResponse(http.StatusInternalServerError, fmt.Sprintf("Error while mapping handler: %s", err))
			logger.Get().Error("Error while maping handler: %v", err)
			return err
		}
	}
	logger.Get().Warning("Handler not defined for event %s", e.Tag)
	*resp = utils.WriteResponse(http.StatusNotImplemented, fmt.Sprintf("Handler not defined for event %s", e.Tag))
	return nil
}
