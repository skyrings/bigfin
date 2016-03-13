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
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/skyring-common/conf"
	"github.com/skyrings/skyring-common/db"
	"github.com/skyrings/skyring-common/event"
	"github.com/skyrings/skyring-common/models"
	"github.com/skyrings/skyring-common/tools/logger"
	"github.com/skyrings/skyring-common/tools/uuid"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"net/http"
	"path/filepath"
	"strings"
	"time"
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

func calamari_server_start_handler(event models.Event, ctxt string) error {
	return nil
}

func ceph_server_add_handler(event models.Event, ctxt string) error {
	return nil
}

func ceph_server_reboot_handler(event models.Event, ctxt string) error {
	return nil
}

func ceph_server_package_change_handler(event models.Event, ctxt string) error {
	return nil
}

func ceph_server_late_reporting_handler(event models.Event, ctxt string) error {
	return nil
}

func ceph_server_contact_regained_handler(event models.Event, ctxt string) error {
	return nil
}

func ceph_cluster_late_reporting_handler(event models.Event, ctxt string) error {
	return nil
}

func ceph_cluster_contact_regained_handler(event models.Event, ctxt string) error {
	return nil
}

func ceph_osd_property_changed_handler(event models.Event, ctxt string) error {
	if strings.HasPrefix(event.Message, "OSD") && strings.HasSuffix(event.Message, "added to the cluster map") {
		sessionCopy := db.GetDatastore().Copy()
		defer sessionCopy.Close()
		coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
		osdname := fmt.Sprintf("osd.%s", event.Tags["service_id"])
		osduuid, err := uuid.Parse(event.Tags["osd_uuid"])
		if err != nil {
			logger.Get().Error("%s-Error parsing the uuid for slu: %s. error: %v", ctxt, osdname, err)
			return errors.New(fmt.Sprintf("Error parsing the uuid for slu: %s. error: %v", osdname, err))
		}
		clusteruuid, err := uuid.Parse(event.Tags["fsid"])
		if err != nil {
			logger.Get().Error("%s-Error parsing the cluster uuid for slu: %s. error: %v", ctxt, osdname, err)
			return errors.New(fmt.Sprintf("Error parsing the cluster uuid for slu: %s. error: %v", osdname, err))
		}
		var updated bool
		for count := 0; count < 12; count++ {
			if err := coll.Update(bson.M{"name": osdname, "clusterid": *clusteruuid}, bson.M{"$set": bson.M{"sluid": *osduuid}}); err != nil {
				if err.Error() == mgo.ErrNotFound.Error() {
					time.Sleep(5 * time.Second)
					continue
				}
				logger.Get().Error("%s-Error updating the uuid for slu: %s. error: %v", ctxt, osdname, err)
				return errors.New(fmt.Sprintf("Error updating the uuid for slu: %s. error: %v", osdname, err))
			} else {
				updated = true
				break
			}
		}
		if !updated {
			logger.Get().Error("%s-Sluid update failed for: %s", ctxt, osdname)
		} else {
			logger.Get().Info("%s-Updated sluid for: %s", ctxt, osdname)
		}
	}
	return nil
}

func ceph_mon_property_changed_handler(event models.Event, ctxt string) error {
	return nil
}

func update_cluster_status(clusterStatus int, event models.Event, ctxt string) error {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Update(bson.M{"clusterid": event.ClusterId}, bson.M{"$set": bson.M{"status": clusterStatus}}); err != nil {
		logger.Get().Error("%s-Error updating the status of cluster: %v. error: %v", ctxt, event.ClusterId, err)
		return err
	}
	return nil
}

func ceph_cluster_health_changed(event models.Event, ctxt string) error {
	cluster, err := getCluster(event.ClusterId)
	if err != nil {
		logger.Get().Error("%s-Error getting the  cluster: %v. error: %v", ctxt, event.ClusterId, err)
		return err
	}
	if cluster.State == models.CLUSTER_STATE_ACTIVE {
		status := strings.SplitAfter(event.Message, " ")[len(strings.SplitAfter(event.Message, " "))-1]
		if err := update_cluster_status(cluster_status_in_enum[status], event, ctxt); err != nil {
			return err
		}
	}
	return nil
}

func HandleEvent(e models.Event, ctxt string) (err error, statusCode int) {
	for tag, handler := range handlermap {
		if match, err := filepath.Match(tag, e.Tag); err == nil {
			if match {
				if err = handler.(func(models.Event, string) error)(e, ctxt); err != nil {
					return fmt.Errorf("Event Handling Failed for event: %s. error: %v", e.Tag, err), http.StatusInternalServerError
				}
				if err = event.Persist_event(e, ctxt); err != nil {
					return fmt.Errorf("Could not persist the event: %s to DB. error: %v", e.Tag, err), http.StatusInternalServerError
				} else {
					return nil, http.StatusOK
				}
			}
		} else {
			return fmt.Errorf("Error while maping handler for event: %s. error: %v", e.Tag, err), http.StatusInternalServerError
		}
	}
	return fmt.Errorf("Handler not defined for event %s", e.Tag), http.StatusNotImplemented
}

func (s *CephProvider) ProcessEvent(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext
	var e models.Event

	if err := json.Unmarshal(req.RpcRequestData, &e); err != nil {
		logger.Get().Error("%s-Unbale to parse the request. error: %v", ctxt, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request. error: %v", err))
		return err
	}

	err, statusCode := HandleEvent(e, ctxt)
	var err_str string
	if err != nil {
		err_str = err.Error()
		if statusCode == http.StatusNotImplemented {
			logger.Get().Warning("%v", err)
		} else {
			logger.Get().Error("%v", err)
		}
	}

	*resp = utils.WriteResponse(statusCode, err_str)
	return err
}
