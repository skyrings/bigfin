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
	"github.com/skyrings/bigfin/backend/cephapi/handler"
	cephapi_models "github.com/skyrings/bigfin/backend/cephapi/models"
	"github.com/skyrings/bigfin/bigfinmodels"
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/skyring-common/conf"
	"github.com/skyrings/skyring-common/db"
	common_event "github.com/skyrings/skyring-common/event"
	"github.com/skyrings/skyring-common/models"
	"github.com/skyrings/skyring-common/tools/logger"
	"github.com/skyrings/skyring-common/tools/schedule"
	"github.com/skyrings/skyring-common/tools/uuid"
	"github.com/skyrings/skyring-common/utils"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var (
	handlermap = map[string]interface{}{
		"calamari/ceph/pool/added":                                       ceph_pool_add_handler,
		"calamari/ceph/pool/deleted":                                     ceph_pool_delete_handler,
		"calamari/ceph/osd/propertyChanged":                              ceph_osd_property_changed_handler,
		"calamari/ceph/mon/propertyChanged":                              ceph_mon_property_changed_handler,
		"calamari/ceph/cluster/health/changed":                           ceph_cluster_health_changed,
		"calamari/ceph/rbd/created":                                      ceph_rbd_create_handler,
		"calamari/ceph/rbd/deleted":                                      ceph_rbd_delete_handler,
		"calamari/ceph/rbd/resized":                                      ceph_rbd_resize_handler,
		"skyring/ceph/cluster/*/threshold/slu_utilization/*":             ceph_osd_utilization_threshold_changed,
		"skyring/ceph/cluster/*/threshold/block_device_utilization/*":    ceph_rbd_utilization_threshold_changed,
		"skyring/ceph/cluster/*/threshold/cluster_utilization/*":         ceph_cluster_utilization_threshold_changed,
		"skyring/ceph/cluster/*/threshold/storage_utilization/*":         ceph_storage_utilization_threshold_changed,
		"skyring/ceph/cluster/*/threshold/storage_profile_utilization/*": ceph_storage_profile_utilization_threshold_changed,
		"Node connectivity changed":                                      node_down_handler,
	}
	cluster_status_in_enum = map[string]int{
		"HEALTH_OK":   models.CLUSTER_STATUS_OK,
		"HEALTH_WARN": models.CLUSTER_STATUS_WARN,
		"HEALTH_ERR":  models.CLUSTER_STATUS_ERROR,
	}
)

func parseThresholdEvent(event models.Event, ctxt string) (models.AppEvent, error) {
	var appEvent models.AppEvent
	eventId, err := uuid.New()
	if err != nil {
		logger.Get().Error("%s- Uuid generation for event failed. Error: %v", ctxt, err)
		return appEvent, err
	}

	currentValue, currentValueErr := util.GetReadableFloat(event.Tags["CurrentValue"], ctxt)
	if currentValueErr != nil {
		logger.Get().Error("%s-Could not parse the current value:%s", ctxt, currentValueErr)
		return appEvent, currentValueErr
	}

	thresholdValue, thresholdValueErr := util.GetReadableFloat(event.Tags["ThresholdValue"], ctxt)
	if thresholdValueErr != nil {
		logger.Get().Error("%s-Could not parse the threshold value:%s", ctxt, thresholdValueErr)
		return appEvent, thresholdValueErr
	}

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	var cluster models.Cluster
	if err := coll.Find(bson.M{"clusterid": event.ClusterId}).One(&cluster); err == nil {
		appEvent.ClusterName = cluster.Name
	}

	appEvent.Tags = map[string]string{
		"Current Utilization": currentValue,
		"Threshold value":     thresholdValue,
		"Entity Name":         event.Tags["EntityName"],
	}

	switch event.Tags["ThresholdType"] {
	case models.OK:
		appEvent.Severity = models.ALARM_STATUS_CLEARED
	case models.WARNING:
		appEvent.Severity = models.ALARM_STATUS_WARNING
	case models.CRITICAL:
		appEvent.Severity = models.ALARM_STATUS_CRITICAL
	default:
		logger.Get().Error("%s-Error unknown ThresholdType for event: %v", event)
		appEvent.Severity = models.ALARM_STATUS_INDETERMINATE
	}

	appEvent.Name = EventTypes[event.Tags["Plugin"]]
	appEvent.EventId = *eventId

	entityId, err := uuid.Parse(event.Tags["EntityId"])
	if err != nil {
		logger.Get().Error("%s-Could not parse the entity UUID. Error: %v", ctxt, err)
		return appEvent, err
	}
	appEvent.EntityId = *entityId

	appEvent.Timestamp = event.Timestamp
	appEvent.ClusterId = event.ClusterId
	appEvent.Message = event.Message
	appEvent.Notify = true
	return appEvent, nil
}

func ceph_osd_utilization_threshold_changed(event models.Event, ctxt string) (models.AppEvent, error) {
	appEvent, err := parseThresholdEvent(event, ctxt)
	if err != nil {
		logger.Get().Error("%s- Could not parse the threshold cross event. Error:%v", ctxt, err)
		return appEvent, err
	}
	appEvent.NotificationEntity = models.NOTIFICATION_ENTITY_SLU

	var slu models.StorageLogicalUnit
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
	if err := coll.Find(bson.M{"clusterid": appEvent.ClusterId,
		"sluid": appEvent.EntityId}).One(&slu); err != nil {
		return appEvent, fmt.Errorf("%s-Error getting the slu:%v for"+
			" cluster: %v. error: %v", ctxt, appEvent.EntityId, appEvent.ClusterId, err)
	}
	appEvent.NodeId = slu.NodeId

	var node models.Node
	coll = sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	if err = coll.Find(bson.M{"nodeid": appEvent.NodeId}).One(&node); err != nil {
		return appEvent, fmt.Errorf("%s-Error getting the node: %v from DB."+
			" error: %v", ctxt, appEvent.NodeId, err)
	}

	appEvent.Message = fmt.Sprintf("OSD utilization for %s on %s cluster has moved to %s",
		event.Tags["EntityName"],
		appEvent.ClusterName,
		event.Tags["ThresholdType"])

	if val, ok := event.Tags["Notify"]; ok {
		if val, err := strconv.ParseBool(val); err != nil {
			logger.Get().Error("%s-Error parsing the value: %s", ctxt, event.Tags["Notify"])
			return appEvent, err
		} else {
			appEvent.Notify = val
		}
	}

	appEvent.NodeName = node.Hostname

	clearedSeverity, err := common_event.ClearCorrespondingAlert(appEvent, ctxt)
	if err != nil && appEvent.Severity == models.ALARM_STATUS_CLEARED {
		logger.Get().Warning("%s-could not clear corresponding"+
			" alert for: %s. Error: %v", ctxt, appEvent.EventId.String(), err)
		return appEvent, err
	}

	if err = common_event.UpdateNodeAlarmCount(
		appEvent,
		clearedSeverity,
		ctxt); err != nil {
		logger.Get().Error("%s-Could not update Alarm state and"+
			" count for event:%v .Error: %v", ctxt, appEvent.EventId, err)
		return appEvent, err
	}

	if err = common_event.UpdateSluAlarmCount(
		appEvent,
		clearedSeverity,
		ctxt); err != nil {
		logger.Get().Error("%s-Could not update Alarm state and"+
			" count for event:%v .Error: %v", ctxt, appEvent.EventId, err)
		return appEvent, err
	}

	if err = common_event.UpdateClusterAlarmCount(
		appEvent,
		clearedSeverity,
		ctxt); err != nil {
		logger.Get().Error("%s-Could not update Alarm state and count for"+
			" event:%v .Error: %v", ctxt, appEvent.EventId, err)
		return appEvent, err
	}

	return appEvent, nil
}

func ceph_rbd_utilization_threshold_changed(event models.Event, ctxt string) (models.AppEvent, error) {
	appEvent, err := parseThresholdEvent(event, ctxt)
	if err != nil {
		logger.Get().Error("%s- Could not parse the threshold cross event. Error:%v", ctxt, err)
		return appEvent, err
	}
	appEvent.NotificationEntity = models.NOTIFICATION_ENTITY_BLOCK_DEVICE

	var BlkDev models.BlockDevice
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_BLOCK_DEVICES)
	if err := coll.Find(bson.M{"clusterid": appEvent.ClusterId,
		"id": appEvent.EntityId}).One(&BlkDev); err != nil {
		return appEvent, fmt.Errorf("%s-Error getting the RBD:%v for"+
			" cluster: %v. error: %v", ctxt, appEvent.EntityId, appEvent.ClusterId, err)
	}

	appEvent.Message = fmt.Sprintf("RBD utilization for %s on %s cluster has moved to %s",
		event.Tags["EntityName"],
		appEvent.ClusterName,
		event.Tags["ThresholdType"])

	clearedSeverity, err := common_event.ClearCorrespondingAlert(appEvent, ctxt)
	if err != nil && appEvent.Severity == models.ALARM_STATUS_CLEARED {
		logger.Get().Warning("%s-could not clear corresponding"+
			" alert for: %s. Error: %v", ctxt, appEvent.EventId.String(), err)
		return appEvent, err
	}

	if err = common_event.UpdateBlockDeviceAlarmCount(
		appEvent,
		clearedSeverity,
		ctxt); err != nil {
		logger.Get().Error("%s-Could not update Alarm state and count for"+
			" event:%v .Error: %v", ctxt, appEvent.EventId, err)
		return appEvent, err
	}

	if err = common_event.UpdateClusterAlarmCount(
		appEvent,
		clearedSeverity,
		ctxt); err != nil {
		logger.Get().Error("%s-Could not update Alarm state and count for"+
			" event:%v .Error: %v", ctxt, appEvent.EventId, err)
		return appEvent, err
	}

	return appEvent, nil
}

func ceph_cluster_utilization_threshold_changed(event models.Event, ctxt string) (models.AppEvent, error) {
	appEvent, err := parseThresholdEvent(event, ctxt)
	if err != nil {
		logger.Get().Error("%s- Could not parse the threshold cross event. Error:%v", ctxt, err)
		return appEvent, err
	}

	appEvent.Message = fmt.Sprintf("Cluster utilization for %s has moved to %s",
		appEvent.ClusterName,
		event.Tags["ThresholdType"])

	appEvent.NotificationEntity = models.NOTIFICATION_ENTITY_CLUSTER

	clearedSeverity, err := common_event.ClearCorrespondingAlert(appEvent, ctxt)
	if err != nil && appEvent.Severity == models.ALARM_STATUS_CLEARED {
		logger.Get().Warning("%s-could not clear corresponding"+
			" alert for: %s. Error: %v", ctxt, appEvent.EventId.String(), err)
		return appEvent, err
	}

	if err = common_event.UpdateClusterAlarmCount(
		appEvent,
		clearedSeverity,
		ctxt); err != nil {
		logger.Get().Error("%s-Could not update Alarm state and count for"+
			" event:%v .Error: %v", ctxt, appEvent.EventId, err)
		return appEvent, err
	}

	return appEvent, nil
}

func ceph_storage_profile_utilization_threshold_changed(event models.Event, ctxt string) (models.AppEvent, error) {
	appEvent, err := parseThresholdEvent(event, ctxt)
	if err != nil {
		logger.Get().Error("%s- Could not parse the threshold cross event. Error:%v", ctxt, err)
		return appEvent, err
	}

	appEvent.NotificationEntity = models.NOTIFICATION_ENTITY_STORAGE_PROFILE

	appEvent.Message = fmt.Sprintf("Storage Profile utilization for profile %s"+
		" on %s cluster has moved to %s",
		event.Tags["EntityName"],
		appEvent.ClusterName,
		event.Tags["ThresholdType"])

	var affectedOSDs string
	osdEvents := event.ImpactingEvents["storage_logical_units"]
	for _, e := range osdEvents {
		currentValue, currentValueErr := util.GetReadableFloat(e.Tags["CurrentValue"], ctxt)
		if currentValueErr != nil {
			logger.Get().Error("%s-Could not parse the current value:%s",
				ctxt, currentValueErr)
			return appEvent, currentValueErr
		}
		if affectedOSDs == "" {
			affectedOSDs += fmt.Sprintf("%s(%s%%)", e.Tags["EntityName"], currentValue)
		} else {
			affectedOSDs += fmt.Sprintf(", %s(%s%%)", e.Tags["EntityName"], currentValue)
		}
	}
	affectedOSDs += fmt.Sprintf(".")

	appEvent.Tags["Affected OSDs"] = affectedOSDs

	clearedSeverity, err := common_event.ClearCorrespondingAlert(appEvent, ctxt)
	if err != nil && appEvent.Severity == models.ALARM_STATUS_CLEARED {
		logger.Get().Warning("%s-could not clear corresponding"+
			" alert for: %s. Error: %v", ctxt, appEvent.EventId.String(), err)
		return appEvent, err
	}

	if err = common_event.UpdateClusterAlarmCount(
		appEvent,
		clearedSeverity,
		ctxt); err != nil {
		logger.Get().Error("%s-Could not update Alarm state and count for"+
			" event:%v .Error: %v", ctxt, appEvent.EventId, err)
		return appEvent, err
	}

	return appEvent, nil
}

func ceph_storage_utilization_threshold_changed(event models.Event, ctxt string) (models.AppEvent, error) {
	appEvent, err := parseThresholdEvent(event, ctxt)
	if err != nil {
		logger.Get().Error("%s- Could not parse the threshold cross event. Error:%v", ctxt, err)
		return appEvent, err
	}

	appEvent.Message = fmt.Sprintf("Pool utilization for pool %s on %s cluster has moved to %s",
		event.Tags["EntityName"],
		appEvent.ClusterName,
		event.Tags["ThresholdType"])

	appEvent.NotificationEntity = models.NOTIFICATION_ENTITY_STORAGE

	clearedSeverity, err := common_event.ClearCorrespondingAlert(appEvent, ctxt)
	if err != nil && appEvent.Severity == models.ALARM_STATUS_CLEARED {
		logger.Get().Warning("%s-could not clear corresponding"+
			" alert for: %s. Error: %v", ctxt, appEvent.EventId.String(), err)
		return appEvent, err
	}

	if err = common_event.UpdateStorageAlarmCount(
		appEvent,
		clearedSeverity,
		ctxt); err != nil {
		logger.Get().Error("%s-Could not update Alarm state and count for"+
			" event:%v .Error: %v", ctxt, appEvent.EventId, err)
		return appEvent, err
	}

	if err = common_event.UpdateClusterAlarmCount(
		appEvent,
		clearedSeverity,
		ctxt); err != nil {
		logger.Get().Error("%s-Could not update Alarm state and count for"+
			" event:%v .Error: %v", ctxt, appEvent.EventId, err)
		return appEvent, err
	}

	return appEvent, nil
}

func osd_add_or_delete_handler(event models.AppEvent, osdname string, ctxt string) (models.AppEvent, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)

	event.Name = EventTypes["osd_added_or_removed"]
	event.Severity = models.ALARM_STATUS_CLEARED
	event.Notify = false

	if strings.HasSuffix(event.Message, bigfinmodels.OSD_ADD_MESSAGE) {
		// Added this sleep to avoid race condition. wherein both event
		// handler and provider try to update the osd details to DB
		// which may result in duplicate osd records in DB.
		time.Sleep(60 * time.Second)
		// this is needed because while cluster is created before even
		// the Cluster is added in DB this event might come.
		var retrievedMonSuccessfully bool
		var monHostname string
		for count := 0; count < 12; count++ {
			monnode, err := GetCalamariMonNode(event.ClusterId, ctxt)
			if err != nil {
				if err.Error() == mgo.ErrNotFound.Error() {
					time.Sleep(5 * time.Second)
					continue
				}
				logger.Get().Error("%s-Error getting a mon node in cluster: %s. error: %v", ctxt, event.ClusterId, err)
				return event, err
			} else {
				monHostname = monnode.Hostname
				retrievedMonSuccessfully = true
				break
			}
		}
		if !retrievedMonSuccessfully {
			logger.Get().Error("%s-Error getting a mon node in cluster: %s.", ctxt, event.ClusterId)
			return event, fmt.Errorf("Could not retrieve the mon node for this cluster")
		}
		osd, err := cephapi_backend.GetOSD(monHostname, event.ClusterId, event.Tags["service_id"], ctxt)
		if err != nil {
			logger.Get().Critical("%s-Error Getting osd:%v", ctxt, err)
			return event, err
		}

		node_coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
		var node models.Node
		if err := node_coll.Find(bson.M{"hostname": osd.Server}).One(&node); err != nil {
			logger.Get().Error("%s-Error fetching node details for SLU id: %d on cluster: %v. error: %v", ctxt, osd.Id, event.ClusterId, err)
			return event, err
		}

		event.NodeId = node.NodeId
		event.NodeName = node.Hostname
		/*deviceDetails, err := salt_backend.GetPartDeviceDetails(
			node.Hostname,
			osd.OsdData,
			ctxt)
		if err != nil {
			logger.Get().Error(
				"%s-Error getting device details of osd.%d. error: %v",
				ctxt,
				osd.Id,
				err)
		}

		newSlu := models.StorageLogicalUnit{
			SluId:     osd.Uuid,
			Name:      fmt.Sprintf("osd.%d", osd.Id),
			Type:      models.CEPH_OSD,
			ClusterId: event.ClusterId,
			NodeId:    event.NodeId,
			Status:    mapOsdStatus(osd.Up, osd.In),
			State:     mapOsdState(osd.In),
		}
		newSlu.StorageDeviceId = deviceDetails.Uuid
		newSlu.StorageDeviceSize = deviceDetails.Size
		newSlu.StorageProfile = get_disk_profile(node.StorageDisks, deviceDetails.PartName)
		var options = make(map[string]interface{})
		options["in"] = strconv.FormatBool(osd.In)
		options["up"] = strconv.FormatBool(osd.Up)
		options["node"] = node.Hostname
		options["publicip4"] = node.PublicIP4
		options["clusterip4"] = node.ClusterIP4
		options["device"] = deviceDetails.DevName
		options["fstype"] = deviceDetails.FSType
		newSlu.Options = options

		if err := coll.Update(bson.M{"nodeid": node.NodeId, "clusterid": event.ClusterId, "options.device": newSlu.Options["device"]}, newSlu); err != nil {
			if err.Error() == mgo.ErrNotFound.Error() {
				if err := coll.Insert(newSlu); err != nil {
					logger.Get().Error("%s-Error creating the new SLU for cluster: %v. error: %v", ctxt, event.ClusterId, err)
					return event, err
				}
				logger.Get().Info("%s-Added new slu: osd.%d on cluster: %v", ctxt, osd.Id, event.ClusterId)
				return event, nil
			} else {
				logger.Get().Error("%s-Error updating the SLU: %s", ctxt, osdname)
				return event, err
			}
		} else {
			logger.Get().Info("%s-Successfully updated the SLU: %s", ctxt, osdname)
			return event, nil
		}*/
		return event, nil
	} else {
		if err := coll.Remove(bson.M{"sluid": event.EntityId}); err != nil {
			logger.Get().Warning(
				"%s-Error removing the slu: %s for cluster: %v. error: %v",
				ctxt,
				event.EntityId,
				event.ClusterId,
				err)
			return event, err
		} else {
			if err := common_event.DismissAllEventsForEntity(event.EntityId, event, ctxt); err != nil {
				logger.Get().Error("%s-Error while dismissing events for entity: %v. Error: %v", ctxt, event.EntityId, err)
			}
			return event, nil
		}
	}
}

func osd_state_change_handler(event models.AppEvent, osdname string, ctxt string) (models.AppEvent, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)

	event.Name = EventTypes["osd_state_changed"]
	event.Notify = true

	//update the state of osd in DB
	monnode, err := GetCalamariMonNode(event.ClusterId, ctxt)
	if err != nil {
		logger.Get().Error("%s-Error getting a mon node in cluster: %s. error: %v", ctxt, event.ClusterId, err)
		return event, err
	}

	osd, err := cephapi_backend.GetOSD(monnode.Hostname, event.ClusterId, event.Tags["service_id"], ctxt)
	if err != nil {
		logger.Get().Critical("%s-Error Getting osd:%v", ctxt, err)
		return event, err
	}
	node_coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	var node models.Node
	if err := node_coll.Find(bson.M{"hostname": osd.Server}).One(&node); err != nil {
		logger.Get().Error("%s-Error fetching node details for SLU id: %d on cluster: %v. error: %v", ctxt, osd.Id, node.ClusterId, err)
		return event, err
	}

	event.NodeId = node.NodeId
	event.NodeName = node.Hostname
	status := mapOsdStatus(osd.Up, osd.In)
	state := mapOsdState(osd.In)

	if err := coll.Update(bson.M{"sluid": osd.Uuid, "clusterid": event.ClusterId}, bson.M{"$set": bson.M{"status": status, "state": state}}); err != nil {
		logger.Get().Error("%s-Error updating the status for slu: %s. error: %v", ctxt, osd.Uuid.String(), err)
		return event, err
	}
	logger.Get().Info("%s-Updated the status of slu: osd.%d on cluster: %v", ctxt, osd.Id, event.ClusterId)
	if strings.Contains(event.Message, bigfinmodels.OSD_DOWN_MESSAGE) {
		util.AppendServiceToNode(bson.M{"nodeid": event.NodeId}, fmt.Sprintf("%s-%s", bigfinmodels.NODE_SERVICE_OSD, osdname), models.STATUS_DOWN, ctxt)
		event.Severity = models.ALARM_STATUS_WARNING
		event.Message = fmt.Sprintf("OSD %s on %s cluster went down", osdname, event.ClusterName)
	} else {
		util.AppendServiceToNode(bson.M{"nodeid": event.NodeId}, fmt.Sprintf("%s-%s", bigfinmodels.NODE_SERVICE_OSD, osdname), models.STATUS_UP, ctxt)
		event.Severity = models.ALARM_STATUS_CLEARED
		event.Message = fmt.Sprintf("OSD %s on %s cluster came up", osdname, event.ClusterName)
	}

	clearedSeverity, err := common_event.ClearCorrespondingAlert(event, ctxt)
	if err != nil && event.Severity == models.ALARM_STATUS_CLEARED {
		logger.Get().Warning("%s-could not clear corresponding"+
			" alert for: %s. Error: %v", ctxt, event.EventId.String(), err)
		return event, err
	}

	if err = common_event.UpdateNodeAlarmCount(
		event,
		clearedSeverity,
		ctxt); err != nil {
		logger.Get().Error("%s-Could not update Alarm state and"+
			" count for event:%v .Error: %v", ctxt, event.EventId, err)
		return event, err
	}

	if err = common_event.UpdateSluAlarmCount(
		event,
		clearedSeverity,
		ctxt); err != nil {
		logger.Get().Error("%s-Could not update Alarm state and"+
			" count for event:%v .Error: %v", ctxt, event.EventId, err)
		return event, err
	}

	if err = common_event.UpdateClusterAlarmCount(
		event,
		clearedSeverity,
		ctxt); err != nil {
		logger.Get().Error("%s-Could not update Alarm state and count for"+
			" event:%v .Error: %v", ctxt, event.EventId, err)
		return event, err
	}

	return event, nil
}

func ceph_osd_property_changed_handler(event models.AppEvent, ctxt string) (models.AppEvent, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	osdname := fmt.Sprintf("osd.%s", event.Tags["service_id"])
	osduuid, err := uuid.Parse(event.Tags["osd_uuid"])
	if err != nil {
		logger.Get().Error("%s-Error parsing the uuid for slu: %s. error: %v", ctxt, osdname, err)
		return event, errors.New(fmt.Sprintf("Error parsing the uuid for slu: %s. error: %v", osdname, err))
	}
	event.EntityId = *osduuid
	event.NotificationEntity = models.NOTIFICATION_ENTITY_SLU

	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	var cluster models.Cluster
	if err := coll.Find(bson.M{"clusterid": event.ClusterId}).One(&cluster); err == nil {
		event.ClusterName = cluster.Name
	}

	if strings.HasSuffix(event.Message, bigfinmodels.OSD_ADD_MESSAGE) || strings.HasSuffix(event.Message, bigfinmodels.OSD_REMOVE_MESSAGE) {
		event, err = osd_add_or_delete_handler(event, osdname, ctxt)
		if err != nil {
			return event, err
		}
	} else if strings.Contains(event.Message, bigfinmodels.OSD_DOWN_MESSAGE) || strings.Contains(event.Message, bigfinmodels.OSD_UP_MESSAGE) {

		event, err = osd_state_change_handler(event, osdname, ctxt)
		if err != nil {
			return event, err
		}
	}

	return event, nil
}

func ceph_mon_property_changed_handler(event models.AppEvent, ctxt string) (models.AppEvent, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	node_coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	var node models.Node
	if err := node_coll.Find(bson.M{"hostname": event.Tags["fqdn"]}).One(&node); err != nil {
		logger.Get().Error("%s-Error fetching node details for mon : %s on cluster: %v. error: %v", ctxt, event.Tags["fqdn"], event.ClusterId, err)
		return event, err
	}

	event.NodeId = node.NodeId
	event.NodeName = node.Hostname
	event.EntityId = event.NodeId
	event.NotificationEntity = models.NOTIFICATION_ENTITY_HOST
	event.Name = EventTypes["mon_state_changed"]
	event.Notify = true
	if strings.Contains(event.Message, "joined quorum") {
		event.Severity = models.ALARM_STATUS_CLEARED
		util.AppendServiceToNode(bson.M{"nodeid": event.NodeId}, bigfinmodels.NODE_SERVICE_MON, models.STATUS_UP, ctxt)
	} else {
		util.AppendServiceToNode(bson.M{"nodeid": event.NodeId}, bigfinmodels.NODE_SERVICE_MON, models.STATUS_DOWN, ctxt)
		event.Severity = models.ALARM_STATUS_WARNING
	}

	clearedSeverity, err := common_event.ClearCorrespondingAlert(event, ctxt)
	if err != nil && event.Severity == models.ALARM_STATUS_CLEARED {
		logger.Get().Warning("%s-could not clear corresponding"+
			" alert for: %s. Error: %v", ctxt, event.EventId.String(), err)
		return event, err
	}

	if err = common_event.UpdateClusterAlarmCount(
		event,
		clearedSeverity,
		ctxt); err != nil {
		logger.Get().Error("%s-Could not update Alarm state and count for"+
			" event:%v .Error: %v", ctxt, event.EventId, err)
		return event, err
	}

	if err = common_event.UpdateNodeAlarmCount(
		event,
		clearedSeverity,
		ctxt); err != nil {
		logger.Get().Error("%s-Could not update Alarm state and"+
			" count for event:%v .Error: %v", ctxt, event.EventId, err)
		return event, err
	}

	return event, nil
}

func update_cluster_status(clusterStatus int, event models.AppEvent, ctxt string) error {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Update(bson.M{"clusterid": event.ClusterId}, bson.M{"$set": bson.M{"status": clusterStatus}}); err != nil {
		logger.Get().Error("%s-Error updating the status of cluster: %v. error: %v", ctxt, event.ClusterId, err)
		return err
	}
	return nil
}

func ceph_cluster_health_changed(event models.AppEvent, ctxt string) (models.AppEvent, error) {
	cluster, err := getCluster(event.ClusterId)
	event.EntityId = event.ClusterId
	event.NotificationEntity = models.NOTIFICATION_ENTITY_CLUSTER
	event.NodeName = ""
	event.Name = EventTypes["cluster_health_changed"]
	status := strings.SplitAfter(event.Message, " ")[len(strings.SplitAfter(event.Message, " "))-1]
	cluster_status, ok := cluster_status_in_enum[status]
	if !ok {
		event.Severity = models.ALARM_STATUS_INDETERMINATE
	} else {
		switch cluster_status {
		case models.CLUSTER_STATUS_ERROR:
			event.Severity = models.ALARM_STATUS_CRITICAL
		case models.CLUSTER_STATUS_WARN:
			event.Severity = models.ALARM_STATUS_WARNING
		case models.CLUSTER_STATUS_OK:
			event.Severity = models.ALARM_STATUS_CLEARED
		default:
			event.Severity = models.ALARM_STATUS_INDETERMINATE
		}
	}
	event.Notify = true

	if err != nil {
		logger.Get().Error("%s-Error getting the  cluster: %v. error: %v", ctxt, event.ClusterId, err)
		return event, err
	}
	if cluster.State == models.CLUSTER_STATE_ACTIVE {
		status := strings.SplitAfter(event.Message, " ")[len(strings.SplitAfter(event.Message, " "))-1]
		if err := update_cluster_status(cluster_status_in_enum[status], event, ctxt); err != nil {
			return event, err
		}
	}

	clearedSeverity, err := common_event.ClearCorrespondingAlert(event, ctxt)
	if err != nil && event.Severity == models.ALARM_STATUS_CLEARED {
		logger.Get().Warning("%s-could not clear corresponding"+
			" alert for: %s. Error: %v", ctxt, event.EventId.String(), err)
		return event, err
	}

	if err = common_event.UpdateClusterAlarmCount(
		event,
		clearedSeverity,
		ctxt); err != nil {
		logger.Get().Error("%s-Could not update Alarm state and count for"+
			" event:%v .Error: %v", ctxt, event.EventId, err)
		return event, err
	}
	return event, nil
}

func ceph_pool_add_handler(event models.AppEvent, ctxt string) (models.AppEvent, error) {
	// if pool is created through skyring let the details be updated by
	// creator itself. This is mainly to handle pool creation from backend
	// So this sleep will make sure that the pool details are updated before
	// this function tries to update.
	time.Sleep(60 * time.Second)
	event.NotificationEntity = models.NOTIFICATION_ENTITY_STORAGE
	event.Name = EventTypes["pool_added_or_removed"]
	event.NodeName = ""
	event.Severity = models.ALARM_STATUS_CLEARED
	event.Notify = false
	monnode, err := GetCalamariMonNode(event.ClusterId, ctxt)
	if err != nil {
		logger.Get().Error(
			"%s-Error getting a mon node in cluster: %v. error: %v",
			ctxt,
			event.ClusterId, err)
		return event, err
	}
	pool_id, err := strconv.Atoi(event.Tags["service_id"])
	if err != nil {
		logger.Get().Error("%s-Error converting pool-id to integer", ctxt)
		return event, err
	}

	pool, err := cephapi_backend.GetPool(monnode.Hostname,
		event.ClusterId,
		pool_id,
		ctxt)
	if err != nil {
		return event, err
	}
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll1 := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	var cluster models.Cluster
	if err := coll1.Find(bson.M{"clusterid": event.ClusterId}).One(&cluster); err != nil {
		return event, fmt.Errorf(
			"%s-Error getting details of cluster: %v. error: %v",
			ctxt,
			event.ClusterId,
			err)
	}
	event.ClusterName = cluster.Name

	storage := models.Storage{
		Name:     pool.Name,
		Replicas: pool.Size,
	}
	event.Message = fmt.Sprintf("Detected addition of new pool %s in cluster %s", pool.Name,
		cluster.Name)
	event.Description = fmt.Sprintf("%s. The new pool will be added to skyring.", event.Message)
	if pool.QuotaMaxObjects != 0 && pool.QuotaMaxBytes != 0 {
		storage.QuotaEnabled = true
		quotaParams := make(map[string]string)
		quotaParams["quota_max_objects"] = strconv.Itoa(pool.QuotaMaxObjects)
		quotaParams["quota_max_bytes"] = strconv.FormatUint(pool.QuotaMaxBytes, 10)
		storage.QuotaParams = quotaParams
	}
	options := make(map[string]string)
	options["id"] = strconv.Itoa(pool.Id)
	options["pg_num"] = strconv.Itoa(pool.PgNum)
	options["pgp_num"] = strconv.Itoa(pool.PgpNum)
	options["full"] = strconv.FormatBool(pool.Full)
	options["hashpspool"] = strconv.FormatBool(pool.HashPsPool)
	options["min_size"] = strconv.FormatUint(pool.MinSize, 10)
	options["crash_replay_interval"] = strconv.Itoa(pool.CrashReplayInterval)
	options["crush_ruleset"] = strconv.Itoa(pool.CrushRuleSet)
	// Get EC profile details of pool
	ok, out, err := cephapi_backend.ExecCmd(
		monnode.Hostname,
		event.ClusterId,
		fmt.Sprintf(
			"ceph --cluster %s osd pool get %s erasure_code_profile --format=json",
			cluster.Name,
			pool.Name),
		ctxt)
	if err != nil || !ok {
		storage.Type = models.STORAGE_TYPE_REPLICATED
		logger.Get().Warning(
			"%s-Error getting EC profile details of pool: %s of cluster: %s",
			ctxt,
			pool.Name,
			cluster.Name)
	} else {
		var ecprofileDet bigfinmodels.ECProfileDet
		if err := json.Unmarshal([]byte(out), &ecprofileDet); err != nil {
			logger.Get().Warning(
				"%s-Error parsing EC profile details of pool: %s of cluster: %s",
				ctxt,
				pool.Name,
				cluster.Name)
		} else {
			storage.Type = models.STORAGE_TYPE_ERASURE_CODED
			options["ecprofile"] = ecprofileDet.ECProfile
		}
	}
	storage.Options = options
	storage.ClusterId = event.ClusterId

	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)

	var existing_storage models.Storage

	if err := coll.Find(bson.M{"name": storage.Name, "clusterid": storage.ClusterId}).One(&existing_storage); err != nil {
		if err.Error() == mgo.ErrNotFound.Error() {
			uuid, err := uuid.New()
			if err != nil {
				logger.Get().Error(
					"%s-Error creating id for the new storage entity: %s. error: %v",
					ctxt,
					storage.Name,
					err)
				return event, nil
			}
			storage.StorageId = *uuid
			if err := coll.Insert(storage); err != nil {
				logger.Get().Error(
					"%s-Error adding storage:%s to DB on cluster: %s. error: %v",
					ctxt,
					storage.Name,
					cluster.Name,
					err)
				return event, nil
			}
			logger.Get().Info(
				"%s-Added the new storage entity: %s on cluster: %s",
				ctxt,
				storage.Name,
				cluster.Name)
			event.EntityId = storage.StorageId
			return event, nil
		} else {
			logger.Get().Error("%s-Error reading storage info from DB. Error: %v", ctxt, err)
			return event, err
		}
	} else {
		return event, fmt.Errorf("pool already added in DB")
	}
}

func ceph_rbd_delete_handler(event models.AppEvent, ctxt string) (models.AppEvent, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	event.NotificationEntity = models.NOTIFICATION_ENTITY_BLOCK_DEVICE
	event.Name = EventTypes["rbd_added_or_removed"]

	var cluster models.Cluster
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Find(bson.M{"clusterid": event.ClusterId}).One(&cluster); err != nil {
		logger.Get().Error("%s- Unable to get cluster: %v detail from DB. Error: %v", ctxt, event.ClusterId, err)
		return event, err
	}

	var pool models.Storage
	poolColl := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
	if err := poolColl.Find(bson.M{"clusterid": event.ClusterId, "name": event.Tags["poolName"]}).One(&pool); err != nil {
		logger.Get().Error(
			"%s-Error getting the pool: %s for cluster: %v. error: %v",
			ctxt,
			event.Tags["poolName"],
			event.ClusterId,
			err)
		return event, err
	}

	event.ClusterName = cluster.Name
	rbdColl := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_BLOCK_DEVICES)
	var rbd models.BlockDevice
	if err := rbdColl.Find(
		bson.M{
			"clusterid": event.ClusterId,
			"storageid": pool.StorageId,
			"name":      event.Tags["rbdName"]}).One(&rbd); err != nil {
		logger.Get().Error(
			"%s-Error finding the block device: %s "+
				"of pool: %s on cluster: %s. error: %v",
			ctxt,
			event.Tags["rbdName"],
			pool.Name,
			cluster.Name,
			err)
		return event, err
	}
	event.EntityId = rbd.Id
	if err := common_event.DismissAllEventsForEntity(event.EntityId, event, ctxt); err != nil {
		logger.Get().Error("%s-Error while dismissing events for entity: %v. Error: %v", ctxt, event.EntityId, err)
	}
	if err := rbdColl.Remove(
		bson.M{
			"clusterid": event.ClusterId,
			"storageid": pool.StorageId,
			"name":      event.Tags["rbdName"]}); err != nil {
		logger.Get().Error(
			"%s-Error removing the block device: %s "+
				"of pool: %s on cluster: %s. error: %v",
			ctxt,
			event.Tags["rbdName"],
			pool.Name,
			cluster.Name,
			err)
		return event, err
	}
	event.Message = fmt.Sprintf("Detected removal of RBD %s on %s pool.",
		event.Tags["rbdName"],
		event.Tags["poolName"],
	)
	event.Severity = models.ALARM_STATUS_CLEARED
	event.Notify = false
	logger.Get().Info(
		"%s-Removed block device: %s of pool: %s "+
			"on cluster: %s from the DB",
		ctxt,
		event.Tags["rbdName"],
		pool.Name,
		cluster.Name)
	return event, nil
}

func ceph_rbd_create_handler(event models.AppEvent, ctxt string) (models.AppEvent, error) {
	// if rbd is created through skyring let the details be updated by
	// creator itself. This is mainly to handle rbd creation from backend
	// So this sleep will make sure that the rbd details are updated before
	// this function tries to update.
	time.Sleep(60 * time.Second)
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	event.NotificationEntity = models.NOTIFICATION_ENTITY_BLOCK_DEVICE
	event.Name = EventTypes["rbd_added_or_removed"]

	cluster, pool, blkDeviceDet, err := get_rbd_detail(event, ctxt)
	if err != nil {
		logger.Get().Error("%s-Could not get the details required for rbd addition. Err: %v", ctxt, err)
		return event, err
	}
	event.ClusterName = cluster.Name

	id, err := uuid.New()
	if err != nil {
		logger.Get().Error(
			"%s-Error creating id for block device. error: %v",
			ctxt,
			err)
		return event, err
	}
	newDevice := models.BlockDevice{
		Id:          *id,
		Name:        blkDeviceDet.Name,
		ClusterId:   cluster.ClusterId,
		ClusterName: cluster.Name,
		StorageId:   pool.StorageId,
		StorageName: pool.Name,
		Size:        fmt.Sprintf("%dMB", blkDeviceDet.Size/1048576),
	}

	var existing_blkDevice models.BlockDevice
	rbdColl := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_BLOCK_DEVICES)
	if err := rbdColl.Find(bson.M{
		"clusterid": event.ClusterId,
		"storageid": pool.StorageId,
		"name":      event.Tags["rbdName"]}).One(&existing_blkDevice); err != nil {
		if err.Error() == mgo.ErrNotFound.Error() {
			if err := rbdColl.Insert(newDevice); err != nil {
				logger.Get().Error(
					"%s-Error adding the block device: %s of pool: %s "+
						"on cluster: %s. error: %v",
					ctxt,
					blkDeviceDet.Name,
					pool.Name,
					cluster.Name,
					err)
				return event, err
			} else {
				logger.Get().Info(
					"%s-Added new block device: %s of pool: %s on cluster: %s",
					ctxt,
					blkDeviceDet.Name,
					pool.Name,
					cluster.Name)
				event.Message = fmt.Sprintf("Detected Addition of RBD %s on %s pool.",
					event.Tags["rbdName"],
					event.Tags["poolName"],
				)
				event.Severity = models.ALARM_STATUS_CLEARED
				event.Notify = false
				event.EntityId = newDevice.Id
				return event, nil
			}
		} else {
			logger.Get().Error("%s- Error while reading RBD details from DB. Error: %v", err)
			return event, err
		}
	}
	return event, fmt.Errorf("%s- RBD %s already present in DB. So this event will be ignored",
		ctxt,
		blkDeviceDet.Name)
}

func get_rbd_detail(event models.AppEvent, ctxt string) (models.Cluster,
	models.Storage,
	backend.BlockDevice,
	error) {
	var pool models.Storage
	var cluster models.Cluster
	var blkDeviceDet backend.BlockDevice

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Find(bson.M{"clusterid": event.ClusterId}).One(&cluster); err != nil {
		logger.Get().Error("%s- Unable to get cluster: %v detail from DB. Error: %v", ctxt, event.ClusterId, err)
		return cluster, pool, blkDeviceDet, err
	}

	poolColl := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
	if err := poolColl.Find(bson.M{"clusterid": event.ClusterId, "name": event.Tags["poolName"]}).One(&pool); err != nil {
		logger.Get().Error(
			"%s-Error getting the pool: %s for cluster: %v. error: %v",
			ctxt,
			event.Tags["poolName"],
			event.ClusterId,
			err)
		return cluster, pool, blkDeviceDet, err
	}

	cmd := fmt.Sprintf(
		"rbd --cluster %s --image %s -p %s info --format=json",
		cluster.Name,
		event.Tags["rbdName"],
		event.Tags["poolName"],
	)

	ok, out, err := cephapi_backend.ExecCmd(
		event.NodeName,
		cluster.ClusterId,
		cmd,
		ctxt)
	if !ok || err != nil {
		logger.Get().Error(
			"%s-Error getting information of block device: %s "+
				"of pool: %s on cluster: %s. error: %v",
			ctxt,
			blkDeviceDet.Name,
			pool.Name,
			cluster.Name,
			err)
		return cluster, pool, blkDeviceDet, err
	} else {
		if err := json.Unmarshal([]byte(out), &blkDeviceDet); err != nil {
			logger.Get().Error(
				"%s-Error parsing block device details for %s "+
					"of pool: %s on cluster: %s. error: %v",
				ctxt,
				blkDeviceDet.Name,
				pool.Name,
				cluster.Name,
				err)
			return cluster, pool, blkDeviceDet, err
		}
	}
	return cluster, pool, blkDeviceDet, nil
}

func ceph_rbd_resize_handler(event models.AppEvent, ctxt string) (models.AppEvent, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	event.NotificationEntity = models.NOTIFICATION_ENTITY_BLOCK_DEVICE
	event.Name = EventTypes["rbd_resized"]

	cluster, pool, blkDeviceDet, err := get_rbd_detail(event, ctxt)
	if err != nil {
		logger.Get().Error("%s-Could not get the details required for rbd resize updation. Err: %v", ctxt, err)
		return event, err
	}
	event.ClusterName = cluster.Name
	rbdColl := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_BLOCK_DEVICES)
	if err := rbdColl.Update(
		bson.M{
			"clusterid": event.ClusterId,
			"storageid": pool.StorageId,
			"name":      event.Tags["rbdName"]},
		bson.M{
			"$set": bson.M{"size": fmt.Sprintf(
				"%dMB",
				blkDeviceDet.Size/1048576)}}); err != nil {
		logger.Get().Error(
			"%s-Error updating the details of block device: %s "+
				"of pool: %s on cluster: %s. error: %v",
			ctxt,
			blkDeviceDet.Name,
			pool.Name,
			cluster.Name,
			err)
		return event, err
	}
	event.Message = fmt.Sprintf("RBD %s on %s pool has been resized to %s",
		event.Tags["rbdName"],
		event.Tags["poolName"],
		fmt.Sprintf("%dMB", blkDeviceDet.Size/1048576),
	)
	event.Severity = models.ALARM_STATUS_CLEARED
	event.Notify = false
	logger.Get().Info(
		"%s-Updated the details of block device: %s of pool: %s "+
			"on cluster: %s",
		ctxt,
		blkDeviceDet.Name,
		pool.Name,
		cluster.Name)
	return event, nil
}

func ceph_pool_delete_handler(event models.AppEvent, ctxt string) (models.AppEvent, error) {
	// if pool is deleted through skyring let the details be updated by
	// skyring itself else the task fails. This is mainly to handle pool
	// deletion from backend  So this sleep will make sure that the pool
	// details are updated before it comes here, if done through skyring.
	time.Sleep(60 * time.Second)
	event.NotificationEntity = models.NOTIFICATION_ENTITY_STORAGE
	event.Name = EventTypes["pool_added_or_removed"]
	event.NodeName = ""
	event.Severity = models.ALARM_STATUS_CLEARED
	event.Notify = false
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	coll1 := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	var cluster models.Cluster
	if err := coll1.Find(bson.M{"clusterid": event.ClusterId}).One(&cluster); err != nil {
		return event, fmt.Errorf(
			"%s-Error getting details of cluster: %v. error: %v",
			ctxt,
			event.ClusterId,
			err)
	}
	event.ClusterName = cluster.Name

	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
	var s models.Storage
	if err := coll.Find(bson.M{"options.id": event.Tags["service_id"],
		"clusterid": event.ClusterId}).One(&s); err != nil {
		logger.Get().Error(
			"%s-Error finding the storage: %s(id) from cluster: %v. error: %v",
			ctxt,
			event.Tags["service_id"],
			event.ClusterId,
			err)
		return event, err
	}
	event.Message = fmt.Sprintf("Detected deletion of pool %s from cluster %s", s.Name,
		cluster.Name)
	event.Description = fmt.Sprintf("%s. This pool will be removed from the DB", event.Message)
	event.EntityId = s.StorageId
	if err := coll.Remove(bson.M{"options.id": event.Tags["service_id"],
		"clusterid": event.ClusterId}); err != nil {
		logger.Get().Error(
			"%s-Error removing the storage from cluster: %v. error: %v",
			ctxt,
			event.ClusterId,
			err)
		return event, err
	}
	if err := common_event.DismissAllEventsForEntity(s.StorageId, event, ctxt); err != nil {
		logger.Get().Error("%s-Error while dismissing events for entity: %v. Error: %v", ctxt, event.EntityId, err)
	}
	return event, nil
}

func HandleEvent(e models.Event, ctxt string) (err error, statusCode int) {
	for tag, handler := range handlermap {
		if match, err := filepath.Match(tag, e.Tag); err == nil {
			if match {
				appEvent, err := handler.(func(models.Event, string) (models.AppEvent, error))(e, ctxt)
				if err != nil {
					return fmt.Errorf("Event Handling Failed for event: %s. error: %v",
						e.Tag, err), http.StatusInternalServerError
				}
				if err = common_event.AuditLog(ctxt, appEvent, GetDbProvider()); err != nil {
					return fmt.Errorf("Could not persist the event: %s to DB. error: %v",
						e.Tag, err), http.StatusInternalServerError
				} else {
					return nil, http.StatusOK
				}
			}
		} else {
			return fmt.Errorf("Error while maping handler for event: %s. error: %v",
				e.Tag, err), http.StatusInternalServerError
		}
	}
	return fmt.Errorf("Handler not defined for event %s", e.Tag), http.StatusNotImplemented
}

func node_down_handler(event models.AppEvent, ctxt string) (models.AppEvent, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	var calamariMonNode models.Node
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	err := coll.Find(
		bson.M{
			"clusterid": event.ClusterId,
			"nodeid":    event.NodeId}).One(&calamariMonNode)
	if err == nil {
		if val, ok := calamariMonNode.Options["calamari"]; val != models.Yes || !ok {
			return event, fmt.Errorf("This node does not have calamari server running. Hence node down event is being ignored")
		}

		// Check availability of calamari
		dummyUrl := fmt.Sprintf(
			"https://%s:%d/%s/v%d/",
			calamariMonNode.Hostname,
			cephapi_models.CEPH_API_PORT,
			cephapi_models.CEPH_API_DEFAULT_PREFIX,
			cephapi_models.CEPH_API_DEFAULT_VERSION)
		resp, err := handler.HttpGet(calamariMonNode.Hostname, dummyUrl)
		if resp != nil {
			resp.Body.Close()
		}
		if err == nil {
			return event, fmt.Errorf("Calamari is accessible from this node, so this event is ignored.")
		}
	} else if err != mgo.ErrNotFound {
		return event, err
	}

	var monNodes models.Nodes
	if err := coll.Find(
		bson.M{
			"clusterid":        event.ClusterId,
			"options.mon":      models.Yes,
			"options.calamari": "N"}).All(&monNodes); err != nil {
		return event, err
	}

	for _, monNode := range monNodes {
		if err := salt_backend.StartCalamari(monNode.Hostname, ctxt); err != nil {
			logger.Get().Warning(
				"%s-Could not start calamari on mon: %s. error: %v",
				ctxt,
				monNode.Hostname,
				err)
			continue
		}
		// Wait for calamari to start serving
		time.Sleep(60 * time.Second)
		if err := coll.Update(
			bson.M{"clusterid": event.ClusterId, "hostname": monNode.Hostname},
			bson.M{"$set": bson.M{
				"options.calamari": "Y"}}); err != nil {
			logger.Get().Warning(
				"%s-Failed to update mon node: %s status as calamari",
				ctxt,
				monNode.Hostname)
			// Stop calamari on the node and try on other one
			if err := salt_backend.StopCalamari(monNode.Hostname, ctxt); err != nil {
				logger.Get().Warning(
					"%s-Could not stop calamari on mon node: %s. error: %v",
					ctxt,
					monNode.Hostname,
					err)
			}
			continue
		}
		// Update that the old calamari server is no longer valid calamari server
		if err := coll.Update(
			bson.M{"clusterid": event.ClusterId, "hostname": calamariMonNode.Hostname},
			bson.M{"$set": bson.M{"options.calamari": "N"}}); err != nil {
			return event, fmt.Errorf("Error disabling invalid calamari node: %s. error: %v", calamariMonNode.Hostname, err)
		}

		// populating values for a new event which says about calamari server change
		var calamariChangeEvent models.AppEvent
		eventId, err := uuid.New()
		if err != nil {
			logger.Get().Error("%s-Uuid generation for the event failed: %s. error: %v", ctxt, err)
			return event, err
		}
		calamariChangeEvent.EventId = *eventId
		calamariChangeEvent.ClusterId = event.ClusterId
		calamariChangeEvent.EntityId = monNode.NodeId
		calamariChangeEvent.NodeId = monNode.NodeId
		calamariChangeEvent.NodeName = monNode.Hostname
		calamariChangeEvent.NotificationEntity = models.NOTIFICATION_ENTITY_HOST
		calamariChangeEvent.Timestamp = time.Now()
		calamariChangeEvent.Name = EventTypes["calamari_server_changed"]
		calamariChangeEvent.Message = fmt.Sprintf("Calamari Server has been moved to %s", monNode.Hostname)
		calamariChangeEvent.Description = fmt.Sprintf("%s. Because system has detected that old server: %s is down", calamariChangeEvent.Message, event.NodeName)
		calamariChangeEvent.Severity = models.ALARM_STATUS_CLEARED
		return calamariChangeEvent, nil
	}

	return event, fmt.Errorf("No valid active calamari mon node found")
}

func HandleCalamariEvent(e models.AppEvent, ctxt string) (err error, statusCode int) {
	for tag, handler := range handlermap {
		if match, err := filepath.Match(tag, e.Name); err != nil {
			return fmt.Errorf("Error while maping handler for event: %s. error: %v",
				e.Name, err), http.StatusInternalServerError
		} else if match {
			appEvent, err := handler.(func(models.AppEvent, string) (models.AppEvent, error))(e, ctxt)
			if err != nil {
				return fmt.Errorf("Event Handling Failed for event: %s. error: %v",
					e.Name, err), http.StatusInternalServerError
			}
			if err = common_event.AuditLog(ctxt, appEvent, GetDbProvider()); err != nil {
				return fmt.Errorf("Could not persist the event: %s to DB. error: %v",
					e.Name, err), http.StatusInternalServerError
			} else {
				return nil, http.StatusOK
			}
		}
	}
	return fmt.Errorf("Handler not defined for event %s", e.Name), http.StatusNotImplemented
}

func (s *CephProvider) ProcessEvent(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext
	var e models.AppEvent
	if err := json.Unmarshal(req.RpcRequestData, &e); err != nil {
		logger.Get().Error("%s-Unbale to parse the request. error: %v", ctxt, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request. error: %v", err))
		return err
	}

	err, statusCode := HandleCalamariEvent(e, ctxt)
	var err_str string
	if err != nil {
		err_str = err.Error()
		if statusCode == http.StatusNotImplemented {
			logger.Get().Warning("%s - %v", ctxt, err)
		} else {
			logger.Get().Error("%s - %v", ctxt, err)
		}
	}

	*resp = utils.WriteResponse(statusCode, err_str)
	return err
}

func EmitRbdEvents(params map[string]interface{}) {
	reqId, err := uuid.New()
	if err != nil {
		logger.Get().Error("Error creating the RequestId. error: %v", err)
		return
	}
	ctxt := fmt.Sprintf("%v:%v", models.ENGINE_NAME, reqId.String())
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	var clusters []models.Cluster
	if err := coll.Find(nil).All(&clusters); err != nil {
		logger.Get().Error(
			"%s-Error getting details of cluster from DB. error: %v",
			ctxt,
			err)
	}
	for _, cluster := range clusters {
		monnode, err := GetCalamariMonNode(cluster.ClusterId, ctxt)
		if err != nil {
			logger.Get().Error("%s-Error getting a mon node in cluster: %s. error: %v",
				ctxt,
				cluster.Name,
				err)
			continue
		}
		err = salt_backend.EmitRbdEvents(monnode.Hostname, cluster.Name, ctxt)
		if err != nil {
			logger.Get().Error("%s-Error while triggering rbd events for cluster: %s. Error: %v",
				ctxt,
				cluster.Name,
				err)
			continue
		}
	}
}

func ScheduleRbdEventEmitter() error {
	scheduler, err := schedule.NewScheduler()
	if err != nil {
		logger.Get().Error("Error: %v", err.Error())
		return err
	} else {
		f := EmitRbdEvents
		m := make(map[string]interface{})
		go scheduler.Schedule(time.Duration(5*time.Minute), f, m)
	}
	return nil
}
