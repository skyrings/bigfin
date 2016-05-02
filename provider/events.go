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
	"github.com/skyrings/bigfin/bigfinmodels"
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/skyring-common/conf"
	"github.com/skyrings/skyring-common/db"
	common_event "github.com/skyrings/skyring-common/event"
	"github.com/skyrings/skyring-common/models"
	"github.com/skyrings/skyring-common/tools/logger"
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
		"calamari/ceph/osd/propertyChanged":                              ceph_osd_property_changed_handler,
		"calamari/ceph/mon/propertyChanged":                              ceph_mon_property_changed_handler,
		"calamari/ceph/cluster/health/changed":                           ceph_cluster_health_changed,
		"skyring/ceph/cluster/*/threshold/slu_utilization/*":             ceph_osd_utilization_threshold_changed,
		"skyring/ceph/cluster/*/threshold/cluster_utilization/*":         ceph_cluster_utilization_threshold_changed,
		"skyring/ceph/cluster/*/threshold/storage_utilization/*":         ceph_storage_utilization_threshold_changed,
		"skyring/ceph/cluster/*/threshold/storage_profile_utilization/*": ceph_storage_profile_utilization_threshold_changed,
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
		// this is needed because while cluster is created before even
		// the Cluster is added in DB this event might come.
		var retrievedMonSuccessfully bool
		var monHostname string
		for count := 0; count < 12; count++ {
			monnode, err := GetRandomMon(event.ClusterId)
			monHostname = monnode.Hostname
			if err != nil {
				if err.Error() == mgo.ErrNotFound.Error() {
					time.Sleep(5 * time.Second)
					continue
				}
				logger.Get().Error("%s-Error getting a mon node in cluster: %s. error: %v", ctxt, event.ClusterId, err)
				return event, err
			} else {
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
		if err := node_coll.Find(bson.M{"hostname": bson.M{"$regex": fmt.Sprintf("%s.*", osd.Server), "$options": "$i"}}).One(&node); err != nil {
			logger.Get().Error("%s-Error fetching node details for SLU id: %d on cluster: %v. error: %v", ctxt, osd.Id, event.ClusterId, err)
			return event, err
		}

		event.NodeId = node.NodeId
		event.NodeName = node.Hostname
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
		}
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
	monnode, err := GetRandomMon(event.ClusterId)
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
	if err := node_coll.Find(bson.M{"hostname": bson.M{"$regex": fmt.Sprintf("%s.*", osd.Server), "$options": "$i"}}).One(&node); err != nil {
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
		event.Severity = models.ALARM_STATUS_WARNING
		event.Message = fmt.Sprintf("OSD %s on %s cluster went down", osdname, event.ClusterName)
	} else {
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
	event.EntityId = event.NodeId
	event.NotificationEntity = models.NOTIFICATION_ENTITY_HOST
	event.Name = EventTypes["mon_state_changed"]
	event.Notify = true
	if strings.Contains(event.Message, "joined quorum") {
		event.Severity = models.ALARM_STATUS_CLEARED
	} else {
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
