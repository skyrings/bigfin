package provider

import (
	"encoding/json"
	"fmt"
	"github.com/skyrings/bigfin/backend"
	bigfin_models "github.com/skyrings/bigfin/bigfinmodels"
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/skyring-common/conf"
	"github.com/skyrings/skyring-common/db"
	"github.com/skyrings/skyring-common/models"
	skyring_monitoring "github.com/skyrings/skyring-common/monitoring"
	"github.com/skyrings/skyring-common/tools/logger"
	"github.com/skyrings/skyring-common/tools/uuid"
	skyring_utils "github.com/skyrings/skyring-common/utils"
	"gopkg.in/mgo.v2/bson"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var monitoringRoutines = []interface{}{
	FetchOSDStats,
	FetchClusterStats,
	FetchRBDStats,
	FetchObjectCount,
	FetchPGSummary,
}

var (
	MonitoringManager skyring_monitoring.MonitoringManagerInterface
)

func InitMonitoringManager() error {
	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig
	if manager, err := skyring_monitoring.InitMonitoringManager(monitoringConfig.ManagerName, monitoringConfig.ConfigFilePath); err != nil {
		return err
	} else {
		MonitoringManager = manager
	}
	return nil
}

func initMonitoringRoutines(ctxt string, cluster models.Cluster, monName string) error {
	/*
		sync.WaitGroup can be used if it is required to do something after all go routines complete.
		For ex: pushing to db can be done after all go-routines complete.
	*/
	var wg sync.WaitGroup
	var errors string
	wg.Add(len(monitoringRoutines))
	for _, iFunc := range monitoringRoutines {
		if function, ok := iFunc.(func(string, models.Cluster, string) (map[string]map[string]string, error)); ok {
			go func(ctxt string, cluster models.Cluster, monName string) {
				defer wg.Done()
				response, err := function(ctxt, cluster, monName)
				if err != nil {
					logger.Get().Error("%s-%v", ctxt, err.Error())
					errors = errors + err.Error()
				}
				if response != nil {
					/*
					 The stats are being pushed as they are obtained because each of them can potentially have different intervals of schedule
					 in which case the respective routines would sleep for the said interval making the push to wait longer.
					*/
					err := pushTimeSeriesData(response, cluster.Name)
					if err != nil {
						logger.Get().Error("%s-%v", ctxt, err.Error())
						errors = errors + err.Error()
					}
				}
			}(ctxt, cluster, monName)
		} else {
			continue
		}
	}

	wg.Wait()
	return nil
}

func (s *CephProvider) MonitorCluster(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext

	cluster_id_str, ok := req.RpcRequestVars["cluster-id"]
	var monnode *models.Node
	if !ok {
		logger.Get().Error("%s-Incorrect cluster id: %s", ctxt, cluster_id_str)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Incorrect cluster id: %s", cluster_id_str))
		return fmt.Errorf("Incorrect cluster id: %s", cluster_id_str)
	}
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("%s-Error parsing the cluster id: %s. Error: %v", ctxt, cluster_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s.Error: %v", cluster_id_str, err))
		return fmt.Errorf("Error parsing the cluster id: %s.Error: %v", cluster_id_str, err)
	}
	cluster, err := getCluster(*cluster_id)
	if err != nil {
		logger.Get().Error("%s-Unable to get cluster with id %v.Err %v", ctxt, cluster_id, err.Error())
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unable to get cluster with id %v.Err %v", cluster_id, err.Error()))
		return fmt.Errorf("Unable to get cluster with id %v.Err %v", cluster_id, err.Error())
	}
	monnode, err = GetCalamariMonNode(*cluster_id, ctxt)
	if err != nil {
		logger.Get().Error("%s-Unable to pick a random mon from cluster %v.Error: %v", ctxt, cluster.Name, err.Error())
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unable to pick a random mon from cluster %v.Error: %v", cluster.Name, err.Error()))
		return fmt.Errorf("Unable to pick a random mon from cluster %v.Error: %v", cluster.Name, err.Error())
	}
	monName := (*monnode).Hostname
	err = initMonitoringRoutines(ctxt, cluster, monName)
	if err != nil {
		logger.Get().Error("%s-Error: %v", ctxt, err.Error())
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error: %v", err.Error()))
		return fmt.Errorf("Error: %v", err.Error())
	}
	*resp = utils.WriteResponseWithData(http.StatusOK, "", []byte{})
	return nil
}

func (s *CephProvider) GetClusterSummary(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext

	result := make(map[string]interface{})
	httpStatusCode := http.StatusOK

	cluster_id_str, ok := req.RpcRequestVars["cluster-id"]
	if !ok {
		logger.Get().Error("%s - Incorrect cluster id: %s", ctxt, cluster_id_str)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Incorrect cluster id: %s", cluster_id_str))
		return fmt.Errorf("Incorrect cluster id: %s", cluster_id_str)
	}
	clusterId, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("%s - Error parsing the cluster id: %s. Error: %v", ctxt, cluster_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s.Error: %v", cluster_id_str, err))
		return fmt.Errorf("Error parsing the cluster id: %s.Error: %v", cluster_id_str, err)
	}

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	mons, monErr := GetMons(bson.M{"clusterid": *clusterId})
	var err_str string
	if monErr != nil {
		err_str = fmt.Sprintf("Unable to fetch monitor nodes.Error %v", monErr.Error())
		logger.Get().Error("%s - Unable to fetch monitor nodes.Error %v", ctxt, monErr.Error())
	} else {
		result[models.Monitor] = len(mons)
	}

	cluster, clusterFetchErr := getCluster(*clusterId)
	if clusterFetchErr != nil {
		logger.Get().Error("%s - Unable to fetch cluster with id %v. Err %v", ctxt, *clusterId, clusterFetchErr)
		return fmt.Errorf("%s - Unable to fetch cluster with id %v. Err %v", ctxt, *clusterId, clusterFetchErr)
	}

	/*
		Fetch pg count
	*/
	pgNum := make(map[string]uint64)
	mon, monErr := GetCalamariMonNode(*clusterId, ctxt)
	if monErr != nil {
		err_str = err_str + fmt.Sprintf("%s - Failed to get the mon from cluster %v", ctxt, *clusterId)
	} else {
		pgCount, pgCountError := cephapi_backend.GetPGCount((*mon).Hostname, *clusterId, ctxt)
		if pgCountError != nil {
			err_str = err_str + fmt.Sprintf("%s - Failed to fetch the number of pgs from cluster %v", ctxt, *clusterId)
		} else {
			for status, statusCount := range pgCount {
				if status == skyring_monitoring.CRITICAL {
					pgNum[models.STATUS_ERR] = statusCount
				}
				if status == models.STATUS_WARN {
					pgNum[models.STATUS_WARN] = statusCount
				}
				pgNum[models.TOTAL] = pgNum[models.TOTAL] + statusCount
			}
		}
		result["pgnum"] = pgNum
	}

	result[bigfin_models.OBJECTS] = map[string]interface{}{bigfin_models.NUMBER_OF_OBJECTS: cluster.ObjectCount[bigfin_models.NUMBER_OF_OBJECTS], bigfin_models.NUMBER_OF_DEGRADED_OBJECTS: cluster.ObjectCount[bigfin_models.NUMBER_OF_DEGRADED_OBJECTS]}

	if err_str != "" {
		if len(result) != 0 {
			httpStatusCode = http.StatusPartialContent
		} else {
			httpStatusCode = http.StatusInternalServerError
		}
		err = fmt.Errorf("%s - %v", ctxt, err_str)
	}
	bytes, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		logger.Get().Error("%s - Failed to marshal %v.Error %v", ctxt, result, marshalErr)
		*resp = utils.WriteResponseWithData(http.StatusInternalServerError, err_str, []byte{})
		return fmt.Errorf("%s - Failed to marshal %v.Error %v", ctxt, result, marshalErr)
	}
	*resp = utils.WriteResponseWithData(httpStatusCode, err_str, bytes)
	return nil
}

func (s *CephProvider) GetSummary(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext

	result := make(map[string]interface{})
	httpStatusCode := http.StatusOK

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	mon_down_count := 0
	monCriticalAlertsCount := 0
	var err_str string
	mons, monErr := GetMons(nil)
	if monErr != nil {
		err_str = fmt.Sprintf("Unable to fetch monitor nodes.Error %v", monErr.Error())
		logger.Get().Error("%s - Unable to fetch monitor nodes.Error %v", ctxt, monErr.Error())
	} else {
		for _, mon := range mons {
			monCriticalAlertsCount = monCriticalAlertsCount + mon.AlmCritCount
			if mon.Status == models.NODE_STATUS_ERROR {
				mon_down_count = mon_down_count + 1
			}
		}
		result[models.Monitor] = map[string]int{skyring_monitoring.TOTAL: len(mons), models.STATUS_DOWN: mon_down_count, "criticalAlerts": monCriticalAlertsCount}
	}

	clusters, clustersFetchErr := getClustersByType(bigfin_models.CLUSTER_TYPE)
	if clustersFetchErr != nil {
		logger.Get().Error("%s - Unable to fetch clusters of type %v. Err %v", ctxt, bigfin_models.CLUSTER_TYPE, clustersFetchErr)
		return fmt.Errorf("%s - Unable to fetch clusters of type %v. Err %v", ctxt, bigfin_models.CLUSTER_TYPE, clustersFetchErr)
	}

	var objectCount int64
	var degradedObjectCount int64

	pgNum := make(map[string]uint64)

	for _, cluster := range clusters {
		/*
			Fetch object count
		*/
		objectCount = objectCount + cluster.ObjectCount[bigfin_models.NUMBER_OF_OBJECTS]
		degradedObjectCount = degradedObjectCount + cluster.ObjectCount[bigfin_models.NUMBER_OF_DEGRADED_OBJECTS]

		mon, monErr := GetRandomMon(cluster.ClusterId)
		if monErr != nil {
			err_str = err_str + fmt.Sprintf("%s - Failed to get the mon from cluster %v.Error %v", ctxt, cluster.ClusterId, monErr)
		} else {
			pgCount, pgCountError := cephapi_backend.GetPGCount((*mon).Hostname, cluster.ClusterId, ctxt)
			if pgCountError != nil {
				err_str = err_str + fmt.Sprintf("%s - Failed to fetch the number of pgs from cluster %v. Error %v", ctxt, cluster.ClusterId, pgCountError)
			} else {
				for status, statusCount := range pgCount {
					if status == skyring_monitoring.CRITICAL {
						pgNum[models.STATUS_ERR] = pgNum[models.STATUS_ERR] + statusCount
					}
					if status == models.STATUS_WARN {
						pgNum[models.STATUS_WARN] = pgNum[models.STATUS_WARN] + statusCount
					}
					pgNum[models.TOTAL] = pgNum[models.TOTAL] + statusCount
				}
			}
		}
		result["pgnum"] = pgNum
	}
	result[bigfin_models.OBJECTS] = map[string]interface{}{bigfin_models.NUMBER_OF_OBJECTS: objectCount, bigfin_models.NUMBER_OF_DEGRADED_OBJECTS: degradedObjectCount}

	if err_str != "" {
		if len(result) != 0 {
			httpStatusCode = http.StatusPartialContent
		} else {
			httpStatusCode = http.StatusInternalServerError
		}
	}
	bytes, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		*resp = utils.WriteResponseWithData(http.StatusInternalServerError, fmt.Sprintf("%s-%s", ctxt, err_str), []byte{})
		logger.Get().Error("%s-Failed to marshal %v.Error %v", ctxt, result, marshalErr)
		return fmt.Errorf("%s-Failed to marshal %v.Error %v", ctxt, result, marshalErr)
	}
	*resp = utils.WriteResponseWithData(httpStatusCode, err_str, bytes)
	return nil
}

func FetchOSDStats(ctxt string, cluster models.Cluster, monName string) (map[string]map[string]string, error) {
	var osdEvents []models.Event
	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig
	statistics, statsFetchErr := salt_backend.GetOSDDetails(monName, cluster.Name, ctxt)
	if statsFetchErr != nil {
		return nil, fmt.Errorf("Unable to fetch cluster stats from mon %v of cluster %v.Error: %v", monName, cluster.Name, statsFetchErr)
	}
	metrics := make(map[string]map[string]string)
	currentTimeStamp := time.Now().Unix()

	for _, osd := range statistics {
		metric_name := monitoringConfig.CollectionName + "." + cluster.Name + "." + skyring_monitoring.SLU_UTILIZATION + "_" + strings.Replace(osd.Name, ".", "_", -1) + "."
		timeStampStr := strconv.FormatInt(currentTimeStamp, 10)
		metrics[metric_name+skyring_monitoring.FREE_SPACE] = map[string]string{timeStampStr: strconv.FormatUint(osd.Available, 10)}
		metrics[metric_name+skyring_monitoring.USED_SPACE] = map[string]string{timeStampStr: strconv.FormatUint(osd.Used, 10)}
		metrics[metric_name+skyring_monitoring.PERCENT_USED] = map[string]string{timeStampStr: strconv.FormatUint(osd.UsagePercent, 10)}
		usage := models.Utilization{Used: int64(osd.Used), Total: int64(osd.Available + osd.Used), PercentUsed: float64(osd.UsagePercent)}
		if dbUpdateErr := updateDB(
			bson.M{"name": osd.Name, "clusterid": cluster.ClusterId},
			bson.M{"$set": bson.M{"usage": usage}},
			models.COLL_NAME_STORAGE_LOGICAL_UNITS); dbUpdateErr != nil {
			logger.Get().Error("%s - Error updating the osd details of %v of cluster %v.Err %v", ctxt, osd.Name, cluster.Name, dbUpdateErr)
		}
		event, isRaiseEvent, err := skyring_utils.AnalyseThresholdBreach(ctxt, skyring_monitoring.SLU_UTILIZATION, osd.Name, float64(osd.UsagePercent), cluster)
		if err != nil {
			logger.Get().Error("%s - Failed to analyse threshold breach for osd utilization of %v.Error %v", ctxt, osd.Name, err)
			continue
		}

		if isRaiseEvent {
			osdEvents = append(osdEvents, event)
		}
	}

	/*
		Correlation of Osd threshold crossing and storage profile threshold crossing

		Facts:
			0. Storage Profile utilization is calculated cluster-wise
			1. Osds are grouped into storage profiles.
			2. Storage profile capacity is the sum of capacities of Osds that are associated with the storage profile.
			3. The default threshold values for:
		      	Storage Profile Utilization : Warning  -> 65
		      								Critical -> 85
		      	OSD Utilization : Warning -> Warning  -> 85
		                                   Critical -> 95
			4. From 1, 2 and 3, storage profile utilization crossing a threshold may or may not have threshold crossings of associated OSDs

		Logic:
			1. Fetch all Osds in the current cluster
			2. Group osds into a map with key as the osds storage profile
			3. Loop over the storage profile threshold events and for each storage profile event:
				3.1. Get the list of osd events for the current storage profile.
				3.2. Add the list of osd events obtained in 3.1(empty or non-empty) to the field ImpactingEvents of the event related to the storage profile
				3.3. Loop over the osd events in 3.2 and set the flag notify false so that they are not separately notified to end user
				3.3. Raise threshold crossing event for storage profile
			4. Iterate over the entries in the map fetched from 2 and raise osd threshold crossing event.
			   For the osds captured already in the storage profile event's ImpactingEvents, the notification flag is turned off so the eventing module doesn't notify this
			   but just maintains the detail.
	*/
	slus, sluFetchErr := getOsds(cluster.ClusterId)
	if sluFetchErr != nil {
		return nil, sluFetchErr
	}

	spToOsdEvent := make(map[string][]models.Event)
	for _, osdEvent := range osdEvents {
		for _, slu := range slus {
			osdIdFromEvent, osdIdFromEventError := uuid.Parse(osdEvent.Tags[models.ENTITY_ID])
			if osdIdFromEventError != nil {
				logger.Get().Error("%s - Failed to parse osd id %v from cluster %v.Error %v", osdIdFromEvent, cluster.ClusterId, osdIdFromEventError)
			}

			if uuid.Equal(slu.SluId, *osdIdFromEvent) && slu.Name == osdEvent.Tags[models.ENTITY_NAME] {
				spToOsdEvent[slu.StorageProfile] = append(spToOsdEvent[slu.StorageProfile], osdEvent)
			}
		}
	}

	storageProfileStats, storageProfileStatsErr, storageProfileEvents := FetchStorageProfileUtilizations(ctxt, statistics, cluster)
	if storageProfileStatsErr != nil {
		for _, event := range osdEvents {
			if err, _ := HandleEvent(event, ctxt); err != nil {
				logger.Get().Error("%s - Threshold: %v.Error %v", ctxt, event, err)
			}
		}
		return metrics, fmt.Errorf("Failed to fetch storage profile utilizations for cluster %v.Error %v", cluster.Name, storageProfileStatsErr)
	}

	for _, spEvent := range storageProfileEvents {
		osdEvents := spToOsdEvent[spEvent.Tags[models.ENTITY_NAME]]
		impactingEvents := make(map[string][]models.Event)
		for _, osdEvent := range osdEvents {
			osdIdFromEvent, osdIdFromEventError := uuid.Parse(osdEvent.Tags[models.ENTITY_ID])
			if osdIdFromEventError != nil {
				logger.Get().Error("%s - Failed to parse osd id %v from cluster %v.Error %v", osdIdFromEvent, cluster.ClusterId, osdIdFromEventError)
			}
			impactingEvents[models.COLL_NAME_STORAGE_LOGICAL_UNITS] = append(impactingEvents[models.COLL_NAME_STORAGE_LOGICAL_UNITS], osdEvent)
			osdEvent.Tags[models.NOTIFY] = strconv.FormatBool(false)
		}
		spEvent.ImpactingEvents = impactingEvents
		if err, _ := HandleEvent(spEvent, ctxt); err != nil {
			logger.Get().Error("%s - Threshold: %v.Error %v", ctxt, spEvent, err)
		}
	}

	for _, osdEvents := range spToOsdEvent {
		for _, osdEvent := range osdEvents {
			if err, _ := HandleEvent(osdEvent, ctxt); err != nil {
				logger.Get().Error("%s - Threshold: %v.Error %v", ctxt, osdEvent, err)
			}
		}
	}

	for key, timeStampValueMap := range storageProfileStats {
		metrics[key] = timeStampValueMap
	}

	return metrics, nil
}

func FetchStorageProfileUtilizations(ctxt string, osdDetails []backend.OSDDetails, cluster models.Cluster) (statsToPush map[string]map[string]string, err error, storageProfileEvents []models.Event) {
	statsToPush = make(map[string]map[string]string)
	statsForEventAnalyse := make(map[string]float64)
	cluster.StorageProfileUsage = make(map[string]models.Utilization)
	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig
	slus, sluFetchErr := getOsds(cluster.ClusterId)
	if sluFetchErr != nil {
		return nil, sluFetchErr, []models.Event{}
	}
	currentTimeStamp := strconv.FormatInt(time.Now().Unix(), 10)
	metric_prefix := monitoringConfig.CollectionName + "." + cluster.Name + "." + skyring_monitoring.STORAGE_PROFILE_UTILIZATION + "_"
	var spFree uint64
	var spUsed uint64
	for _, slu := range slus {
		for _, osdDetail := range osdDetails {
			if osdDetail.Name == slu.Name {

				metric_name := metric_prefix + slu.StorageProfile + "."
				if utilization, ok := statsToPush[metric_name+skyring_monitoring.USED_SPACE]; ok {
					existingUtilization, err := strconv.ParseUint(utilization[currentTimeStamp], 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Error fetching osd stats for cluster %v. Error %v", cluster.Name, err), []models.Event{}
					}
					spUsed = osdDetail.Used + existingUtilization
					statsToPush[metric_name+skyring_monitoring.USED_SPACE] = map[string]string{currentTimeStamp: strconv.FormatUint(spUsed, 10)}
				} else {
					spUsed = osdDetail.Used
					statsToPush[metric_name+skyring_monitoring.USED_SPACE] = map[string]string{currentTimeStamp: strconv.FormatUint(spUsed, 10)}
				}

				if utilization, ok := statsToPush[metric_name+skyring_monitoring.FREE_SPACE]; ok {
					existingUtilization, err := strconv.ParseUint(utilization[currentTimeStamp], 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Error fetching osd stats for cluster %v. Error %v", cluster.Name, err), []models.Event{}
					}
					spFree = osdDetail.Available + existingUtilization
					statsToPush[metric_name+skyring_monitoring.FREE_SPACE] = map[string]string{currentTimeStamp: strconv.FormatUint(spFree, 10)}
				} else {
					spFree = osdDetail.Available
					statsToPush[metric_name+skyring_monitoring.FREE_SPACE] = map[string]string{currentTimeStamp: strconv.FormatUint(spFree, 10)}
				}

				percentUsed := float64(spUsed*100) / float64(spUsed+spFree)
				cluster.StorageProfileUsage[slu.StorageProfile] = models.Utilization{Used: int64(spUsed), Total: int64(spUsed + spFree), PercentUsed: percentUsed}

				percent_used := (float64(spUsed) * 100) / (float64(spUsed + spFree))
				statsToPush[metric_name+skyring_monitoring.USAGE_PERCENT] = map[string]string{currentTimeStamp: fmt.Sprintf("%v", percent_used)}

				statsForEventAnalyse[slu.StorageProfile] = percent_used
			}
		}
	}

	for statProfile, stat := range statsForEventAnalyse {
		event, isRaiseEvent, err := skyring_utils.AnalyseThresholdBreach(ctxt, skyring_monitoring.STORAGE_PROFILE_UTILIZATION, statProfile, stat, cluster)
		if err != nil {
			logger.Get().Error("%s - Failed to analyse threshold breach for storage profile utilization of %v.Error %v", ctxt, statProfile, err)
			continue
		}
		if isRaiseEvent {
			storageProfileEvents = append(storageProfileEvents, event)
		}
	}
	if err := updateDB(
		bson.M{"clusterid": cluster.ClusterId},
		bson.M{"$set": bson.M{"storageprofileusage": cluster.StorageProfileUsage}},
		models.COLL_NAME_STORAGE_CLUSTERS); err != nil {
		logger.Get().Error("%s - Updating the storage profile statistics to db for the cluster %v failed.Error %v", ctxt, cluster.Name, err.Error())
	}

	return statsToPush, nil, storageProfileEvents
}

func updateDB(query interface{}, update interface{}, collectionName string) error {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(collectionName)
	dbUpdateError := coll.Update(query, update)
	if dbUpdateError != nil {
		return dbUpdateError
	}
	return nil
}

func getOsds(clusterId uuid.UUID) (slus []models.StorageLogicalUnit, err error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
	if err := coll.Find(bson.M{"clusterid": clusterId}).Sort("name").All(&slus); err != nil {
		return nil, fmt.Errorf("Error getting the slus for cluster: %v. error: %v", clusterId, err)
	}
	return slus, nil
}

func FetchRBDStats(ctxt string, cluster models.Cluster, monName string) (map[string]map[string]string, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	collection := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
	var pools []models.Storage
	if err := collection.Find(bson.M{"clusterid": cluster.ClusterId}).All(&pools); err != nil {
		return nil, err
	}
	metrics := make(map[string]map[string]string)
	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig
	var err_str string
	currentTimeStamp := strconv.FormatInt(time.Now().Unix(), 10)
	for _, pool := range pools {
		metric_name := fmt.Sprintf("%s.%s.%s.%s.", monitoringConfig.CollectionName, cluster.Name, pool.Name, models.COLL_NAME_BLOCK_DEVICES)
		statistics, statsFetchErr := salt_backend.GetRBDStats(monName, pool.Name, cluster.Name, ctxt)
		if statsFetchErr != nil {
			err_str = fmt.Sprintf("%s.Unable to fetch rbd stats from mon %v of pool %v, cluster %v.Error: %v",
				err_str, monName, pool.Name, cluster.Name, statsFetchErr)
			continue
		}
		rbdColl := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_BLOCK_DEVICES)
		for _, rbdStat := range statistics {
			metric_name = fmt.Sprintf("%s%s.", metric_name, rbdStat.Name)
			var percent_used float64
			if rbdStat.Total > 0 {
				percent_used = float64(rbdStat.Used*100) / float64(rbdStat.Total)
			}
			if err := rbdColl.Update(bson.M{"clusterid": cluster.ClusterId, "name": rbdStat.Name, "storageid": pool.StorageId}, bson.M{"$set": bson.M{"usage": models.Utilization{Used: int64(rbdStat.Used), Total: int64(rbdStat.Total), PercentUsed: percent_used}}}); err != nil {
				err_str = fmt.Sprintf("%s.Unable to update rbd stats of rbd %v from mon %v of pool %v, cluster %v.Error: %v",
					err_str, rbdStat.Name, monName, pool.Name, cluster.Name, err)
			}

			event, isRaiseEvent, err := skyring_utils.AnalyseThresholdBreach(ctxt, skyring_monitoring.BLOCK_DEVICE_UTILIZATION, rbdStat.Name, percent_used, cluster)
			if err != nil {
				logger.Get().Error("%s - Failed to analyse threshold breach for utilization of rbd %v of pool %v from cluster %v.Error %v", ctxt, rbdStat.Name, pool.Name, cluster.Name, err)
			}
			if isRaiseEvent {
				if err, _ := HandleEvent(event, ctxt); err != nil {
					logger.Get().Error("%s - Threshold: %v.Error %v", ctxt, event, err)
				}
			}

			metrics[metric_name+skyring_monitoring.USED_SPACE] = map[string]string{currentTimeStamp: fmt.Sprintf("%v", rbdStat.Used)}
			metrics[metric_name+skyring_monitoring.TOTAL_SPACE] = map[string]string{currentTimeStamp: fmt.Sprintf("%v", rbdStat.Total)}
			metrics[metric_name+skyring_monitoring.PERCENT_USED] = map[string]string{currentTimeStamp: fmt.Sprintf("%v", percent_used)}
		}
	}
	if err_str == "" {
		return metrics, nil
	} else {
		return metrics, fmt.Errorf("%v", err_str)
	}
}

func FetchClusterStats(ctxt string, cluster models.Cluster, monName string) (map[string]map[string]string, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig

	statistics, statsFetchErr := salt_backend.GetClusterStats(monName, cluster.Name, ctxt)
	if statsFetchErr != nil {
		return nil, fmt.Errorf("Unable to fetch cluster stats from mon %v of cluster %v.Error: %v", monName, cluster.Name, statsFetchErr)
	}

	metrics := make(map[string]map[string]string)
	currentTimeStamp := time.Now().Unix()

	metric_name := fmt.Sprintf("%s.%s.%s.", monitoringConfig.CollectionName, cluster.Name, skyring_monitoring.CLUSTER_UTILIZATION)

	metrics[metric_name+skyring_monitoring.USED_SPACE] = map[string]string{strconv.FormatInt(currentTimeStamp, 10): fmt.Sprintf("%v", statistics.Used)}
	metrics[metric_name+skyring_monitoring.FREE_SPACE] = map[string]string{strconv.FormatInt(currentTimeStamp, 10): fmt.Sprintf("%v", statistics.Available)}
	metrics[metric_name+skyring_monitoring.TOTAL_SPACE] = map[string]string{strconv.FormatInt(currentTimeStamp, 10): fmt.Sprintf("%v", statistics.Total)}

	percentUsed := 0.0
	if statistics.Total != 0 {
		percentUsed = (float64(statistics.Used*100) / float64(statistics.Total))
	}
	metrics[metric_name+skyring_monitoring.USAGE_PERCENTAGE] = map[string]string{strconv.FormatInt(currentTimeStamp, 10): fmt.Sprintf("%v", percentUsed)}
	if err := updateDB(
		bson.M{"clusterid": cluster.ClusterId},
		bson.M{
			"$set": bson.M{
				"usage": models.Utilization{
					Used:        statistics.Used,
					Total:       statistics.Total,
					PercentUsed: percentUsed}}},
		models.COLL_NAME_STORAGE_CLUSTERS); err != nil {
		logger.Get().Error("%s-Updating the cluster statistics to db for the cluster %v failed.Error %v", ctxt, cluster.Name, err.Error())
	}

	event, isRaiseEvent, err := skyring_utils.AnalyseThresholdBreach(ctxt, skyring_monitoring.CLUSTER_UTILIZATION, cluster.Name, percentUsed, cluster)
	if err != nil {
		logger.Get().Error("%s - Failed to analyse threshold breach for cluster utilization of %v.Error %v", ctxt, cluster.Name, err)
	}
	if isRaiseEvent {
		if err, _ := HandleEvent(event, ctxt); err != nil {
			logger.Get().Error("%s - Threshold: %v.Error %v", ctxt, event, err)
		}
	}

	collection := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
	for _, poolStat := range statistics.Pools {
		used := poolStat.Used
		total := poolStat.Used + poolStat.Available
		percentUsed := 0.0
		if total != 0 {
			percentUsed = (float64(used*100) / float64(total))
			event, isRaiseEvent, err := skyring_utils.AnalyseThresholdBreach(ctxt, skyring_monitoring.STORAGE_UTILIZATION, poolStat.Name, percentUsed, cluster)
			if err != nil {
				logger.Get().Error("%s - Failed to analyse threshold breach for pool utilization of %v.Error %v", ctxt, poolStat.Name, err)
				continue
			}
			if isRaiseEvent {
				if err, _ := HandleEvent(event, ctxt); err != nil {
					logger.Get().Error("%s - Threshold: %v.Error %v", ctxt, event, err)
				}
			}
		}
		dbUpdateError := collection.Update(bson.M{"clusterid": cluster.ClusterId, "name": poolStat.Name}, bson.M{"$set": bson.M{"usage": models.Utilization{Used: used, Total: total, PercentUsed: percentUsed}}})
		if dbUpdateError != nil {
			logger.Get().Error("%s-Failed to update utilization of pool %v of cluster %v to db.Error %v", ctxt, poolStat.Name, cluster.Name, dbUpdateError)
		}
	}

	return metrics, nil
}

func FetchObjectCount(ctxt string, cluster models.Cluster, monName string) (map[string]map[string]string, error) {
	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig
	statistics, statsFetchErr := salt_backend.GetObjectCount(monName, cluster.Name, ctxt)
	if statsFetchErr != nil {
		return nil, fmt.Errorf("Unable to fetch object count from mon %v of cluster %v.Error: %v", monName, cluster.Name, statsFetchErr)
	}

	//object_cnt, cerr := strconv.ParseUint(statistics, 10, 64)
	objectCnt := statistics[bigfin_models.NUMBER_OF_OBJECTS]
	objectErrCnt := statistics[bigfin_models.NUMBER_OF_DEGRADED_OBJECTS]
	metrics := make(map[string]map[string]string)
	currentTimeStamp := time.Now().Unix()
	timeStampStr := strconv.FormatInt(currentTimeStamp, 10)
	metric_name := monitoringConfig.CollectionName + "." + cluster.Name + "." + skyring_monitoring.NO_OF_OBJECT
	metrics[metric_name] = map[string]string{timeStampStr: fmt.Sprintf("%v", objectCnt)}

	if err := updateDB(
		bson.M{"clusterid": cluster.ClusterId},
		bson.M{
			"$set": bson.M{
				"objectcount": map[string]int64{
					bigfin_models.NUMBER_OF_OBJECTS:          objectCnt,
					bigfin_models.NUMBER_OF_DEGRADED_OBJECTS: objectErrCnt}}},
		models.COLL_NAME_STORAGE_CLUSTERS); err != nil {
		logger.Get().Error("%s-Updating the object count to db for the cluster %v failed.Error %v", ctxt, cluster.Name, err.Error())
	}

	return metrics, nil
}

func FetchPGSummary(ctxt string, cluster models.Cluster, monName string) (map[string]map[string]string, error) {
	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig
	statistics, statsFetchErr := cephapi_backend.GetPGSummary(monName, cluster.ClusterId, ctxt)
	if statsFetchErr != nil {
		return nil, fmt.Errorf("Unable to fetch PG Details from mon %v of cluster %v.Error: %v", monName, cluster.Name, statsFetchErr)
	}
	metrics := make(map[string]map[string]string)
	currentTimeStamp := time.Now().Unix()
	for k := range statistics.ByPool {
		for k1, v1 := range statistics.ByPool[k] {
			k1 = strings.Replace(k1, "+", "_", -1)
			metric_name := fmt.Sprintf("%s.%s.%s.pool-%s.%s", monitoringConfig.CollectionName, cluster.Name, skyring_monitoring.PG_SUMMARY, k, k1)
			statMap := make(map[string]string)
			statMap[strconv.FormatInt(currentTimeStamp, 10)] = strconv.FormatUint(v1, 10)
			metrics[metric_name] = statMap
		}
	}
	for k, v := range statistics.All {
		k = strings.Replace(k, "+", "_", -1)
		metric_name := fmt.Sprintf("%s.%s.%s.all.%s", monitoringConfig.CollectionName, cluster.Name, skyring_monitoring.PG_SUMMARY, k)
		statMap := make(map[string]string)
		statMap[strconv.FormatInt(currentTimeStamp, 10)] = strconv.FormatUint(v, 10)
		metrics[metric_name] = statMap

	}

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	var slus []models.StorageLogicalUnit
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_LOGICAL_UNITS)
	if err := coll.Find(bson.M{"clusterid": cluster.ClusterId}).All(&slus); err != nil {
		logger.Get().Error("%s-Error getting the slus for cluster: %v. error: %v", ctxt, cluster.ClusterId, err)
		return metrics, nil
	}

	for _, slu := range slus {
		slu.Options["pgsummary"] = statistics.ByOSD[strings.Replace(slu.Name, "osd.", "", 1)]
		if err := coll.Update(
			bson.M{"clusterid": cluster.ClusterId, "name": slu.Name},
			bson.M{"$set": bson.M{"options": slu.Options}}); err != nil {
			logger.Get().Error("%s-Error updating the slu: %s. error: %v", ctxt, slu.Name, err)
			continue
		}
	}

	return metrics, nil
}

func pushTimeSeriesData(metrics map[string]map[string]string, clusterName string) error {
	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig
	if metrics == nil {
		return fmt.Errorf("The metrics are nil")
	}
	if MonitoringManager == nil {
		return fmt.Errorf("Monitoring manager was not initialized successfully")
	}
	if err := MonitoringManager.PushToDb(metrics, monitoringConfig.Hostname, monitoringConfig.DataPushPort); err != nil {
		return fmt.Errorf("Failed to push statistics of cluster %v to db.Error: %v", clusterName, err)
	}
	return nil
}

func getClustersByType(clusterType string) (clusters []models.Cluster, err error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	collection := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := collection.Find(bson.M{"type": clusterType}).All(&clusters); err != nil {
		return clusters, err
	}
	return clusters, nil
}

func getCluster(cluster_id uuid.UUID) (cluster models.Cluster, err error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	collection := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := collection.Find(bson.M{"clusterid": cluster_id}).One(&cluster); err != nil {
		return cluster, err
	}
	return cluster, nil
}

func (s *CephProvider) GetServiceCount(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext
	request := make(map[string]interface{})
	if err := json.Unmarshal(req.RpcRequestData, &request); err != nil {
		logger.Get().Error(fmt.Sprintf("%s-Unbale to parse the Get Service Count request. error: %v", ctxt, err))
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request. error: %v", err))
		return err
	}
	Hostname := request["hostname"].(string)
	TotalSluCount := request["totalslu"].(float64)
	NodeRoles := request["noderoles"]
	ServiceDetails := make(map[string]interface{})
	ServiceCount, err := salt_backend.GetServiceCount(Hostname, ctxt)
	if err != nil {
		logger.Get().Error(
			"%s-Error fetching service count for node: %s. error: %v",
			ctxt,
			Hostname,
			err)
		*resp = utils.WriteResponse(http.StatusInternalServerError,
			fmt.Sprintf("Error fetching service count for node %s. error %v", Hostname, err))
		return err

	}
	for _, role := range NodeRoles.([]interface{}) {
		switch role.(string) {
		case strings.ToUpper(bigfin_models.NODE_SERVICE_OSD):
			SluUp := ServiceCount[bigfin_models.SLU_SERVICE_COUNT]
			SluDown := int(TotalSluCount) - ServiceCount[bigfin_models.SLU_SERVICE_COUNT]
			ServiceDetails["slu"] = map[string]int{"up": SluUp, "down": SluDown}
		case strings.ToUpper(bigfin_models.NODE_SERVICE_MON):
			ServiceDetails["mon"] = ServiceCount[bigfin_models.MON_SERVICE_COUNT]
		}
	}
	var bytes []byte
	bytes, err = json.Marshal(ServiceDetails)
	if err != nil {
		logger.Get().Error("%s-Unable to marshal the service count details :%s", ctxt, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError,
			fmt.Sprintf("Unable to marshal the service count details for node :%s . error %v", Hostname, err))
		return err
	}
	*resp = utils.WriteResponseWithData(http.StatusOK, "", bytes)
	return nil
}
