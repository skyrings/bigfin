package provider

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

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
)

var MonitoringRoutines = []interface{}{
	FetchOSDStats,
	FetchClusterStats,
	FetchRBDStats,
	FetchObjectCount,
	FetchPGSummary,
}

type ClusterStats struct {
	Stats struct {
		Total       int64   `json:"total_bytes"`
		Used        int64   `json:"total_used_bytes"`
		Available   int64   `json:"total_avail_bytes"`
		PercentUsed float64 `json:"percent_used"`
	} `json:"stats"`
	Pools []PoolStats `json:"pools"`
}

type PoolStats struct {
	Name            string `json:"name"`
	Id              int    `json:"id"`
	PoolUtilization struct {
		Used        int64   `json:"bytes_used"`
		Available   int64   `json:"max_avail"`
		PercentUsed float64 `json:"percent_used"`
	} `json:"stats"`
}

type OSDStats struct {
	OSDs []struct {
		Name         string  `json:"name"`
		Id           int     `json:"id"`
		Available    int64   `json:"kb_avail"`
		Used         int64   `json:"kb_used"`
		UsagePercent float64 `json:"utilization"`
		Total        int64   `json:"kb"`
	} `json:"nodes"`
}

type RBDStats struct {
	RBDs []struct {
		Name  string `json:"name"`
		Used  int64  `json:"used_size"`
		Total int64  `json:"provisioned_size"`
	} `json:"images"`
}

var commandTemplates = map[string]string{
	skyring_monitoring.SLU_UTILIZATION:          "ceph osd df --cluster {{.clusterName}} -f json",
	skyring_monitoring.CLUSTER_UTILIZATION:      "ceph df --cluster {{.clusterName}}",
	skyring_monitoring.BLOCK_DEVICE_UTILIZATION: "rbd du --cluster {{.clusterName}} -p {{.poolName}} --format=json",
	skyring_monitoring.NO_OF_OBJECT:             "rados df --cluster {{.clusterName}} --format=json",
}

var (
	MonitoringManager skyring_monitoring.MonitoringManagerInterface
)

func getStatsFromCalamariApi(ctxt string, mon string, clusterId uuid.UUID, commandParams map[string]string, utilizationType string) (response string, isSuccess bool) {
	buf := new(bytes.Buffer)
	parsedTemplate, err := template.New("monitoring").Parse(commandTemplates[utilizationType])
	if err != nil {
		logger.Get().Error("%s - Failed to fetch %v of cluster %v.Could not parse template %v with params %v.Error %v", ctxt, utilizationType, clusterId, commandTemplates[utilizationType], commandParams, err)
		return "", false
	}
	parsedTemplate.Execute(buf, commandParams)
	commandString := buf.String()

	ok, out, err := cephapi_backend.ExecCmd(mon, clusterId, commandString, ctxt)
	if !ok || err != nil {
		logger.Get().Error("%s - Failed to fetch %v metrics from cluster %v.Err %v", ctxt, utilizationType, clusterId, err)
		return "", false
	}
	return out, true
}

func InitMonitoringManager() error {
	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig
	if manager, err := skyring_monitoring.InitMonitoringManager(monitoringConfig.ManagerName, monitoringConfig.ConfigFilePath); err != nil {
		return err
	} else {
		MonitoringManager = manager
	}
	return nil
}

func initMonitoringRoutines(ctxt string, cluster models.Cluster, monName string, monitoringRoutines []interface{}) error {
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
	err = initMonitoringRoutines(ctxt, cluster, monName, MonitoringRoutines)
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

	mon_down_count := 0
	monCriticalAlertsCount := 0
	mons, monErr := GetMons(bson.M{"clusterid": *clusterId})
	var err_str string
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

	cluster, clusterFetchErr := getCluster(*clusterId)
	if clusterFetchErr != nil {
		logger.Get().Error("%s - Unable to fetch cluster with id %v. Err %v", ctxt, *clusterId, clusterFetchErr)
		return fmt.Errorf("%s - Unable to fetch cluster with id %v. Err %v", ctxt, *clusterId, clusterFetchErr)
	}

	/*
		Fetch pg count
	*/
	if pgCount, err := ComputeClusterPGNum(cluster, ctxt); err == nil {
		result["pgnum"] = pgCount
	} else {
		err_str = err_str + fmt.Sprintf("%s", err.Error())
	}

	result[bigfin_models.OBJECTS] = ComputeClusterObjectCount(cluster)

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

func ComputeMonCount(selectCriteria bson.M) (map[string]int, error) {
	mon_down_count := 0
	monCriticalAlertsCount := 0
	mons, monErr := GetMons(selectCriteria)
	if monErr != nil {
		return map[string]int{skyring_monitoring.TOTAL: len(mons), models.STATUS_DOWN: mon_down_count, "criticalAlerts": monCriticalAlertsCount}, monErr
	}
	for _, mon := range mons {
		monCriticalAlertsCount = monCriticalAlertsCount + mon.AlmCritCount
		if mon.Status == models.NODE_STATUS_ERROR {
			mon_down_count = mon_down_count + 1
		}
	}
	return map[string]int{skyring_monitoring.TOTAL: len(mons), models.STATUS_DOWN: mon_down_count, "criticalAlerts": monCriticalAlertsCount}, monErr
}

func UpdateMonCountToSummaries(ctxt string, cluster models.Cluster) {
	monCnt, monErr := ComputeMonCount(bson.M{"clusterid": cluster.ClusterId})
	if monErr != nil {
		logger.Get().Error("%s - Failed to get mon count for cluster %v.Error %v", ctxt, cluster.Name, monErr)
	} else {
		updateField := fmt.Sprintf("providermonitoringdetails.%s.monitor", cluster.Type)
		skyring_utils.UpdateDb(bson.M{"clusterid": cluster.ClusterId}, bson.M{updateField: monCnt}, models.COLL_NAME_CLUSTER_SUMMARY, ctxt)
		monCnt, monErr := ComputeMonCount(nil)
		if monErr != nil {
			logger.Get().Error("%s - Failed to upadte system mon count.Error %v", ctxt, monErr)
		}
		skyring_utils.UpdateDb(bson.M{"name": skyring_monitoring.SYSTEM}, bson.M{updateField: monCnt}, models.COLL_NAME_SKYRING_UTILIZATION, ctxt)
	}
}

func ComputeClusterObjectCount(cluster models.Cluster) map[string]interface{} {
	return map[string]interface{}{
		bigfin_models.NUMBER_OF_OBJECTS:          cluster.ObjectCount[bigfin_models.NUMBER_OF_OBJECTS],
		bigfin_models.NUMBER_OF_DEGRADED_OBJECTS: cluster.ObjectCount[bigfin_models.NUMBER_OF_DEGRADED_OBJECTS],
	}
}

func ComputeClusterPGNum(cluster models.Cluster, ctxt string) (map[string]uint64, error) {
	pgNum := make(map[string]uint64)
	mon, monErr := GetCalamariMonNode(cluster.ClusterId, ctxt)
	if monErr != nil {
		return pgNum, fmt.Errorf("Failed to get the mon from cluster %v", cluster.ClusterId)
	} else {
		pgCount, pgCountError := cephapi_backend.GetPGCount((*mon).Hostname, cluster.ClusterId, ctxt)
		if pgCountError != nil {
			return nil, fmt.Errorf("Failed to fetch the number of pgs from cluster %v", cluster.Name)
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
	}
	return pgNum, nil
}

func UpdatePgNumToSummaries(cluster models.Cluster, ctxt string) {
	cPgNum, err := ComputeClusterPGNum(cluster, ctxt)
	if err != nil {
		logger.Get().Error("%s - Failed to fetch pg summary for cluster %v. Error %v", ctxt, cluster.Name, err)
		return
	} else {
		updateField := fmt.Sprintf("providermonitoringdetails.%s.pgnum", cluster.Type)
		skyring_utils.UpdateDb(bson.M{"clusterid": cluster.ClusterId}, bson.M{updateField: cPgNum}, models.COLL_NAME_CLUSTER_SUMMARY, ctxt)
		sPgNum, err := ComputeSystemPGNum()
		if err != nil {
			logger.Get().Error("%s - Failed to update pg summary to system summary.Error %v", ctxt, err)
		} else {
			skyring_utils.UpdateDb(bson.M{"name": skyring_monitoring.SYSTEM}, bson.M{updateField: sPgNum}, models.COLL_NAME_SKYRING_UTILIZATION, ctxt)
		}
	}
}

func ComputeSystemPGNum() (map[string]uint64, error) {
	var err_str string
	pgNum := make(map[string]uint64)
	clusterSummaries, clusterSummariesFetchErr := skyring_utils.GetClusterSummaries(bson.M{"type": bigfin_models.CLUSTER_TYPE})
	if clusterSummariesFetchErr != nil {
		if system, systemFetchErr := skyring_utils.GetSystem(); systemFetchErr == nil {
			if providerDetails, ok := system.ProviderMonitoringDetails[bigfin_models.CLUSTER_TYPE]; ok {
				if pgNum, pgNumOk := providerDetails["pgnum"]; pgNumOk {
					return pgNum.(map[string]uint64), clusterSummariesFetchErr
				}
			}
		}
		return pgNum, fmt.Errorf("Unable to fetch clusters of type %v. Err %v", bigfin_models.CLUSTER_TYPE, clusterSummariesFetchErr)
	}
	for _, clusterSummary := range clusterSummaries {
		if pgnum, ok := clusterSummary.ProviderMonitoringDetails[bigfin_models.CLUSTER_TYPE]["pgnum"].(map[string]interface{}); ok {
			if errorCnt, errorCntOk := pgnum["error"].(float64); errorCntOk {
				pgNum["error"] = pgNum["error"] + uint64(errorCnt)
			}
			if warnCnt, warnCntOk := pgnum["warning"].(float64); warnCntOk {
				pgNum["warning"] = pgNum["warning"] + uint64(warnCnt)
			}
			if totalCnt, totalCntOk := pgnum["total"].(float64); totalCntOk {
				pgNum["total"] = pgNum["total"] + uint64(totalCnt)
			}
		}
	}
	return pgNum, fmt.Errorf("%s", err_str)
}

func UpdateObjectCountToSummaries(ctxt string, cluster models.Cluster) {
	objCnt, err := ComputeObjectCount(bson.M{"clusterid": cluster.ClusterId})
	if err != nil {
		logger.Get().Error("%s - Failed to get object count from cluster %v. Error %v", ctxt, cluster.Name)
	} else {
		skyring_utils.UpdateDb(bson.M{"clusterid": cluster.ClusterId}, bson.M{"objectcount": objCnt}, models.COLL_NAME_CLUSTER_SUMMARY, ctxt)
		objCnt, err := ComputeObjectCount(nil)
		if err != nil {
			logger.Get().Error("%s - Failed to update object count for system summary. Error %v", ctxt)
		} else {
			skyring_utils.UpdateDb(bson.M{"name": skyring_monitoring.SYSTEM}, bson.M{"objectcount": objCnt}, models.COLL_NAME_SKYRING_UTILIZATION, ctxt)
		}
	}
}

func (s *CephProvider) GetSummary(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext

	result := make(map[string]interface{})
	httpStatusCode := http.StatusOK

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

	var err error

	result["pgnum"], err = ComputeSystemPGNum()
	err_str = err_str + fmt.Sprintf("%s", err)

	if err_str != "" {
		if len(result) != 0 {
			httpStatusCode = http.StatusPartialContent
		} else {
			httpStatusCode = http.StatusInternalServerError
		}
	}
	objCount, err := ComputeObjectCount(nil)
	if err != nil {
		logger.Get().Error("%s - Error fetching the object count. Error %v", ctxt, err)
	}
	result["objects"] = objCount
	bytes, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		*resp = utils.WriteResponseWithData(http.StatusInternalServerError, fmt.Sprintf("%s-%s", ctxt, err_str), []byte{})
		logger.Get().Error("%s-Failed to marshal %v.Error %v", ctxt, result, marshalErr)
		return fmt.Errorf("%s-Failed to marshal %v.Error %v", ctxt, result, marshalErr)
	}
	*resp = utils.WriteResponseWithData(httpStatusCode, err_str, bytes)
	return nil
}

func ComputeObjectCount(selectCriteria bson.M) (map[string]int64, error) {
	clusters, err := skyring_utils.GetClusters(selectCriteria)
	var objectCount int64
	var degradedObjectCount int64

	for _, cluster := range clusters {
		objectCount = objectCount + cluster.ObjectCount[bigfin_models.NUMBER_OF_OBJECTS]
		degradedObjectCount = degradedObjectCount + cluster.ObjectCount[bigfin_models.NUMBER_OF_DEGRADED_OBJECTS]
	}
	return map[string]int64{bigfin_models.NUMBER_OF_OBJECTS: objectCount, bigfin_models.NUMBER_OF_DEGRADED_OBJECTS: degradedObjectCount}, err
}

func updateOSDStats(ctxt string, cluster models.Cluster, monName string) (map[string]map[string]string, OSDStats, error) {
	var statistics OSDStats
	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig

	response, isSuccess := getStatsFromCalamariApi(ctxt,
		monName,
		cluster.ClusterId,
		map[string]string{"clusterName": cluster.Name},
		skyring_monitoring.SLU_UTILIZATION)

	if !isSuccess {
		/*
			Already generically handled in above getStatsFromCalamariApi function.
			So just a return from this function is required here.
		*/

		return nil, statistics, fmt.Errorf("%s - Failed to fetch osd stats for cluster %v from mon %v", ctxt, cluster.Name, monName)
	}

	if err := json.Unmarshal([]byte(response), &statistics); err != nil {
		return nil, statistics, fmt.Errorf("%s - Failed to fetch osd stats from cluster %v.Could not unmarshal %v.Error %v", ctxt, cluster.Name, response, err)
	}

	metrics := make(map[string]map[string]string)
	currentTimeStamp := time.Now().Unix()

	for _, osd := range statistics.OSDs {
		metric_name := monitoringConfig.CollectionName + "." + strings.Replace(cluster.Name, ".", "_", -1) + "." + skyring_monitoring.SLU_UTILIZATION + "_" + strings.Replace(osd.Name, ".", "_", -1) + "."
		timeStampStr := strconv.FormatInt(currentTimeStamp, 10)
		metrics[metric_name+skyring_monitoring.FREE_SPACE] = map[string]string{timeStampStr: strconv.FormatInt(osd.Available*1024, 10)}
		metrics[metric_name+skyring_monitoring.USED_SPACE] = map[string]string{timeStampStr: strconv.FormatInt(osd.Used*1024, 10)}
		metrics[metric_name+skyring_monitoring.PERCENT_USED] = map[string]string{timeStampStr: strconv.FormatFloat(osd.UsagePercent, 'E', -1, 64)}
		usage := models.Utilization{
			Used:        int64(osd.Used * 1024),
			Total:       int64(osd.Total * 1024),
			PercentUsed: float64(osd.UsagePercent),
			UpdatedAt:   time.Now().String()}
		if dbUpdateErr := updateDB(
			bson.M{"name": osd.Name, "clusterid": cluster.ClusterId},
			bson.M{"$set": bson.M{"usage": usage}},
			models.COLL_NAME_STORAGE_LOGICAL_UNITS); dbUpdateErr != nil {
			logger.Get().Error("%s - Error updating the osd details of %v of cluster %v.Err %v", ctxt, osd.Name, cluster.Name, dbUpdateErr)
		}
	}
	return metrics, statistics, nil
}

func FetchOSDStats(ctxt string, cluster models.Cluster, monName string) (map[string]map[string]string, error) {
	var osdEvents []models.Event
	metrics, statistics, err := updateOSDStats(ctxt, cluster, monName)
	if err != nil {
		logger.Get().Error("%s - %v", ctxt, err)
		return metrics, err
	}

	for _, osd := range statistics.OSDs {
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

func FetchStorageProfileUtilizations(ctxt string, osdDetails OSDStats, cluster models.Cluster) (statsToPush map[string]map[string]string, err error, storageProfileEvents []models.Event) {
	statsToPush = make(map[string]map[string]string)
	statsForEventAnalyse := make(map[string]float64)
	cluster.StorageProfileUsage = make(map[string]models.Utilization)
	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig
	slus, sluFetchErr := getOsds(cluster.ClusterId)
	if sluFetchErr != nil {
		return nil, sluFetchErr, []models.Event{}
	}
	currentTimeStamp := strconv.FormatInt(time.Now().Unix(), 10)
	metric_prefix := monitoringConfig.CollectionName + "." + strings.Replace(cluster.Name, ".", "_", -1) + "." + skyring_monitoring.STORAGE_PROFILE_UTILIZATION + "_"
	var spFree int64
	var spUsed int64
	for _, slu := range slus {
		for _, osdDetail := range osdDetails.OSDs {
			if osdDetail.Name == slu.Name {

				metric_name := metric_prefix + strings.Replace(slu.StorageProfile, ".", "_", -1) + "."
				if utilization, ok := statsToPush[metric_name+skyring_monitoring.USED_SPACE]; ok {
					existingUtilization, err := strconv.ParseInt(utilization[currentTimeStamp], 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Error fetching osd stats for cluster %v. Error %v", cluster.Name, err), []models.Event{}
					}
					spUsed = (osdDetail.Used * 1024) + existingUtilization
					statsToPush[metric_name+skyring_monitoring.USED_SPACE] = map[string]string{currentTimeStamp: strconv.FormatInt(spUsed, 10)}
				} else {
					spUsed = osdDetail.Used * 1024
					statsToPush[metric_name+skyring_monitoring.USED_SPACE] = map[string]string{currentTimeStamp: strconv.FormatInt(spUsed, 10)}
				}

				if utilization, ok := statsToPush[metric_name+skyring_monitoring.FREE_SPACE]; ok {
					existingUtilization, err := strconv.ParseInt(utilization[currentTimeStamp], 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Error fetching osd stats for cluster %v. Error %v", cluster.Name, err), []models.Event{}
					}
					spFree = (osdDetail.Available * 1024) + existingUtilization
					statsToPush[metric_name+skyring_monitoring.FREE_SPACE] = map[string]string{currentTimeStamp: strconv.FormatInt(spFree, 10)}
				} else {
					spFree = osdDetail.Available * 1024
					statsToPush[metric_name+skyring_monitoring.FREE_SPACE] = map[string]string{currentTimeStamp: strconv.FormatInt(spFree, 10)}
				}
				var percentUsed float64
				if spUsed+spFree > 0 {
					percentUsed = float64(spUsed*100) / float64(spUsed+spFree)
				}
				cluster.StorageProfileUsage[slu.StorageProfile] = models.Utilization{Used: int64(spUsed), Total: int64(spUsed + spFree), PercentUsed: percentUsed, UpdatedAt: time.Now().String()}

				statsToPush[metric_name+skyring_monitoring.USAGE_PERCENT] = map[string]string{currentTimeStamp: fmt.Sprintf("%v", percentUsed)}

				statsForEventAnalyse[slu.StorageProfile] = percentUsed
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

func updateRBDStats(ctxt string, cluster models.Cluster, monName string) (map[string]map[string]string, []RBDStats, error) {
	var err_str string
	var rbdStats []RBDStats
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	collection := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
	var pools []models.Storage
	if err := collection.Find(bson.M{"clusterid": cluster.ClusterId}).All(&pools); err != nil {
		logger.Get().Error("%s - Failed to fetch rbd stats for cluster %v.Error %v", ctxt, cluster.Name, err)
		return nil, rbdStats, err
	}
	metrics := make(map[string]map[string]string)
	currentTimeStamp := strconv.FormatInt(time.Now().Unix(), 10)
	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig
	for _, pool := range pools {
		var statistics RBDStats
		metric_name := fmt.Sprintf("%s.%s.%s.%s.", monitoringConfig.CollectionName, strings.Replace(cluster.Name, ".", "_", -1), strings.Replace(pool.Name, ".", "_", -1), models.COLL_NAME_BLOCK_DEVICES)
		response, isSuccess := getStatsFromCalamariApi(ctxt,
			monName,
			cluster.ClusterId,
			map[string]string{"clusterName": cluster.Name, "poolName": pool.Name},
			skyring_monitoring.BLOCK_DEVICE_UTILIZATION)

		if !isSuccess {
			/*
				Already generically handled in above getStatsFromCalamariApi function.
				So just a process next item.
			*/

			continue
		}

		if err := json.Unmarshal([]byte(response), &statistics); err != nil {
			err_str = fmt.Sprintf("%s. Failed to fetch rbd stats from pool %v, cluster %v.Could not unmarshal %v.Error %v", err_str, pool.Name, cluster.Name, response, err)
			continue
		}

		rbdColl := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_BLOCK_DEVICES)
		for _, rbdStat := range statistics.RBDs {
			metric_name = fmt.Sprintf("%s%s.", metric_name, strings.Replace(rbdStat.Name, ".", "_", -1))
			var percent_used float64
			if rbdStat.Total > 0 {
				percent_used = float64(rbdStat.Used*100) / float64(rbdStat.Total)
			}
			if err := rbdColl.Update(bson.M{"clusterid": cluster.ClusterId, "name": rbdStat.Name, "storageid": pool.StorageId}, bson.M{"$set": bson.M{"usage": models.Utilization{
				Used:        int64(rbdStat.Used),
				Total:       int64(rbdStat.Total),
				PercentUsed: percent_used,
				UpdatedAt:   time.Now().String()}}}); err != nil {
				err_str = fmt.Sprintf("%s.Unable to update rbd stats of rbd %v from mon %v of pool %v, cluster %v.Error: %v",
					err_str, rbdStat.Name, monName, pool.Name, cluster.Name, err)
			}
			metrics[metric_name+skyring_monitoring.USED_SPACE] = map[string]string{currentTimeStamp: fmt.Sprintf("%v", rbdStat.Used)}
			metrics[metric_name+skyring_monitoring.TOTAL_SPACE] = map[string]string{currentTimeStamp: fmt.Sprintf("%v", rbdStat.Total)}
			metrics[metric_name+skyring_monitoring.PERCENT_USED] = map[string]string{currentTimeStamp: fmt.Sprintf("%v", percent_used)}
		}
		rbdStats = append(rbdStats, statistics)
	}
	return metrics, rbdStats, fmt.Errorf("%v", err_str)
}

func FetchRBDStats(ctxt string, cluster models.Cluster, monName string) (map[string]map[string]string, error) {
	metrics, statistics, statsErr := updateRBDStats(ctxt, cluster, monName)

	for _, stat := range statistics {
		for _, rbdStat := range stat.RBDs {
			var percent_used float64
			if rbdStat.Total > 0 {
				percent_used = float64(rbdStat.Used*100) / float64(rbdStat.Total)
			}
			event, isRaiseEvent, err := skyring_utils.AnalyseThresholdBreach(ctxt, skyring_monitoring.BLOCK_DEVICE_UTILIZATION, rbdStat.Name, percent_used, cluster)
			if err != nil {
				logger.Get().Error("%s - Failed to analyse threshold breach for utilization of rbd %v from cluster %v.Error %v", ctxt, rbdStat.Name, cluster.Name, err)
			}
			if isRaiseEvent {
				if err, _ := HandleEvent(event, ctxt); err != nil {
					logger.Get().Error("%s - Threshold: %v.Error %v", ctxt, event, err)
				}
			}
		}
	}
	return metrics, statsErr
}

func GetStatInBytes(stat string) (int64, error) {
	if strings.HasSuffix(stat, "M") {
		size, err := strconv.ParseInt(strings.TrimSuffix(stat, "M"), 10, 64)
		return size * 1024 * 1024, err
	}
	if strings.HasSuffix(stat, "G") {
		size, err := strconv.ParseInt(strings.TrimSuffix(stat, "G"), 10, 64)
		return size * 1024 * 1024 * 1024, err
	}
	if strings.HasSuffix(stat, "T") {
		size, err := strconv.ParseInt(strings.TrimSuffix(stat, "T"), 10, 64)
		return size * 1024 * 1024 * 1024 * 1024, err
	}
	if strings.HasSuffix(stat, "P") {
		size, err := strconv.ParseInt(strings.TrimSuffix(stat, "P"), 10, 64)
		return size * 1024 * 1024 * 1024 * 1024 * 1024, err
	}
	if strings.HasSuffix(stat, "E") {
		size, err := strconv.ParseInt(strings.TrimSuffix(stat, "E"), 10, 64)
		return size * 1024 * 1024 * 1024 * 1024 * 1024 * 1024, err
	}
	if strings.HasSuffix(stat, "k") {
		size, err := strconv.ParseInt(strings.TrimSuffix(stat, "k"), 10, 64)
		return size * 1024, err
	}
	return strconv.ParseInt(stat, 10, 64)
}

func parseClusterStats(cmdOp string) (stats ClusterStats, err error) {
	var err_str string
	var poolStats []PoolStats

	cmdOp = strings.Replace(cmdOp, "RAW USED", "RAW_USED", 1)
	cmdOp = strings.Replace(cmdOp, "%RAW USED", "%RAW_USED", 1)
	cmdOp = strings.Replace(cmdOp, "MAX AVAIL", "MAX_AVAIL", 1)

	lines := strings.Split(cmdOp, "\n")
	if len(lines) < 3 {
		return stats, fmt.Errorf("Failed to parse cluster and pool stats from\n%s", cmdOp)
	}
	index := 0

	poolNameIndex := -1
	poolIdIndex := -1
	poolUsedIndex := -1
	poolPercentUsedIndex := -1
	poolMaxAvailIndex := -1
	processPoolStats := false

	if !strings.HasPrefix(cmdOp, "GLOBAL") {
		return stats, fmt.Errorf("Missing fields from cluster stats\n%s", cmdOp)
	}

	for index < len(lines) {
		line := lines[index]
		if line == "" || line == "\n" {
			if err_str != "" {
				err = fmt.Errorf("%s", err_str)
			}
			return stats, err
		}

		if strings.HasPrefix(line, "GLOBAL") {
			index = index + 1
			if index >= len(lines) {
				return stats, fmt.Errorf("%s.No cluster stats to parse from %s", err_str, cmdOp)
			}
			line := lines[index]
			clusterFields := strings.Fields(line)
			clusterSizeIndex := skyring_utils.StringIndexInSlice(clusterFields, "SIZE")
			clusterAvailableSizeIndex := skyring_utils.StringIndexInSlice(clusterFields, "AVAIL")
			clusterUsedSizeIndex := skyring_utils.StringIndexInSlice(clusterFields, "RAW_USED")
			clusterPercentUsedIndex := skyring_utils.StringIndexInSlice(clusterFields, "%RAW_USED")
			if clusterSizeIndex == -1 || clusterAvailableSizeIndex == -1 || clusterUsedSizeIndex == -1 || clusterPercentUsedIndex == -1 {
				return stats, fmt.Errorf("Missing fields in cluster stats\n%s", cmdOp)
			}
			index = index + 1
			if index >= len(lines) {
				return stats, fmt.Errorf("%s.No cluster stats to parse from %s", err_str, cmdOp)
			}
			line = lines[index]
			clusterFields = strings.Fields(line)
			if len(clusterFields) < 4 {
				return stats, fmt.Errorf("Missing fields in cluster stats\n%s", cmdOp)
			}
			stats.Stats.Total, err = GetStatInBytes(clusterFields[clusterSizeIndex])
			if err != nil {
				err_str = fmt.Sprintf("%s. Failed to fetch cluster total size. Error %s", err_str, err.Error())
			}
			stats.Stats.Used, err = GetStatInBytes(clusterFields[clusterUsedSizeIndex])
			if err != nil {
				err_str = fmt.Sprintf("%s. Failed to fetch cluster used stats.Error %s", err_str, err.Error())
			}
			stats.Stats.Available, err = GetStatInBytes(clusterFields[clusterAvailableSizeIndex])
			if err != nil {
				err_str = fmt.Sprintf("%s. Failed to fetch cluster available size.Error %s", err_str, err.Error())
			}
			stats.Stats.PercentUsed, err = strconv.ParseFloat(clusterFields[clusterPercentUsedIndex], 64)
			if err != nil {
				err_str = fmt.Sprintf("%s. Failed to fetch cluster percent used. Error %s", err_str, err.Error())
			}
		}
		if strings.HasPrefix(line, "POOLS") {
			processPoolStats = true
			index = index + 1
			if index >= len(lines) {
				if err_str != "" {
					return stats, fmt.Errorf("%s", err_str)
				}
				return stats, nil
			}
			line = lines[index]
			poolFields := strings.Fields(line)

			poolNameIndex = skyring_utils.StringIndexInSlice(poolFields, "NAME")
			poolIdIndex = skyring_utils.StringIndexInSlice(poolFields, "ID")
			poolUsedIndex = skyring_utils.StringIndexInSlice(poolFields, "USED")
			poolPercentUsedIndex = skyring_utils.StringIndexInSlice(poolFields, "%USED")
			poolMaxAvailIndex = skyring_utils.StringIndexInSlice(poolFields, "MAX_AVAIL")

			if poolNameIndex == -1 || poolIdIndex == -1 || poolUsedIndex == -1 || poolPercentUsedIndex == -1 || poolMaxAvailIndex == -1 {
				return stats, nil
			}
			index = index + 1
		}
		if processPoolStats {
			poolFetchSuccess := true
			if index >= len(lines) {
				if err_str != "" {
					return stats, fmt.Errorf("%s", err_str)
				}
				return stats, nil
			}
			line = lines[index]

			poolFields := strings.Fields(line)

			var poolStat PoolStats

			if len(poolFields) == 0 {
				continue
			}
			if len(poolFields) < 5 {
				err_str = fmt.Sprintf("Missing fields in pool stats\n %s", line)
				continue
			}
			poolStat.PoolUtilization.Available, err = GetStatInBytes(poolFields[poolMaxAvailIndex])
			if err != nil {
				poolFetchSuccess = false
				err_str = fmt.Sprintf("%s. Failed to fetch pool available size from %s. Error %s", err_str, line, err.Error())
			}
			poolStat.PoolUtilization.Used, err = GetStatInBytes(poolFields[poolUsedIndex])
			if err != nil {
				poolFetchSuccess = false
				err_str = fmt.Sprintf("%s. Failed to fetch pool used size from %s. Error %s", err_str, line, err.Error())
			}
			poolStat.PoolUtilization.PercentUsed, err = strconv.ParseFloat(strings.TrimSuffix(poolFields[poolPercentUsedIndex], "M"), 64)
			if err != nil {
				err_str = fmt.Sprintf("%s. Failed to fetch pool percent used from %s. Error %s", err_str, line, err.Error())
			}
			poolStat.Name = poolFields[poolNameIndex]
			poolStat.Id, err = strconv.Atoi(poolFields[poolIdIndex])
			if err != nil {
				poolFetchSuccess = false
				err_str = fmt.Sprintf("%s. Failed to fetch pool id from %s. Error %s", err_str, line, err.Error())
			}
			if poolFetchSuccess {
				poolStats = append(poolStats, poolStat)
				stats.Pools = poolStats
			}
		}
		index = index + 1
	}
	if err_str != "" {
		err = fmt.Errorf("%s", err_str)
	}
	return stats, err
}

func updateClusterStats(ctxt string, cluster models.Cluster, monName string) (map[string]map[string]string, ClusterStats, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig

	var statistics ClusterStats

	response, isSuccess := getStatsFromCalamariApi(ctxt,
		monName,
		cluster.ClusterId,
		map[string]string{"clusterName": cluster.Name},
		skyring_monitoring.CLUSTER_UTILIZATION)

	if !isSuccess {
		return nil, statistics, nil
	}
	var err error
	if statistics, err = parseClusterStats(response); err != nil {
		return nil, statistics, fmt.Errorf("Failed to fetch cluster stats from cluster %v.Could not unmarshal %v.Error %v", cluster.Name, response, err)
	}

	metrics := make(map[string]map[string]string)
	currentTimeStamp := time.Now().Unix()

	metric_name := fmt.Sprintf("%s.%s.%s.", monitoringConfig.CollectionName, strings.Replace(cluster.Name, ".", "_", -1), skyring_monitoring.CLUSTER_UTILIZATION)

	metrics[metric_name+skyring_monitoring.USED_SPACE] = map[string]string{strconv.FormatInt(currentTimeStamp, 10): fmt.Sprintf("%v", statistics.Stats.Used)}
	metrics[metric_name+skyring_monitoring.FREE_SPACE] = map[string]string{strconv.FormatInt(currentTimeStamp, 10): fmt.Sprintf("%v", statistics.Stats.Available)}
	metrics[metric_name+skyring_monitoring.TOTAL_SPACE] = map[string]string{strconv.FormatInt(currentTimeStamp, 10): fmt.Sprintf("%v", statistics.Stats.Total)}

	metrics[metric_name+skyring_monitoring.USAGE_PERCENTAGE] = map[string]string{strconv.FormatInt(currentTimeStamp, 10): fmt.Sprintf("%v", statistics.Stats.PercentUsed)}
	if err := updateDB(
		bson.M{"clusterid": cluster.ClusterId},
		bson.M{
			"$set": bson.M{
				"usage": models.Utilization{
					Used:        statistics.Stats.Used,
					Total:       statistics.Stats.Total,
					PercentUsed: statistics.Stats.PercentUsed,
					UpdatedAt:   time.Now().String()}}},
		models.COLL_NAME_STORAGE_CLUSTERS); err != nil {
		return metrics, statistics, fmt.Errorf("Updating the cluster statistics to db for the cluster %v failed.Error %v", cluster.Name, err.Error())
	}
	return metrics, statistics, nil
}

func updateStatsToPools(ctxt string, statistics ClusterStats, clusterId uuid.UUID) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	collection := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
	for _, poolStat := range statistics.Pools {
		used := poolStat.PoolUtilization.Used
		percentUsed := poolStat.PoolUtilization.PercentUsed
		total := int64(float64(poolStat.PoolUtilization.Available) / float64(0.01*(100-percentUsed)))
		dbUpdateError := collection.Update(bson.M{"clusterid": clusterId, "name": poolStat.Name}, bson.M{"$set": bson.M{"usage": models.Utilization{
			Used:        used,
			Total:       total,
			PercentUsed: percentUsed,
			UpdatedAt:   time.Now().String()}}})
		if dbUpdateError != nil {
			logger.Get().Error("%s-Failed to update utilization of pool %v of cluster %v to db.Error %v", ctxt, poolStat.Name, clusterId, dbUpdateError)
		}
	}

}

func FetchClusterStats(ctxt string, cluster models.Cluster, monName string) (map[string]map[string]string, error) {
	metrics, statistics, err := updateClusterStats(ctxt, cluster, monName)
	percentUsed := 0.0
	if err != nil {
		logger.Get().Error("%s - Failed to fetch cluster usage statistics.Error %v", ctxt, err)
	}
	if statistics.Stats.Total != 0 {
		percentUsed = (float64(statistics.Stats.Used*100) / float64(statistics.Stats.Total))
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
	updateStatsToPools(ctxt, statistics, cluster.ClusterId)
	for _, poolStat := range statistics.Pools {
		percentUsed := poolStat.PoolUtilization.PercentUsed
		total := int64(float64(poolStat.PoolUtilization.Available) / float64(0.01*(100-percentUsed)))
		if total != 0 {
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
	metric_name := monitoringConfig.CollectionName + "." + strings.Replace(cluster.Name, ".", "_", -1) + "." + skyring_monitoring.NO_OF_OBJECT
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
			metric_name := fmt.Sprintf("%s.%s.%s.pool-%s.%s", monitoringConfig.CollectionName, strings.Replace(cluster.Name, ".", "_", -1), skyring_monitoring.PG_SUMMARY, k, k1)
			statMap := make(map[string]string)
			statMap[strconv.FormatInt(currentTimeStamp, 10)] = strconv.FormatUint(v1, 10)
			metrics[metric_name] = statMap
		}
	}
	for k, v := range statistics.All {
		k = strings.Replace(k, "+", "_", -1)
		metric_name := fmt.Sprintf("%s.%s.%s.all.%s", monitoringConfig.CollectionName, strings.Replace(cluster.Name, ".", "_", -1), skyring_monitoring.PG_SUMMARY, k)
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
