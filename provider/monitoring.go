package provider

import (
	"encoding/json"
	"fmt"
	"github.com/skyrings/bigfin/backend"
	bigfin_models "github.com/skyrings/bigfin/bigfinmodels"
	"github.com/skyrings/bigfin/conf"
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/skyring-common/db"
	"github.com/skyrings/skyring-common/models"
	skyring_monitoring "github.com/skyrings/skyring-common/monitoring"
	"github.com/skyrings/skyring-common/tools/logger"
	"github.com/skyrings/skyring-common/tools/uuid"
	"gopkg.in/mgo.v2/bson"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	MonitoringManager skyring_monitoring.MonitoringManagerInterface
	cluster           models.Cluster
	nodes             models.Nodes
	monName           string
)

var monitoringRoutines = []interface{}{
	FetchOSDStats,
	FetchClusterStats,
	FetchObjectCount,
	FetchPGSummary,
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

func initMonitoringRoutines(ctxt string) error {
	/*
		sync.WaitGroup can be used if it is required to do something after all go routines complete.
		For ex: pushing to db can be done after all go-routines complete.
	*/
	var wg sync.WaitGroup
	var errors string
	wg.Add(len(monitoringRoutines))
	for _, iFunc := range monitoringRoutines {
		if function, ok := iFunc.(func(string) (map[string]map[string]string, error)); ok {
			go func(ctxt string) {
				defer wg.Done()
				response, err := function(ctxt)
				if err != nil {
					logger.Get().Error("%s-%v", ctxt, err.Error())
					errors = errors + err.Error()
				}
				if response != nil {
					/*
					 The stats are being pushed as they are obtained because each of them can potentially have different intervals of schedule
					 in which case the respective routines would sleep for the said interval making the push to wait longer.
					*/
					err := pushTimeSeriesData(response)
					if err != nil {
						logger.Get().Error("%s-%v", ctxt, err.Error())
						errors = errors + err.Error()
					}
				}
			}(ctxt)
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
	cluster, err = getCluster(*cluster_id)
	if err != nil {
		logger.Get().Error("%s-Unable to get cluster with id %v.Err %v", ctxt, cluster_id, err.Error())
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unable to get cluster with id %v.Err %v", cluster_id, err.Error()))
		return fmt.Errorf("Unable to get cluster with id %v.Err %v", cluster_id, err.Error())
	}
	monnode, err = GetRandomMon(*cluster_id)
	if err != nil {
		logger.Get().Error("%s-Unable to pick a random mon from cluster %v.Error: %v", ctxt, cluster.Name, err.Error())
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unable to pick a random mon from cluster %v.Error: %v", cluster.Name, err.Error()))
		return fmt.Errorf("Unable to pick a random mon from cluster %v.Error: %v", cluster.Name, err.Error())
	}
	monName = (*monnode).Hostname
	nodes, err = getClusterNodesById(cluster_id)
	if err != nil {
		logger.Get().Error("%s-Unable to fetch nodes of cluster %v.Error: %v", ctxt, cluster.Name, err.Error())
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unable to fetch nodes of cluster %v.Error: %v", cluster.Name, err.Error()))
		return fmt.Errorf("Unable to fetch nodes of cluster %v.Error: %v", cluster.Name, err.Error())
	}
	err = initMonitoringRoutines(ctxt)
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
	mon, monErr := GetRandomMon(*clusterId)
	if monErr != nil {
		err_str = err_str + fmt.Sprintf("%s - Failed to get the mon from cluster %v", ctxt, *clusterId)
	} else {
		pgCount, pgCountError := cephapi_backend.GetPGCount((*mon).Hostname, *clusterId, ctxt)
		if pgCountError != nil {
			err_str = err_str + fmt.Sprintf("%s - Failed to fetch the number of pgs from cluster %v", ctxt, *clusterId)
		} else {
			for status, statusCount := range pgCount {
				if status == "critical" {
					pgNum[models.STATUS_ERR] = statusCount
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
	}
	bytes, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		logger.Get().Error("%s - Failed to marshal %v.Error %v", ctxt, result, marshalErr)
		*resp = utils.WriteResponseWithData(http.StatusInternalServerError, err_str, []byte{})
		return fmt.Errorf("%s - Failed to marshal %v.Error %v", ctxt, result, marshalErr)
	}
	*resp = utils.WriteResponseWithData(httpStatusCode, fmt.Sprintf("%s-%s", ctxt, err_str), bytes)
	return fmt.Errorf("%s-%v", ctxt, err_str)
}

func (s *CephProvider) GetSummary(req models.RpcRequest, resp *models.RpcResponse) error {
	ctxt := req.RpcRequestContext

	result := make(map[string]interface{})
	httpStatusCode := http.StatusOK

	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	mons, monErr := GetMons(nil)
	var err_str string
	if monErr != nil {
		err_str = fmt.Sprintf("Unable to fetch monitor nodes.Error %v", monErr.Error())
		logger.Get().Error("%s - Unable to fetch monitor nodes.Error %v", ctxt, monErr.Error())
	} else {
		result[models.Monitor] = len(mons)
	}

	clusters, clustersFetchErr := getClustersByType(bigfin_models.CLUSTER_TYPE)
	if clustersFetchErr != nil {
		logger.Get().Error("%s - Unable to fetch clusters of type %v. Err %v", ctxt, bigfin_models.CLUSTER_TYPE, clustersFetchErr)
		return fmt.Errorf("%s - Unable to fetch clusters of type %v. Err %v", ctxt, bigfin_models.CLUSTER_TYPE, clustersFetchErr)
	}

	var objectCount int64
	var degradedObjectCount int64

	/*
		Fetch pg count
	*/
	collection := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
	var storages models.Storages
	if sorageFetchErr := collection.Find(nil).All(&storages); sorageFetchErr != nil {
		err_str = fmt.Sprintf("Error getting the storage list error: %v", sorageFetchErr)
		logger.Get().Error("%s - Error getting the storage list error: %v", ctxt, sorageFetchErr)
	} else {
		var pg_count uint64
		for _, storage := range storages {
			if pgnum, ok := storage.Options["pgnum"]; ok {
				val, _ := strconv.ParseUint(pgnum, 10, 64)
				pg_count = pg_count + val
			}
		}
		result["pgnum"] = pg_count
	}

	for _, cluster := range clusters {
		/*
			Fetch object count
		*/
		objectCount = objectCount + cluster.ObjectCount[bigfin_models.NUMBER_OF_OBJECTS]
		degradedObjectCount = degradedObjectCount + cluster.ObjectCount[bigfin_models.NUMBER_OF_DEGRADED_OBJECTS]
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
	return fmt.Errorf("%v", err_str)
}

func FetchOSDStats(ctxt string) (map[string]map[string]string, error) {
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
	}

	storageProfileStats, storageProfileStatsErr := FetchStorageProfileUtilizations(statistics)
	if storageProfileStatsErr != nil {
		return metrics, fmt.Errorf("Failed to fetch storage profile utilizations for cluster %v.Error %v", cluster.Name, storageProfileStatsErr)
	}
	for key, timeStampValueMap := range storageProfileStats {
		metrics[key] = timeStampValueMap
	}
	return metrics, nil
}

func FetchStorageProfileUtilizations(osdDetails []backend.OSDDetails) (statsToPush map[string]map[string]string, err error) {
	statsToPush = make(map[string]map[string]string)
	cluster.StorageProfileUsage = make(map[string]models.Utilization)
	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig
	slus, sluFetchErr := getOsds(cluster.ClusterId)
	if sluFetchErr != nil {
		return nil, sluFetchErr
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
						return nil, fmt.Errorf("Error fetching osd stats for cluster %v. Error %v", cluster.Name, err)
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
						return nil, fmt.Errorf("Error fetching osd stats for cluster %v. Error %v", cluster.Name, err)
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
			}
		}
	}

	if err := updateCluster(bson.M{"clusterid": cluster.ClusterId}, bson.M{"$set": bson.M{"storageprofileusage": cluster.StorageProfileUsage}}); err != nil {
		logger.Get().Error("Updating the storage profile statistics to db for the cluster %v failed.Error %v", cluster.Name, err.Error())
	}

	return statsToPush, nil
}

func updateCluster(query interface{}, update interface{}) error {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
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
	if err := coll.Find(bson.M{"clusterid": cluster.ClusterId}).All(&slus); err != nil {
		return nil, fmt.Errorf("Error getting the slus for cluster: %v. error: %v", cluster.Name, err)
	}
	return slus, nil
}

func FetchClusterStats(ctxt string) (map[string]map[string]string, error) {
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
	if err := updateCluster(bson.M{"clusterid": cluster.ClusterId}, bson.M{"$set": bson.M{"usage": models.Utilization{Used: statistics.Used, Total: statistics.Total, PercentUsed: percentUsed}}}); err != nil {
		logger.Get().Error("%s-Updating the cluster statistics to db for the cluster %v failed.Error %v", ctxt, cluster.Name, err.Error())
	}

	collection := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
	for _, poolStat := range statistics.Pools {
		used := poolStat.Used
		total := poolStat.Used + poolStat.Available
		percentUsed := 0.0
		if total != 0 {
			percentUsed = (float64(used*100) / float64(total))
		}
		dbUpdateError := collection.Update(bson.M{"clusterid": cluster.ClusterId, "name": poolStat.Name}, bson.M{"$set": bson.M{"usage": models.Utilization{Used: used, Total: total, PercentUsed: percentUsed}}})
		if dbUpdateError != nil {
			logger.Get().Error("%s-Failed to update utilization of pool %v of cluster %v to db.Error %v", ctxt, poolStat.Name, cluster.Name, dbUpdateError)
		}
	}

	return metrics, nil
}

func FetchObjectCount(ctxt string) (map[string]map[string]string, error) {
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

	if err := updateCluster(bson.M{"clusterid": cluster.ClusterId}, bson.M{"$set": bson.M{"objectcount": map[string]int64{bigfin_models.NUMBER_OF_OBJECTS: objectCnt, bigfin_models.NUMBER_OF_DEGRADED_OBJECTS: objectErrCnt}}}); err != nil {
		logger.Get().Error("%s-Updating the object count to db for the cluster %v failed.Error %v", ctxt, cluster.Name, err.Error())
	}

	return metrics, nil
}

func FetchPGSummary(ctxt string) (map[string]map[string]string, error) {
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
	return metrics, nil
}

func pushTimeSeriesData(metrics map[string]map[string]string) error {
	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig
	if metrics == nil {
		return fmt.Errorf("The metrics are nil")
	}
	if MonitoringManager == nil {
		return fmt.Errorf("Monitoring manager was not initialized successfully")
	}
	if err := MonitoringManager.PushToDb(metrics, monitoringConfig.Hostname, monitoringConfig.DataPushPort); err != nil {
		return fmt.Errorf("Failed to push statistics of cluster %v to db.Error: %v", cluster.Name, err)
	}
	return nil
}

func getClusterNodesById(cluster_id *uuid.UUID) (models.Nodes, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	collection := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	var nodes models.Nodes
	if err := collection.Find(bson.M{"clusterid": *cluster_id}).All(&nodes); err != nil {
		return nil, err
	}
	return nodes, nil
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
