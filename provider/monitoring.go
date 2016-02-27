package provider

import (
	"encoding/json"
	"fmt"
	"github.com/skyrings/bigfin/backend"
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

const (
	monitor = "monitor"
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

func initMonitoringRoutines() error {
	/*
		sync.WaitGroup can be used if it is required to do something after all go routines complete.
		For ex: pushing to db can be done after all go-routines complete.
	*/
	var wg sync.WaitGroup
	var errors string
	wg.Add(len(monitoringRoutines))
	for _, iFunc := range monitoringRoutines {
		if function, ok := iFunc.(func() (map[string]map[string]string, error)); ok {
			go func() {
				defer wg.Done()
				response, err := function()
				if err != nil {
					logger.Get().Error(err.Error())
					errors = errors + err.Error()
				}
				if response != nil {
					/*
					 The stats are being pushed as they are obtained because each of them can potentially have different intervals of schedule
					 in which case the respective routines would sleep for the said interval making the push to wait longer.
					*/
					err := pushTimeSeriesData(response)
					if err != nil {
						logger.Get().Error(err.Error())
						errors = errors + err.Error()
					}
				}
			}()
		} else {
			continue
		}
	}

	wg.Wait()
	return nil
}

func (s *CephProvider) MonitorCluster(req models.RpcRequest, resp *models.RpcResponse) error {
	cluster_id_str, ok := req.RpcRequestVars["cluster-id"]
	var monnode *models.Node
	if !ok {
		logger.Get().Error("Incorrect cluster id: %s", cluster_id_str)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Incorrect cluster id: %s", cluster_id_str))
		return fmt.Errorf("Incorrect cluster id: %s", cluster_id_str)
	}
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("Error parsing the cluster id: %s. Error: %v", cluster_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s.Error: %v", cluster_id_str, err))
		return fmt.Errorf("Error parsing the cluster id: %s.Error: %v", cluster_id_str, err)
	}
	cluster, err = getCluster(*cluster_id)
	if err != nil {
		logger.Get().Error("Unable to get cluster with id %v.Err %v", cluster_id, err.Error())
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unable to get cluster with id %v.Err %v", cluster_id, err.Error()))
		return fmt.Errorf("Unable to get cluster with id %v.Err %v", cluster_id, err.Error())
	}
	monnode, err = GetRandomMon(*cluster_id)
	if err != nil {
		logger.Get().Error("Unable to pick a random mon from cluster %v.Error: %v", cluster.Name, err.Error())
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unable to pick a random mon from cluster %v.Error: %v", cluster.Name, err.Error()))
		return fmt.Errorf("Unable to pick a random mon from cluster %v.Error: %v", cluster.Name, err.Error())
	}
	monName = (*monnode).Hostname
	nodes, err = getClusterNodesById(cluster_id)
	if err != nil {
		logger.Get().Error("Unable to fetch nodes of cluster %v.Error: %v", cluster.Name, err.Error())
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unable to fetch nodes of cluster %v.Error: %v", cluster.Name, err.Error()))
		return fmt.Errorf("Unable to fetch nodes of cluster %v.Error: %v", cluster.Name, err.Error())
	}
	err = initMonitoringRoutines()
	if err != nil {
		logger.Get().Error("Error: %v", err.Error())
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error: %v", err.Error()))
		return fmt.Errorf("Error: %v", err.Error())
	}
	*resp = utils.WriteResponseWithData(http.StatusOK, "", []byte{})
	return nil
}

func (s *CephProvider) GetSummary(req models.RpcRequest, resp *models.RpcResponse) error {
	result := make(map[string]interface{})
	httpStatusCode := http.StatusOK

	mons, monErr := GetMons(nil)
	var err_str string
	if monErr != nil {
		err_str = fmt.Sprintf("Unable to fetch monitor nodes.Error %v", monErr.Error())
		logger.Get().Error("Unable to fetch monitor nodes.Error %v", monErr.Error())
	} else {
		result[monitor] = len(mons)
	}

	if err_str != "" {
		if len(result) != 0 {
			httpStatusCode = http.StatusPartialContent
		} else {
			httpStatusCode = http.StatusInternalServerError
		}
	}
	bytes, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		*resp = utils.WriteResponseWithData(http.StatusInternalServerError, err_str, []byte{})
		return fmt.Errorf("Failed to marshal %v.Error %v", result, marshalErr)
	}
	*resp = utils.WriteResponseWithData(httpStatusCode, err_str, bytes)
	return fmt.Errorf("%v", err_str)
}

func FetchOSDStats() (map[string]map[string]string, error) {
	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig
	statistics, statsFetchErr := salt_backend.GetOSDDetails(monName, cluster.Name)
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

				cluster.StorageProfileUsage[slu.StorageProfile] = models.Utilization{Used: int64(spUsed), Total: int64(spUsed + spFree)}

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

func FetchClusterStats() (map[string]map[string]string, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig
	statistics, statsFetchErr := salt_backend.GetClusterStats(monName, cluster.Name)
	if statsFetchErr != nil {
		return nil, fmt.Errorf("Unable to fetch cluster stats from mon %v of cluster %v.Error: %v", monName, cluster.Name, statsFetchErr)
	}

	metrics := make(map[string]map[string]string)
	currentTimeStamp := time.Now().Unix()

	table_name := monitoringConfig.CollectionName + "." + cluster.Name + "." + skyring_monitoring.CLUSTER_UTILIZATION + "."

	metrics[table_name+skyring_monitoring.USED_SPACE] = map[string]string{strconv.FormatInt(currentTimeStamp, 10): strconv.FormatInt(statistics.ClusterStats.Used, 10)}
	metrics[table_name+skyring_monitoring.FREE_SPACE] = map[string]string{strconv.FormatInt(currentTimeStamp, 10): strconv.FormatInt(statistics.ClusterStats.Available, 10)}
	metrics[table_name+skyring_monitoring.TOTAL_SPACE] = map[string]string{strconv.FormatInt(currentTimeStamp, 10): strconv.FormatInt(statistics.ClusterStats.Total, 10)}

	if err := updateCluster(bson.M{"clusterid": cluster.ClusterId}, bson.M{"$set": bson.M{"usage": models.Utilization{Used: statistics.ClusterStats.Used, Total: statistics.ClusterStats.Total}}}); err != nil {
		logger.Get().Error("Updating the cluster statistics to db for the cluster %v failed.Error %v", cluster.Name, err.Error())
	}

	collection := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE)
	for _, poolStat := range statistics.PoolUtilizations {
		dbUpdateError := collection.Update(bson.M{"clusterid": cluster.ClusterId, "id": strconv.Itoa(poolStat.Id)}, bson.M{"usage": models.Utilization{Used: poolStat.Utilization.Used, Total: poolStat.Utilization.Used + poolStat.Utilization.Available}})
		if dbUpdateError != nil {
			logger.Get().Error("Failed to update utilization of pool %v of cluster %v to db.Error %v", poolStat.Name, cluster.Name, dbUpdateError)
		}
	}

	return metrics, nil
}

func FetchObjectCount() (map[string]map[string]string, error) {
	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig
	statistics, statsFetchErr := salt_backend.GetObjectCount(monName, cluster.Name)
	if statsFetchErr != nil {
		return nil, fmt.Errorf("Unable to fetch object count from mon %v of cluster %v.Error: %v", monName, cluster.Name, statsFetchErr)
	}

	object_cnt, cerr := strconv.ParseUint(statistics, 10, 64)
	if cerr != nil {
		logger.Get().Error("Inalid object count %v. Error %v", statistics, cerr)
		return nil, fmt.Errorf("Inalid object count %v. Error %v", statistics, cerr)
	}

	metrics := make(map[string]map[string]string)
	currentTimeStamp := time.Now().Unix()
	timeStampStr := strconv.FormatInt(currentTimeStamp, 10)
	metric_name := monitoringConfig.CollectionName + "." + cluster.Name + "." + skyring_monitoring.NO_OF_OBJECT
	metrics[metric_name] = map[string]string{timeStampStr: statistics}

	if err := updateCluster(bson.M{"clusterid": cluster.ClusterId}, bson.M{"$set": bson.M{"objectcnt": object_cnt}}); err != nil {
		logger.Get().Error("Updating the object count to db for the cluster %v failed.Error %v", cluster.Name, err.Error())
	}

	return metrics, nil
}

func FetchPGSummary() (map[string]map[string]string, error) {
	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig
	statistics, statsFetchErr := cephapi_backend.GetPGSummary(monName, cluster.ClusterId)
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

func getCluster(cluster_id uuid.UUID) (cluster models.Cluster, err error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	collection := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := collection.Find(bson.M{"clusterid": cluster_id}).One(&cluster); err != nil {
		return cluster, err
	}
	return cluster, nil
}
