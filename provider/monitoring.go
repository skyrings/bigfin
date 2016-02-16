package provider

import (
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

				percent_used := (float64(spUsed) * 100) / (float64(spUsed + spFree))
				statsToPush[metric_name+skyring_monitoring.USAGE_PERCENT] = map[string]string{currentTimeStamp: fmt.Sprintf("%v", percent_used)}
			}
		}
	}
	return statsToPush, nil
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
	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig
	statistics, statsFetchErr := salt_backend.GetClusterStats(monName, cluster.Name)
	if statsFetchErr != nil {
		return nil, fmt.Errorf("Unable to fetch cluster stats from mon %v of cluster %v.Error: %v", monName, cluster.Name, statsFetchErr)
	}

	metrics := make(map[string]map[string]string)
	currentTimeStamp := time.Now().Unix()
	for statType, value := range statistics {
		metric_name := monitoringConfig.CollectionName + "." + cluster.Name + "." + skyring_monitoring.CLUSTER_UTILIZATION + "." + statType
		statMap := make(map[string]string)
		statMap[strconv.FormatInt(currentTimeStamp, 10)] = strconv.FormatInt(value, 10)
		metrics[metric_name] = statMap
	}
	return metrics, nil
}

func FetchObjectCount() (map[string]map[string]string, error) {
	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig
	statistics, statsFetchErr := salt_backend.GetObjectCount(monName, cluster.Name)
	if statsFetchErr != nil {
		return nil, fmt.Errorf("Unable to fetch object count from mon %v of cluster %v.Error: %v", monName, cluster.Name, statsFetchErr)
	}
	metrics := make(map[string]map[string]string)
	currentTimeStamp := time.Now().Unix()
	timeStampStr := strconv.FormatInt(currentTimeStamp, 10)
	metric_name := monitoringConfig.CollectionName + "." + cluster.Name + "." + skyring_monitoring.NO_OF_OBJECT
	metrics[metric_name] = map[string]string{timeStampStr: statistics}
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
