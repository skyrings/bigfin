package provider

import (
	"fmt"
	"github.com/skyrings/bigfin/conf"
	"github.com/skyrings/bigfin/tools/logger"
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/skyring/db"
	"github.com/skyrings/skyring/models"
	skyring_monitoring "github.com/skyrings/skyring/monitoring"
	"github.com/skyrings/skyring/tools/uuid"
	"gopkg.in/mgo.v2/bson"
	"net/http"
	"strconv"
	"time"
)

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

func (s *CephProvider) MonitorCluster(req models.RpcRequest, resp *models.RpcResponse) error {
	cluster_id_str, ok := req.RpcRequestVars["cluster-id"]
	if !ok {
		logger.Get().Error("Incorrect cluster id: %s", cluster_id_str)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Incorrect cluster id: %s", cluster_id_str))
		return fmt.Errorf("Incorrect cluster id: %s", cluster_id_str)
	}
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("Error parsing the cluster id: %s. Error: %v", cluster_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s.Error: %v", cluster_id_str, err))
		return err
	}

	go FetchClusterStats(*cluster_id)

	*resp = utils.WriteResponseWithData(http.StatusOK, "", []byte{})
	return nil
}

func FetchClusterStats(cluster_id uuid.UUID) {
	monitoringConfig := conf.SystemConfig.TimeSeriesDBConfig
	cluster, clusterFetchErr := getCluster(cluster_id)
	if clusterFetchErr != nil {
		logger.Get().Error("Unbale to parse the request %v", clusterFetchErr.Error())
		return
	}

	monnode, monNodeFetchErr := GetRandomMon(cluster_id)
	if monNodeFetchErr != nil {
		logger.Get().Error("Unbale to pick a random mon from cluster %v.Error: %v", cluster.Name, monNodeFetchErr)
		return
	}

	statistics, statsFetchErr := salt_backend.GetClusterStats(monnode.Hostname, cluster.Name)
	if statsFetchErr != nil {
		logger.Get().Error("Unable to fetch cluster stats from mon %v of cluster %v.Error: %v", monnode.Hostname, cluster.Name, statsFetchErr)
		return
	}

	metrics := make(map[string]map[string]string)
	currentTimeStamp := time.Now().Unix()
	for statType, value := range statistics {
		metric_name := monitoringConfig.CollectionName + "." + cluster.Name + "." + "cluster_utilization" + "." + statType
		statMap := make(map[string]string)
		statMap[strconv.FormatInt(currentTimeStamp, 10)] = strconv.FormatInt(value, 10)
		metrics[metric_name] = statMap
	}

	if MonitoringManager == nil {
		logger.Get().Warning("Monitoring manager was not initialized successfully")
		return
	}
	if err := MonitoringManager.PushToDb(metrics, monitoringConfig.Hostname, monitoringConfig.DataPushPort); err != nil {
		logger.Get().Error("Failed to push statistics of cluster %v to db.Error: %v", cluster.Name, err)
	}
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
