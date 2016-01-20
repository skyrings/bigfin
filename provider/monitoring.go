package provider

import (
	"encoding/json"
	"github.com/marpaia/graphite-golang"
	"github.com/skyrings/skyring/models"
	"github.com/skyrings/skyring/tools/logger"
	"github.com/skyrings/skyring/tools/uuid"
)

func (s *CephProvider) MonitorCluster(req models.RpcRequest, resp *models.RpcResponse) (interface{}, error) {
	logger.Get().Error("In MonitorCluster in BigFin")

	cluster_id_str := req.RpcRequestVars["cluster-id"]
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("Error parsing the cluster id: %s", cluster_id_str)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s", cluster_id_str))
		return err
	}

	cluster, clusterFetchErr := getCluster(cluster_id)
	if clusterFetchErr != nil {
		logger.Get().Error("Unbale to parse the request %v", err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request %v", err))
		return nil, err
	}

	logger.Get().Error("Cluster in MonitoringCluster is %v", clusterFetchErr)

	metric_name = "skyring." + cluster.Name + "." + "utilization"
	gMetric := []graphite.Metric{{Name: metric_name, Value: "10"}}

	utils.WriteResponseWithData(http.StatusOK, "", json.Marshal(gMetric))
	return gMetric, nil
}

func getCluster(cluster_id *uuid.UUID) (cluster models.Cluster, err error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	collection := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := collection.Find(bson.M{"clusterid": *cluster_id}).One(&cluster); err != nil {
		return cluster, err
	}
	return cluster, nil
}
