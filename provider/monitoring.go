package provider

import (
	"encoding/json"
	"fmt"
	"github.com/marpaia/graphite-golang"
	"github.com/skyrings/bigfin/tools/logger"
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/skyring/conf"
	"github.com/skyrings/skyring/db"
	"github.com/skyrings/skyring/models"
	"github.com/skyrings/skyring/tools/uuid"
	"gopkg.in/mgo.v2/bson"
	"net/http"
)

func (s *CephProvider) MonitorCluster(req models.RpcRequest, resp *models.RpcResponse) error {
	logger.Get().Error("In MonitorCluster in BigFin")

	cluster_id_str := req.RpcRequestVars["cluster-id"]
	logger.Get().Error("Cluster-id in bigfin monitoring is :%v", cluster_id_str)
	cluster_id, err := uuid.Parse(cluster_id_str)
	if err != nil {
		logger.Get().Error("Error parsing the cluster id: %s", cluster_id_str)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the cluster id: %s", cluster_id_str))
		return err
	}

	cluster, clusterFetchErr := getCluster(cluster_id)
	if clusterFetchErr != nil {
		logger.Get().Error("Unbale to parse the request %v", clusterFetchErr.Error())
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to parse the request %v", clusterFetchErr.Error()))
		return clusterFetchErr
	}

	logger.Get().Error("Cluster in MonitoringCluster is %v", cluster)

	metric_name := "skyring." + cluster.Name + "." + "utilization"
	gMetric := []graphite.Metric{{Name: metric_name, Value: "10"}}

	result, marshalErr := json.Marshal(gMetric)
	if marshalErr != nil {
		logger.Get().Error("Unbale to marshal the response %v", marshalErr.Error())
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Unbale to marshal the response %v", marshalErr.Error()))
		return marshalErr
	}
	logger.Get().Error("The result is %v\n The marshalled request is %v\n", gMetric, result)
	*resp = utils.WriteResponseWithData(http.StatusOK, "", result)
	return nil
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
