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

package main

import (
	"encoding/json"
	"github.com/natefinch/pie"
	"github.com/skyrings/skyring/conf"
	"github.com/skyrings/skyring/db"
	"github.com/skyrings/skyring/models"
	"github.com/skyrings/skyring/tools/uuid"
	"gopkg.in/mgo.v2/bson"
	"log"
	"net/http"
	"net/rpc/jsonrpc"
)

const (
	CLUSTER_STATUS_UP   = "up"
	CLUSTER_STATUS_DOWN = "down"
)

func main() {
	log.SetPrefix("[cep provider log] ")
	p := pie.NewProvider()
	if err := p.RegisterName("Plugin", CephProvider{}); err != nil {
		log.Fatalf("Failed to register plugin: %s", err)
	}
	p.ServeCodec(jsonrpc.NewServerCodec)
}

type CephProvider struct {}

func (s *CephProvider) CreateCluster(args models.Args, resp *[]byte) {
	var request models.StorageCluster
	
	if err := json.Unmarshal(args.Request, &request); err != nil {
		*resp = writeResponse(http.StatusBadRequest, "Unbale to parse the request")
		return
	}
	
	// Invoke the cluster create backend

	// If success, persist the details in DB
	cluster_uuid, _ := uuid.New()
	request.ClusterId = *cluster_uuid
	request.ClusterStatus = CLUSTER_STATUS_UP
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()

	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Insert(request); err != nil {
		*resp = writeResponse(http.StatusInternalServerError, err.Error())
		return
	}

	for _, node := range request.Nodes {
		coll = sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
		if err := coll.Update(bson.M{"publicip": node.PublicIPAddress}, bson.M{"$set": bson.M{"clusterid": request.ClusterId, "managedstate": models.NODE_STATE_USED}}); err != nil {
			*resp = writeResponse(http.StatusInternalServerError, err.Error())
			return
		}
	}

	// Send response
	*resp = writeResponse(http.StatusOK, "Done")
}

func writeResponse(code int, msg string) []byte {
	var response models.RpcResponse
	response.Status.StatusCode = code
	response.Status.StatusMessage = msg
	ret_str, _ := json.Marshal(response)
	return ret_str
}

