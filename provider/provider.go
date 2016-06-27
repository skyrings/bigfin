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
	"errors"
	"fmt"
	"github.com/skyrings/bigfin/backend/cephapi"
	"github.com/skyrings/bigfin/backend/cephapi/handler"
	cephapi_models "github.com/skyrings/bigfin/backend/cephapi/models"
	"github.com/skyrings/bigfin/backend/salt"
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/skyring-common/conf"
	"github.com/skyrings/skyring-common/db"
	"github.com/skyrings/skyring-common/dbprovider"
	"github.com/skyrings/skyring-common/models"
	"github.com/skyrings/skyring-common/provisioner"
	"github.com/skyrings/skyring-common/tools/logger"
	"github.com/skyrings/skyring-common/tools/uuid"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"

	bigfin_conf "github.com/skyrings/bigfin/conf"
)

var (
	salt_backend      = salt.New()
	cephapi_backend   = cephapi.New()
	EventTypes        map[string]string
	DbManager         dbprovider.DbInterface
	installer_backend provisioner.Provisioner
)

type CephProvider struct{}

func GetRandomMon(clusterId uuid.UUID) (*models.Node, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	var mons models.Nodes
	var clusterNodes models.Nodes
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	if err := coll.Find(bson.M{"clusterid": clusterId}).All(&clusterNodes); err != nil {
		return nil, err
	}
	for _, clusterNode := range clusterNodes {
		for k, v := range clusterNode.Options {
			if k == "mon" && v == "Y" {
				mons = append(mons, clusterNode)
			}
		}
	}
	if len(mons) <= 0 {
		return nil, errors.New(fmt.Sprintf("No mons available for cluster: %v", clusterId))
	}

	// Pick a random mon from the list
	var monNodeId uuid.UUID
	if len(mons) == 1 {
		monNodeId = mons[0].NodeId
	} else {
		randomIndex := utils.RandomNum(0, len(mons)-1)
		monNodeId = mons[randomIndex].NodeId
	}
	var monnode models.Node
	coll = sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	if err := coll.Find(bson.M{"nodeid": monNodeId}).One(&monnode); err != nil {
		return nil, err
	}
	return &monnode, nil
}

func GetCalamariMonNode(clusterId uuid.UUID, ctxt string) (*models.Node, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	var calamariMonNode models.Node
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	err := coll.Find(
		bson.M{
			"clusterid":        clusterId,
			"options.mon":      models.Yes,
			"options.calamari": models.Yes}).One(&calamariMonNode)
	if err == nil {
		// Check availability of calamari
		dummyUrl := fmt.Sprintf(
			"https://%s:%d/%s/v%d/",
			calamariMonNode.Hostname,
			cephapi_models.CEPH_API_PORT,
			cephapi_models.CEPH_API_DEFAULT_PREFIX,
			cephapi_models.CEPH_API_DEFAULT_VERSION)
		resp, err := handler.HttpGet(calamariMonNode.Hostname, dummyUrl)
		if resp != nil {
			resp.Body.Close()
		}
		if err != nil {
			// Not a valid calamari. start another one
			if err := coll.Update(
				bson.M{"clusterid": clusterId, "hostname": calamariMonNode.Hostname},
				bson.M{"$set": bson.M{"options.calamari": "N"}}); err != nil {
				return nil, fmt.Errorf("Error disabling invalid calamari node: %s. error: %v", calamariMonNode.Hostname, err)
			}
		} else {
			return &calamariMonNode, nil
		}
	} else if err != mgo.ErrNotFound {
		return nil, err
	}

	var monNodes models.Nodes
	if err := coll.Find(
		bson.M{
			"clusterid":        clusterId,
			"options.mon":      models.Yes,
			"options.calamari": "N"}).All(&monNodes); err != nil {
		return nil, err
	}
	logger.Get().Error("Mon length: %d", len(monNodes))

	for _, monNode := range monNodes {
		if err := salt_backend.StartCalamari(monNode.Hostname, ctxt); err != nil {
			logger.Get().Warning(
				"%s-Could not start calamari on mon: %s. error: %v",
				ctxt,
				monNode.Hostname,
				err)
			continue
		}
		// Wait for calamari to start serving
		time.Sleep(60 * time.Second)
		if err := coll.Update(
			bson.M{"clusterid": clusterId, "hostname": monNode.Hostname},
			bson.M{"$set": bson.M{
				"options.calamari": "Y"}}); err != nil {
			logger.Get().Warning(
				"%s-Failed to update mon node: %s status as calamari",
				ctxt,
				monNode.Hostname)
			// Stop calamari on the node and try on other one
			if err := salt_backend.StopCalamari(monNode.Hostname, ctxt); err != nil {
				logger.Get().Warning(
					"%s-Could not stop calamari on mon node: %s. error: %v",
					ctxt,
					monNode.Hostname,
					err)
			}
			continue
		}
		return &monNode, nil
	}

	return nil, fmt.Errorf("No valid active calamari mon node found")
}

func InitializeDb() error {
	if mgr, err := dbprovider.InitDbProvider("mongodbprovider", ""); err != nil {
		logger.Get().Error("Error Initializing the Db Provider: %s", err)
		return err
	} else {
		DbManager = mgr
	}
	if err := DbManager.InitDb(); err != nil {
		logger.Get().Error("Error Initializing the DAOs. Error: %s", err)
		return err
	}
	return nil
}

func GetDbProvider() dbprovider.DbInterface {
	return DbManager
}

func GetMons(param bson.M) (models.Nodes, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	var mons models.Nodes
	var clusterNodes models.Nodes
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_NODES)
	if err := coll.Find(param).All(&clusterNodes); err != nil {
		return mons, err
	}
	for _, clusterNode := range clusterNodes {
		for k, v := range clusterNode.Options {
			if k == models.Mon && v == models.Yes {
				mons = append(mons, clusterNode)
			}
		}
	}
	return mons, nil
}

func CreateDefaultECProfiles(ctxt string, mon string, clusterId uuid.UUID) (bool, error) {
	sessionCopy := db.GetDatastore().Copy()
	defer sessionCopy.Close()
	var cluster models.Cluster
	coll := sessionCopy.DB(conf.SystemConfig.DBConfig.Database).C(models.COLL_NAME_STORAGE_CLUSTERS)
	if err := coll.Find(bson.M{"clusterid": clusterId}).One(&cluster); err != nil {
		logger.Get().Error("%s-Error getting cluster details for %v. error: %v", ctxt, clusterId, err)
		return false, err
	}
	var cmdMap map[string]string = map[string]string{
		"4+2": fmt.Sprintf("ceph osd erasure-code-profile set k4m2 plugin=jerasure k=4 m=2 --cluster %s", cluster.Name),
		"6+3": fmt.Sprintf("ceph osd erasure-code-profile set k6m3 plugin=jerasure k=6 m=3 --cluster %s", cluster.Name),
		"8+4": fmt.Sprintf("ceph osd erasure-code-profile set k8m4 plugin=jerasure k=8 m=4 --cluster %s", cluster.Name),
	}

	for k, v := range cmdMap {
		ok, _, err := cephapi_backend.ExecCmd(mon, clusterId, v, ctxt)
		if err != nil || !ok {
			logger.Get().Error("%s-Error creating EC profile for %s. error: %v", ctxt, k, err)
			continue
		} else {
			logger.Get().Debug("%s-Added EC profile for %s", ctxt, k)
		}
	}
	return true, nil
}

func InitInstaller() error {
	if installerinst, err := provisioner.InitializeProvisioner(conf.SystemConfig.Provisioners[bigfin_conf.ProviderName]); err != nil {
		logger.Get().Error("Unable to initialize the provisioner:%v", err)
		return err
	} else {
		installer_backend = installerinst
	}
	return nil
}

func SortDisksOnSize(disks []models.Disk) []models.Disk {
	if len(disks) <= 1 {
		return disks
	}

	mid := len(disks) / 2
	left := disks[:mid]
	right := disks[mid:]

	left = SortDisksOnSize(left)
	right = SortDisksOnSize(right)

	return merge(left, right)
}

func merge(left, right []models.Disk) []models.Disk {
	var result []models.Disk
	for len(left) > 0 || len(right) > 0 {
		if len(left) > 0 && len(right) > 0 {
			if left[0].Size >= right[0].Size {
				result = append(result, left[0])
				left = left[1:]
			} else {
				result = append(result, right[0])
				right = right[1:]
			}
		} else if len(left) > 0 {
			result = append(result, left[0])
			left = left[1:]
		} else if len(right) > 0 {
			result = append(result, right[0])
			right = right[1:]
		}
	}

	return result
}
