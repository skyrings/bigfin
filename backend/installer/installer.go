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
package installer

import (
	"github.com/skyrings/bigfin/backend"
	"github.com/skyrings/skyring-common/provisioner"
	"github.com/skyrings/skyring-common/tools/logger"
	"github.com/skyrings/skyring-common/tools/uuid"

	bigfin_conf "github.com/skyrings/bigfin/conf"
)

type Installer struct {
}

var installer provisioner.Provisioner

func New() (backend.Backend, error) {
	installerapi := new(Installer)
	//Initilaize the provisioner
	installer, err := provisioner.InitializeProvisioner(bigfin_conf.SystemConfig.Provisioners[bigfin_conf.ProviderName])
	if err != nil {
		logger.Get().Error("Unable to initialize the provisioner, skipping the provider:%v", bigfin_conf.SystemConfig.Provisioners[bigfin_conf.ProviderName])
		return installerapi, err
	}
	logger.Get().Debug("Installer:%v", installer)
	return installerapi, nil
}

func (c Installer) CreateCluster(clusterName string, fsid uuid.UUID, mons []backend.Mon, ctxt string) (bool, error) {
	return true, nil
}

func (c Installer) AddMon(clusterName string, mons []backend.Mon, ctxt string) (bool, error) {
	return true, nil
}

func (c Installer) StartMon(nodes []string, ctxt string) (bool, error) {
	return true, nil
}

func (c Installer) AddOSD(clusterName string, osd backend.OSD, ctxt string) (map[string][]string, error) {
	return map[string][]string{}, nil
}

func (c Installer) CreatePool(name string, mon string, clusterName string, pgnum uint, replicas int, quotaMaxObjects int, quotaMaxBytes uint64, ctxt string) (bool, error) {
	return true, nil
}

func (c Installer) ListPoolNames(mon string, clusterName string, ctxt string) ([]string, error) {
	return []string{}, nil
}

func (c Installer) GetClusterStatus(mon string, clusterId uuid.UUID, clusterName string, ctxt string) (status string, err error) {
	return "", nil
}

func (c Installer) GetClusterStats(mon string, clusterName string, ctxt string) (backend.ClusterUtilization, error) {
	return backend.ClusterUtilization{}, nil
}

func (c Installer) GetPools(mon string, clusterId uuid.UUID, ctxt string) ([]backend.CephPool, error) {
	return []backend.CephPool{}, nil
}

func (c Installer) UpdatePool(mon string, clusterId uuid.UUID, poolId int, pool map[string]interface{}, ctxt string) (bool, error) {
	return true, nil
}

func (c Installer) RemovePool(mon string, clusterId uuid.UUID, clusterName string, pool string, poolId int, ctxt string) (bool, error) {
	return true, nil
}

func (c Installer) GetOSDDetails(mon string, clusterName string, ctxt string) (osds []backend.OSDDetails, err error) {
	return []backend.OSDDetails{}, nil
}

func (c Installer) GetObjectCount(mon string, clusterName string, ctxt string) (map[string]int64, error) {
	return map[string]int64{}, nil
}

func (c Installer) GetPGSummary(mon string, clusterId uuid.UUID, ctxt string) (backend.PgSummary, error) {
	return backend.PgSummary{}, nil
}

func (c Installer) ExecCmd(mon string, clusterId uuid.UUID, cmd string, ctxt string) (bool, string, error) {
	return true, "", nil
}

func (c Installer) GetOSDs(mon string, clusterId uuid.UUID, ctxt string) ([]backend.CephOSD, error) {
	return []backend.CephOSD{}, nil
}

func (c Installer) UpdateOSD(mon string, clusterId uuid.UUID, osdId string, params map[string]interface{}, ctxt string) (bool, error) {
	return true, nil
}

func (c Installer) GetOSD(mon string, clusterId uuid.UUID, osdId string, ctxt string) (backend.CephOSD, error) {
	return backend.CephOSD{}, nil
}
