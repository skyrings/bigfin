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
package mariner

import (
	"github.com/skyrings/bigfin/backend"
	bigfin_conf "github.com/skyrings/bigfin/conf"
	"github.com/skyrings/skyring-common/provisioner"
	"github.com/skyrings/skyring-common/tools/logger"
	"github.com/skyrings/skyring-common/tools/uuid"
)

type Mariner struct {
}

var cephInstaller provisioner.Provisioner

func New() (backend.Backend, error) {
	marinerapi := new(Mariner)
	//Initilaize the provisioner
	cephInstaller, err := provisioner.InitializeProvisioner(bigfin_conf.SystemConfig.Provisioners[bigfin_conf.ProviderName])
	if err != nil {
		logger.Get().Error("Unable to initialize the provisioner, skipping the provider:%v", bigfin_conf.SystemConfig.Provisioners[bigfin_conf.ProviderName])
		return marinerapi, err
	}
	logger.Get().Debug("Installer:%v", cephInstaller)
	return marinerapi, nil
}

func (c Mariner) CreateCluster(clusterName string, fsid uuid.UUID, mons []backend.Mon, ctxt string) (bool, error) {
	return true, nil
}

func (c Mariner) AddMon(clusterName string, mons []backend.Mon, ctxt string) (bool, error) {
	return true, nil
}

func (c Mariner) StartMon(nodes []string, ctxt string) (bool, error) {
	return true, nil
}

func (c Mariner) AddOSD(clusterName string, osd backend.OSD, ctxt string) (map[string][]string, error) {
	return map[string][]string{}, nil
}

func (c Mariner) CreatePool(name string, mon string, clusterName string, pgnum uint, replicas int, quotaMaxObjects int, quotaMaxBytes uint64) (bool, error) {
	return true, nil
}

func (c Mariner) ListPoolNames(mon string, clusterName string) ([]string, error) {
	return []string{}, nil
}

func (c Mariner) GetClusterStatus(mon string, clusterName string) (status string, err error) {
	return "", nil
}

func (c Mariner) GetClusterStats(mon string, clusterName string) (map[string]int64, error) {
	return nil, nil
}

func (c Mariner) GetPools(mon string, clusterId uuid.UUID) ([]backend.CephPool, error) {
	return []backend.CephPool{}, nil
}

func (c Mariner) UpdatePool(mon string, clusterId uuid.UUID, poolId int, pool map[string]interface{}) (bool, error) {
	return true, nil
}

func (c Mariner) RemovePool(mon string, clusterId uuid.UUID, clusterName string, pool string, poolId int, ctxt string) (bool, error) {
	return true, nil
}

func (c Mariner) GetOSDDetails(mon string, clusterName string) (osds []backend.OSDDetails, err error) {
	return []backend.OSDDetails{}, nil
}

func (c Mariner) GetObjectCount(mon string, clusterName string) (string, error) {
	return "", nil
}

func (c Mariner) GetPGSummary(mon string, clusterId uuid.UUID) (backend.PgSummary, error) {
	return backend.PgSummary{}, nil
}

func (c Mariner) ExecCmd(mon string, clusterId uuid.UUID, cmd string) (bool, error) {
	return true, nil
}

func (c Mariner) GetOSDs(mon string, clusterId uuid.UUID) ([]backend.CephOSD, error) {
	return []backend.CephOSD{}, nil
}

func (c Mariner) UpdateOSD(mon string, clusterId uuid.UUID, osdId string, params map[string]interface{}) (bool, error) {
	return true, nil
}

func (c Mariner) GetOSD(mon string, clusterId uuid.UUID, osdId string) (backend.CephOSD, error) {
	return backend.CephOSD{}, nil
}
