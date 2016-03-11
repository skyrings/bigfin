// Copyright 2015 Red Hat, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package salt

import (
	"github.com/skyrings/bigfin/backend"
	"github.com/skyrings/skyring-common/tools/gopy"
	"github.com/skyrings/skyring-common/tools/uuid"
	"sync"
)

var funcNames = [...]string{
	"CreateCluster",
	"AddMon",
	"StartMon",
	"AddOSD",
	"CreatePool",
	"ListPool",
	"GetClusterStatus",
	"GetClusterStats",
	"GetOSDDetails",
	"GetObjectCount",
	"RemovePool",
}

var pyFuncs map[string]*gopy.PyFunction

var mutex sync.Mutex

func init() {
	var err error
	if pyFuncs, err = gopy.Import("bigfin.saltwrapper", funcNames[:]...); err != nil {
		panic(err)
	}
}

type Salt struct {
}

func (s Salt) CreateCluster(clusterName string, fsid uuid.UUID, mons []backend.Mon, ctxt string) (bool, error) {
	mutex.Lock()
	defer mutex.Unlock()
	pyobj, err := pyFuncs["CreateCluster"].Call(clusterName, fsid.String(), mons, ctxt)
	if err == nil {
		return gopy.Bool(pyobj), nil
	}

	return false, err
}

func (s Salt) AddMon(clusterName string, mons []backend.Mon, ctxt string) (bool, error) {
	mutex.Lock()
	defer mutex.Unlock()
	pyobj, err := pyFuncs["AddMon"].Call(clusterName, mons, ctxt)
	if err == nil {
		return gopy.Bool(pyobj), nil
	}

	return false, err
}

func (s Salt) StartMon(nodes []string, ctxt string) (bool, error) {
	mutex.Lock()
	defer mutex.Unlock()
	pyobj, err := pyFuncs["StartMon"].Call(nodes, ctxt)
	if err == nil {
		return gopy.Bool(pyobj), nil
	}

	return false, err
}

func (s Salt) AddOSD(clusterName string, osd backend.OSD, ctxt string) (osds map[string][]string, err error) {
	mutex.Lock()
	defer mutex.Unlock()
	osdMap := make(map[string][]string)
	if pyobj, loc_err := pyFuncs["AddOSD"].Call(clusterName, osd, ctxt); loc_err == nil {
		err = gopy.Convert(pyobj, &osdMap)
		osds = osdMap
	} else {
		err = loc_err
	}

	return
}

func (s Salt) CreatePool(name string, mon string, clusterName string, pgnum uint, replicas int, quotaMaxObjects int, quotaMaxBytes uint64, ctxt string) (bool, error) {
	mutex.Lock()
	defer mutex.Unlock()
	pyobj, err := pyFuncs["CreatePool"].Call(name, mon, clusterName, pgnum, ctxt)
	if err == nil {
		return gopy.Bool(pyobj), nil
	}

	return false, err
}

func (s Salt) ListPoolNames(mon string, clusterName string, ctxt string) (names []string, err error) {
	mutex.Lock()
	defer mutex.Unlock()
	if pyobj, loc_err := pyFuncs["ListPool"].Call(mon, clusterName, ctxt); loc_err == nil {
		err = gopy.Convert(pyobj, &names)
	} else {
		err = loc_err
	}

	return
}

func (s Salt) GetClusterStatus(mon string, clusterId uuid.UUID, clusterName string, ctxt string) (status string, err error) {
	mutex.Lock()
	defer mutex.Unlock()
	if pyobj, loc_err := pyFuncs["GetClusterStatus"].Call(mon, clusterName, ctxt); loc_err == nil {
		err = gopy.Convert(pyobj, &status)
	} else {
		err = loc_err
	}

	return
}

func (s Salt) GetPools(mon string, clusterId uuid.UUID, ctxt string) ([]backend.CephPool, error) {
	return []backend.CephPool{}, nil
}

func (s Salt) GetClusterStats(mon string, clusterName string, ctxt string) (stats backend.ClusterUtilization, err error) {
	stats = backend.ClusterUtilization{}
	mutex.Lock()
	defer mutex.Unlock()
	if pyobj, loc_err := pyFuncs["GetClusterStats"].Call(mon, clusterName, ctxt); loc_err == nil {
		err = gopy.Convert(pyobj, &stats)
	} else {
		err = loc_err
	}

	return
}

func (s Salt) UpdatePool(mon string, clusterId uuid.UUID, poolId int, pool map[string]interface{}, ctxt string) (bool, error) {
	return true, nil
}

func (s Salt) RemovePool(mon string, clusterId uuid.UUID, clusterName string, pool string, poolId int, ctxt string) (bool, error) {
	mutex.Lock()
	defer mutex.Unlock()
	pyobj, err := pyFuncs["RemovePool"].Call(mon, clusterName, pool, ctxt)
	if err == nil {
		return gopy.Bool(pyobj), nil
	}

	return false, err
}

func New() backend.Backend {
	return new(Salt)
}

func (s Salt) GetPGErrorCount(mon string, clusterId uuid.UUID, ctxt string) (int, error) {
	return 0, nil
}

func (s Salt) GetOSDDetails(mon string, clusterName string, ctxt string) (osds []backend.OSDDetails, err error) {
	mutex.Lock()
	defer mutex.Unlock()
	if pyobj, loc_err := pyFuncs["GetOSDDetails"].Call(mon, clusterName, ctxt); loc_err == nil {
		err = gopy.Convert(pyobj, &osds)
	} else {
		err = loc_err
	}
	return
}

func (s Salt) GetObjectCount(mon string, clusterName string, ctxt string) (obj map[string]int64, err error) {
	mutex.Lock()
	defer mutex.Unlock()
	obj = make(map[string]int64)
	if pyobj, loc_err := pyFuncs["GetObjectCount"].Call(mon, clusterName, ctxt); loc_err == nil {
		err = gopy.Convert(pyobj, &obj)
	} else {
		err = loc_err
	}

	return
}

func (s Salt) GetPGSummary(mon string, clusterId uuid.UUID, ctxt string) (backend.PgSummary, error) {
	var pgsummary backend.PgSummary
	return pgsummary, nil
}

func (s Salt) ExecCmd(mon string, clusterId uuid.UUID, cmd string, ctxt string) (bool, string, error) {
	return true, "", nil
}

func (s Salt) GetOSDs(mon string, clusterId uuid.UUID, ctxt string) ([]backend.CephOSD, error) {
	return []backend.CephOSD{}, nil
}

func (c Salt) UpdateOSD(mon string, clusterId uuid.UUID, osdId string, params map[string]interface{}, ctxt string) (bool, error) {
	return true, nil

}

func (c Salt) GetOSD(mon string, clusterId uuid.UUID, osdId string, ctxt string) (backend.CephOSD, error) {

	return backend.CephOSD{}, nil
}
