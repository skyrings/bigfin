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

func (s Salt) CreateCluster(clusterName string, fsid uuid.UUID, mons []backend.Mon) (bool, error) {
	mutex.Lock()
	defer mutex.Unlock()
	pyobj, err := pyFuncs["CreateCluster"].Call(clusterName, fsid.String(), mons)
	if err == nil {
		return gopy.Bool(pyobj), nil
	}

	return false, err
}

func (s Salt) AddMon(clusterName string, mons []backend.Mon) (bool, error) {
	mutex.Lock()
	defer mutex.Unlock()
	pyobj, err := pyFuncs["AddMon"].Call(clusterName, mons)
	if err == nil {
		return gopy.Bool(pyobj), nil
	}

	return false, err
}

func (s Salt) StartMon(nodes []string) (bool, error) {
	mutex.Lock()
	defer mutex.Unlock()
	pyobj, err := pyFuncs["StartMon"].Call(nodes)
	if err == nil {
		return gopy.Bool(pyobj), nil
	}

	return false, err
}

func (s Salt) AddOSD(clusterName string, osd backend.OSD) (bool, error) {
	mutex.Lock()
	defer mutex.Unlock()
	pyobj, err := pyFuncs["AddOSD"].Call(clusterName, osd)
	if err == nil {
		return gopy.Bool(pyobj), nil
	}

	return false, err
}

func (s Salt) CreatePool(name string, mon string, clusterName string, pgnum uint, replicas int, quotaMaxObjects int, quotaMaxBytes uint64) (bool, error) {
	mutex.Lock()
	defer mutex.Unlock()
	pyobj, err := pyFuncs["CreatePool"].Call(name, mon, clusterName, pgnum)
	if err == nil {
		return gopy.Bool(pyobj), nil
	}

	return false, err
}

func (s Salt) ListPoolNames(mon string, clusterName string) (names []string, err error) {
	mutex.Lock()
	defer mutex.Unlock()
	if pyobj, loc_err := pyFuncs["ListPool"].Call(mon, clusterName); loc_err == nil {
		err = gopy.Convert(pyobj, &names)
	} else {
		err = loc_err
	}

	return
}

func (s Salt) GetClusterStatus(mon string, clusterName string) (status string, err error) {
	mutex.Lock()
	defer mutex.Unlock()
	if pyobj, loc_err := pyFuncs["GetClusterStatus"].Call(mon, clusterName); loc_err == nil {
		err = gopy.Convert(pyobj, &status)
	} else {
		err = loc_err
	}

	return
}

func (s Salt) GetPools(mon string, clusterName string) ([]backend.CephPool, error) {
	return []backend.CephPool{}, nil
}

func New() backend.Backend {
	return new(Salt)
}
