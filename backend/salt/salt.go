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
	"github.com/skyrings/skyring/tools/gopy"
	"github.com/skyrings/skyring/tools/uuid"
)

var funcNames = [...]string{
	"CreateCluster",
	"AddMon",
	"StartMon",
	"AddOSD",
	"CreatePool",
	"ListPool",
}

var pyFuncs map[string]*gopy.PyFunction

func init() {
	var err error
	if pyFuncs, err = gopy.Import("skyring.provider.ceph.saltwrapper", funcNames[:]...); err != nil {
		panic(err)
	}
}

type Salt struct {
}

func (s Salt) CreateCluster(clusterName string, fsid uuid.UUID, mons []backend.Mon) (status bool, err error) {
	if pyobj, err := pyFuncs["CreateCluster"].Call(clusterName, fsid.String(), mons); err == nil {
		status = gopy.Bool(pyobj)
	}

	return
}

func (s Salt) AddMon(clusterName string, mons []backend.Mon) (status bool, err error) {
	if pyobj, err := pyFuncs["AddMon"].Call(clusterName, mons); err == nil {
		status = gopy.Bool(pyobj)
	}

	return
}

func (s Salt) StartMon(nodes []string) (status bool, err error) {
	if pyobj, err := pyFuncs["StartMon"].Call(nodes); err == nil {
		status = gopy.Bool(pyobj)
	}

	return
}

func (s Salt) AddOSD(clusterName string, osd backend.OSD) (status bool, err error) {
	if pyobj, err := pyFuncs["AddOSD"].Call(clusterName, osd); err == nil {
		status = gopy.Bool(pyobj)
	}

	return
}

func (s Salt) CreatePool(name string, mon string, clusterName string, pgnum uint, replicas int, quotaMaxObjects int, quotaMaxBytes uint64) (status bool, err error) {
	if pyobj, err := pyFuncs["CreatePool"].Call(name, mon, clusterName, pgnum); err == nil {
		status = gopy.Bool(pyobj)
	}

	return
}

func (s Salt) ListPool(mon string, clusterName string) (names []string, err error) {
	if pyobj, err := pyFuncs["ListPool"].Call(mon, clusterName); err == nil {
		err = gopy.Convert(pyobj, &names)
	}

	return
}

func New() backend.Backend {
	return new(Salt)
}
