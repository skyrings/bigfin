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
	"github.com/sbinet/go-python"
	"github.com/skyrings/bigfin/backend"
	"github.com/skyrings/skyring/tools/gopy"
	"github.com/skyrings/skyring/tools/uuid"
)

var functions = [...]string{
	"CreateCluster",
	"AddMon"
	"StartMon",
	"AddOSD",
	"CreatePool",
	"ListPool",
}

var py_functions map[string]*gopy.PyFunction

func init() {
	var err error
	py_functions, err = gopy.Import("skyring.provider.ceph.salt_wrapper", functions[:]...)
	if err != nil {
		panic(err)
	}
}

type Salt struct {
}

func (s Salt) CreateCluster(clusterName string, fsid uuid.UUID, mons []Mon) (bool, error) {
	if pyobj, err := py_functions["CreateCluster"].call(clusterName, fsid.String(), mons); err != nil {
		return false, err
	}
	return gopy.Bool(pyobj), nil
}

func (s Salt) AddMon(clusterName string, mons []Mon) (bool, error) {
	if pyobj, err := py_functions["AddMon"].call(clusterName, mons); err != nil {
		return false, err
	}
	return gopy.Bool(pyobj), nil
}

func (s Salt) StartMon(nodes []string) (bool, error) {
	if pyobj, err := py_functions["StartMon"].call(nodes); err != nil {
		return false, err
	}
	return gopy.Bool(pyobj), nil
}

func (s Salt) AddOSD(clusterName string, osd OSD) (bool, error) {
	if pyobj, err := py_functions["AddOSD"].call(clusterName, osd); err != nil {
		return false, err
	}
	return gopy.Bool(pyobj), nil
}

func (s Salt) CreatePool(mon string, clusterName string, name string, pgnum uint) (bool, error) {
	if pyobj, err := py_functions["CreatePool"].call(mon, clusterName, name, pgnum); err != nil {
		return false, err
	}
	return gopy.Bool(pyobj), nil
}

func (s Salt) ListPool(mon string, clusterName string) (bool, error) {
	if pyobj, err := py_functions["ListPool"].call(mon, clusterName); err != nil {
		return false, err
	}
	return gopy.Bool(pyobj), nil
}

func New() backend.Backend {
	return new(Salt)
}
