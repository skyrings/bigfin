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

package backend

import (
	"github.com/skyrings/skyring-common/tools/uuid"
)

type Mon struct {
	Node       string
	PublicIP4  string //TODO: use ipv4 type
	ClusterIP4 string //TODO: use ipv4 type
}

type OSD struct {
	Node       string
	PublicIP4  string //TODO: use ipv4 type
	ClusterIP4 string //TODO: use ipv4 type
	Device     string
	FSType     string
}

type CephPool struct {
	Id                  int    `json:"id"`
	Name                string `json:"name"`
	Size                int    `json:"size"`
	MinSize             uint64 `json:"min_size"`
	QuotaMaxObjects     int    `json:"quota_max_objects"`
	HashPsPool          bool   `json:"hashpspool"`
	QuotaMaxBytes       uint64 `json:"quota_max_bytes"`
	PgNum               int    `json:"pg_num"`
	PgpNum              int    `json:"pgp_num"`
	Full                bool   `json:"full"`
	CrashReplayInterval int    `json:"crash_replay_interval"`
	CrushRuleSet        int    `json:"crush_ruleset"`
}

type Backend interface {
	CreateCluster(clusterName string, fsid uuid.UUID, mons []Mon, ctxt string) (bool, error)
	AddMon(clusterName string, mons []Mon, ctxt string) (bool, error)
	StartMon(nodes []string, ctxt string) (bool, error)
	AddOSD(clusterName string, osd OSD, ctxt string) (map[string][]string, error)
	CreatePool(name string, mon string, clusterName string, pgnum uint, replicas int, quotaMaxObjects int, quotaMaxBytes uint64) (bool, error)
	ListPoolNames(mon string, clusterName string) ([]string, error)
	GetClusterStatus(mon string, clusterName string) (string, error)
	GetPools(mon string, clusterId uuid.UUID) ([]CephPool, error)
	UpdatePool(mon string, clusterId uuid.UUID, poolId int, pool map[string]interface{}) (bool, error)
	GetClusterStats(mon string, clusterName string) (map[string]int64, error)
	GetOSDDetails(mon string, clusterName string) ([]OSDDetails, error)
}

type OSDDetails struct {
	Name         string
	Id           uint
	Available    uint
	UsagePercent uint
	Used         uint
}
