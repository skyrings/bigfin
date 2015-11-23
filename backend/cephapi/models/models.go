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

package models

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

type CephCluster struct {
	Name       string `json:"name"`
	Id         string `json:"id"`
	UpdateTime string `json:"update_time"`
}

type CephAsyncRequest struct {
	RequestId string `json:"request_id"`
}

type CephRequestStatus struct {
	Id           string `json:"id"`
	State        string `json:"state"`
	Headline     string `json:"headline"`
	Status       string `json:"status"`
	Error        bool   `json:"error"`
	ErrorMessage string `json:"error_message"`
}

type CephOSD struct {
	UUID               string   `json:"uuid"`
	Id                 int      `json:"id"`
	ReWeight           float32  `json:"reweight"`
	Up                 bool     `json:"up"`
	Server             string   `json:"server"`
	PublicAddr         string   `json:"public_addr"`
	In                 bool     `json:"in"`
	Pools              []int    `json:"pools"`
	ValidCommands      []string `json:"valid_commands"`
	ClusterAddr        string   `json:"cluster_addr"`
	BackendPartionPath string   `json:"backend_partition_path"`
	BackendDevieMode   string   `json:"backend_device_mode"`
	CrushNodeAncestry  [][]int  `json:"crush_node_ancestry"`
}
