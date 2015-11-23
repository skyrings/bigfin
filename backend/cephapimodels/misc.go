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

package cephapimodels

type CephPoolRequest struct {
	Name                string `json:"name"`
	Size                int    `json:"size"`
	MinSize             uint64 `json:"min_size"`
	QuotaMaxObjects     int    `json:"quota_max_objects"`
	HashPsPool          bool   `json:"hashpspool"`
	QuotaMaxBytes       uint64 `json:"quota_max_bytes"`
	PgNum               int    `json:"pg_num"`
	PgpNum              int    `json:"pgp_num"`
	CrashReplayInterval int    `json:"crash_replay_interval"`
	CrushRuleSet        int    `json:"crush_ruleset"`
}
