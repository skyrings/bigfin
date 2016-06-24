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

package cephapi

type CephApiRoute struct {
	Name    string
	Pattern string
	Method  string
	Version int
}

var (
	CEPH_API_ROUTES = make(map[string]CephApiRoute)
)

func (c *CephApi) LoadRoutes() {
	var routes = []CephApiRoute{
		{
			Name:    "CreatePool",
			Pattern: "cluster/{cluster-fsid}/pool",
			Method:  "POST",
			Version: 2,
		},
		{
			Name:    "GetCluster",
			Pattern: "cluster?format=json",
			Method:  "GET",
			Version: 2,
		},
		{
			Name:    "GetRequestStatus",
			Pattern: "request/{request-fsid}?format=json",
			Method:  "GET",
			Version: 2,
		},
		{
			Name:    "GetOSDs",
			Pattern: "cluster/{cluster-fsid}/osd?format=json",
			Method:  "GET",
			Version: 2,
		},
		{
			Name:    "GetPGCount",
			Pattern: "cluster/{cluster-fsid}/health_counters?format=json",
			Method:  "GET",
			Version: 1,
		},
		{
			Name:    "GetOSD",
			Pattern: "cluster/{cluster-fsid}/osd?id__in[]={osd-id}&format=json",
			Method:  "GET",
			Version: 2,
		},
		{
			Name:    "UpdateOSD",
			Pattern: "cluster/{cluster-fsid}/osd/{osd-id}",
			Method:  "PATCH",
			Version: 2,
		},
		{
			Name:    "GetPools",
			Pattern: "cluster/{cluster-fsid}/pool?format=json",
			Method:  "GET",
			Version: 2,
		},
		{
			Name:    "GetPool",
			Pattern: "cluster/{cluster-fsid}/pool/{pool-id}?format=json",
			Method:  "GET",
			Version: 2,
		},
		{
			Name:    "UpdatePool",
			Pattern: "cluster/{cluster-fsid}/pool/{pool-id}",
			Method:  "PATCH",
			Version: 2,
		},
		{
			Name:    "RemovePool",
			Pattern: "cluster/{cluster-fsid}/pool/{pool-id}",
			Method:  "DELETE",
			Version: 2,
		},
		{
			Name:    "PGStatistics",
			Pattern: "cluster/{cluster-fsid}/sync_object/pg_summary?format=json",
			Method:  "GET",
			Version: 2,
		},
		{
			Name:    "ExecCmd",
			Pattern: "cluster/{cluster-fsid}/cli",
			Method:  "POST",
			Version: 2,
		},
		{
			Name:    "GetClusterStatus",
			Pattern: "cluster/{cluster-fsid}/sync_object/health?format=json",
			Method:  "GET",
			Version: 2,
		},
		{
			Name:    "GetClusterConfig",
			Pattern: "cluster/{cluster-fsid}/sync_object/config?format=json",
			Method:  "GET",
			Version: 2,
		},
		{

			Name:    "CreateCrushRule",
			Pattern: "cluster/{cluster-fsid}/crush_rule",
			Method:  "POST",
			Version: 2,
		},
		{
			Name:    "CreateCrushNode",
			Pattern: "cluster/{cluster-fsid}/crush_node",
			Method:  "POST",
			Version: 2,
		},
		{
			Name:    "GetCrushNodes",
			Pattern: "cluster/{cluster-fsid}/crush_node",
			Method:  "GET",
			Version: 2,
		},
		{
			Name:    "GetCrushNode",
			Pattern: "cluster/{cluster-fsid}/crush_node/{crush-node-id}",
			Method:  "GET",
			Version: 2,
		},
		{
			Name:    "GetCrushRule",
			Pattern: "cluster/{cluster-fsid}/crush_rule/{crush-rule-id}",
			Method:  "GET",
			Version: 2,
		},
		{
			Name:    "GetCrushRules",
			Pattern: "cluster/{cluster-fsid}/crush_rule",
			Method:  "GET",
			Version: 2,
		},
		{
			Name:    "PatchCrushNode",
			Pattern: "cluster/{cluster-fsid}/crush_node/{crush-node-id}",
			Method:  "PATCH",
			Version: 2,
		},
		{
			Name:    "PatchCrushRule",
			Pattern: "cluster/{cluster-fsid}/crush_rule/{crush-rule-id}",
			Method:  "PATCH",
			Version: 2,
		},
		{
			Name:    "GetClusterNetworks",
			Pattern: "cluster/{cluster-fsid}/sync_object/config?format=json",
			Method:  "GET",
			Version: 2,
		},
		{
			Name:    "GetNodes",
			Pattern: "cluster/{cluster-fsid}/server?format=json",
			Method:  "GET",
			Version: 2,
		},
		{
			Name:    "GetMons",
			Pattern: "cluster/{cluster-fsid}/sync_object/mon_map?format=json",
			Method:  "GET",
			Version: 2,
		},
		{
			Name:    "GetMonStatus",
			Pattern: "cluster/{cluster-fsid}/mon/{mon-name}/status?format=json",
			Method:  "GET",
			Version: 2,
		},
	}
	for _, route := range routes {
		CEPH_API_ROUTES[route.Name] = route
	}
}
