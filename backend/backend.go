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
	"github.com/skyrings/skyring-common/models"
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

type ClusterUtilization struct {
	Used      int64
	Available int64
	Total     int64
	Pools     []PoolStats
}

type PoolStats struct {
	Id        int
	Name      string
	Used      int64
	Available int64
}

type CephClusterHealth struct {
	OverallStatus string                      `json:"overall_status"`
	Health        []CephClusterHealthServices `json:"health_services"`
	Summary       []SeveritySummary           `json:"summary"`
	TimeChecks    ClusterTimeChecks           `json:"timechecks"`
}

type CephClusterHealthServices struct {
	Mons []MonDetail `json:"mons"`
}

type MonDetail struct {
	Name         string     `json:"name"`
	LastUpdated  string     `json:"last_updated"`
	AvailPercent int        `json:"avail_percent"`
	KbTotal      uint64     `json:"kb_total"`
	KbAvail      uint64     `json:"kb_avail"`
	KbUsed       uint64     `json:"kb_used"`
	Health       string     `json:"health"`
	StoreStats   StoreStats `json:"store_stats"`
}

type StoreStats struct {
	BytesTotak  uint64 `json:"bytes_total"`
	BytesLog    uint64 `json:"bytes_log"`
	BytesMisc   uint64 `json:"bytes_misc"`
	BytesSst    uint64 `json:"bytes_sst"`
	LastUpdated string `json:"last_updated"`
}

type SeveritySummary struct {
	Severity string `json:"severity"`
	Summary  string `json:"summary"`
}

type ClusterTimeChecks struct {
	RoundStatus string `json:"round_status"`
	Epoch       int    `json:"epoch"`
	Round       int    `json:"round"`
}

type Backend interface {
	CreateCluster(clusterName string, fsid uuid.UUID, mons []Mon, ctxt string) (bool, error)
	AddMon(clusterName string, mons []Mon, ctxt string) (bool, error)
	StartMon(nodes []string, ctxt string) (bool, error)
	AddOSD(clusterName string, osd OSD, ctxt string) (map[string][]string, error)
	CreatePool(name string, mon string, clusterName string, pgnum uint, replicas int, quotaMaxObjects int, quotaMaxBytes uint64, ruleset int, ctxt string) (bool, error)
	CreateECPool(name string, mon string, clusterName string, pgnum uint, replicas int, quotaMaxObjects int, quotaMaxBytes uint64, ecProfile string, ruleset map[string]interface{}, sProfile string, ctxt string) (bool, error)
	ListPoolNames(mon string, clusterName string, ctxt string) ([]string, error)
	GetClusterStatus(mon string, clusterId uuid.UUID, clusterName string, ctxt string) (string, error)
	GetPools(mon string, clusterId uuid.UUID, ctxt string) ([]CephPool, error)
	GetPool(mon string, clusterId uuid.UUID, pool_id int, ctxt string) (CephPool, error)
	UpdatePool(mon string, clusterId uuid.UUID, poolId int, pool map[string]interface{}, ctxt string) (bool, error)
	RemovePool(mon string, clusterId uuid.UUID, clusterName string, pool string, poolId int, ctxt string) (bool, error)
	GetClusterStats(mon string, clusterName string, ctxt string) (ClusterUtilization, error)
	GetOSDDetails(mon string, clusterName string, ctxt string) ([]OSDDetails, error)
	GetObjectCount(mon string, clusterName string, ctxt string) (map[string]int64, error)
	GetPGSummary(mon string, clusterId uuid.UUID, ctxt string) (PgSummary, error)
	ExecCmd(mon string, clusterId uuid.UUID, cmd string, ctxt string) (bool, string, error)
	GetOSDs(mon string, clusterId uuid.UUID, ctxt string) ([]CephOSD, error)
	GetOSD(mon string, clusterId uuid.UUID, osdId string, ctxt string) (CephOSD, error)
	UpdateOSD(mon string, clusterId uuid.UUID, osdId string, params map[string]interface{}, ctxt string) (bool, error)
	GetPGCount(mon string, clusterId uuid.UUID, ctxt string) (map[string]uint64, error)
	GetClusterConfig(mon string, clusterId uuid.UUID, ctxt string) (map[string]string, error)
	CreateCrushRule(mon string, clusterId uuid.UUID, rule CrushRuleRequest, ctxt string) (int, error)
	CreateCrushNode(mon string, clusterId uuid.UUID, node CrushNodeRequest, ctxt string) (int, error)
	GetCrushNodes(mon string, clusterId uuid.UUID, ctxt string) ([]CrushNode, error)
	GetCrushNode(mon string, clusterId uuid.UUID, crushNodeId int, ctxt string) (CrushNode, error)
	PatchCrushNode(mon string, clusterId uuid.UUID, crushNodeId int, params map[string]interface{}, ctxt string) (bool, error)
	GetCrushRules(mon string, clusterId uuid.UUID, ctxt string) ([]map[string]interface{}, error)
	GetCrushRule(mon string, clusterId uuid.UUID, crushRuleId int, ctxt string) (map[string]interface{}, error)
	PatchCrushRule(mon string, clusterId uuid.UUID, crushRuleId int, params map[string]interface{}, ctxt string) (bool, error)
	GetMonitors(mon string, clusterId uuid.UUID, ctxt string) ([]string, error)
	GetCluster(mon string, ctxt string) (CephCluster, error)
	GetClusterNetworks(mon string, clusterId uuid.UUID, ctxt string) (models.ClusterNetworks, error)
	GetClusterNodes(mon string, clusterId uuid.UUID, ctxt string) ([]CephClusterNode, error)
	GetMonStatus(mon string, clusterId uuid.UUID, node string, ctxt string) (MonNodeStatus, error)
	ParticipatesInCluster(node string, ctxt string) bool
	GetPartDeviceDetails(node string, partPath string, ctxt string) (DeviceDetail, error)
	GetJournalDeviceDetails(node string, journalPath string, ctxt string) (DeviceDetail, error)
	GetServiceCount(hostname string, ctxt string) (map[string]int, error)
	GetRBDStats(mon string, poolName string, clusterName string, ctxt string) ([]RBDStats, error)
	StartCalamari(node string, ctxt string) error
	StopCalamari(node string, ctxt string) error
	EmitRbdEvents(node string, cluster string, ctxt string) error
	AddOsdToCrush(mon string, clusterName string, osdName string, weight float64, host string, ctxt string) (bool, error)
}

type OSDDetails struct {
	Name         string
	Id           uint
	Available    uint64
	UsagePercent uint64
	Used         uint64
}

type PgSummary struct {
	ByOSD  map[string]map[string]uint64 `json:"by_osd"`
	ByPool map[string]map[string]uint64 `json:"by_pool"`
	All    map[string]uint64            `json:"all"`
}

type RBDStats struct {
	Name  string `json:"name"`
	Used  uint64 `json:"used"`
	Total uint64 `json:"total"`
}

type CephOSD struct {
	Uuid                 uuid.UUID `json:"uuid"`
	Up                   bool      `json:"up"`
	In                   bool      `json:"in"`
	Id                   int       `json:"id"`
	Reweight             float32   `json:"reweight"`
	Server               string    `json:"server"`
	Pools                []int     `json:"pools"`
	ValidCommand         []string  `json:"valid_commands"`
	PublicAddr           string    `json:"public_addr"`
	ClusterAddr          string    `json:"cluster_addr"`
	CrushNodeAncestry    [][]int   `json:"crush_node_ancestry"`
	BackendPartitionPath string    `json:"backend_partition_path"`
	BackendDeviceNode    string    `json:"backend_device_node"`
	OsdData              string    `json:"osd_data"`
	OsdJournal           string    `json:"osd_journal"`
}

type CrushItem struct {
	Id     int     `json:"id"`
	Weight float64 `json:"weight"`
	Pos    int     `json:"pos"`
}

type CrushNodeRequest struct {
	BucketType string      `json:"bucket_type"`
	Name       string      `json:"name"`
	Items      []CrushItem `json:"items"`
}

type CrushRuleRequest struct {
	Name    string                   `json:"name"`
	Type    string                   `json:"type"`
	MinSize int                      `json:"min_size"`
	MaxSize int                      `json:"max_size"`
	Steps   []map[string]interface{} `json:"steps"`
}

type CrushNode struct {
	BucketType string      `json:"bucket_type"`
	Name       string      `json:"name"`
	Id         int         `json:"id"`
	Weight     float64     `json:"weight"`
	Alg        string      `json:"alg"`
	Hash       string      `json:"hash"`
	Items      []CrushItem `json:"items"`
}

type CephMons struct {
	Quorum    []int     `json:"quorum"`
	Created   string    `json:"crated"`
	Mofified  string    `json:"modified"`
	ClusterId uuid.UUID `json:"fsid"`
	Mons      []CephMon `json:"mons"`
}

type CephMon struct {
	Name string `json:"name"`
	Rank int    `json:"rank"`
	Addr string `json:"addr"`
}

type BlockDevice struct {
	Name            string `json:"name"`
	Size            uint64 `json:"size"`
	Objects         uint64 `json:"objects"`
	Order           int    `json:"order"`
	ObjectSize      uint64 `json:"object_size"`
	BlockNamePrefix string `json:"block_name_prefix"`
	Format          int    `json:"format"`
}

type CephCluster struct {
	Id   uuid.UUID `json:"id"`
	Name string    `json:"name"`
}

type CephClusterNode struct {
	Hostname    string            `json:"hostname"`
	FQDN        string            `json:"fqdn"`
	Managed     bool              `json:"managed"`
	CephVersion string            `json:"ceph_version"`
	Services    []CephNodeService `json:"services"`
}

type CephNodeService struct {
	Id      string `json:"id"`
	Type    string `json:"type"`
	Running bool   `json:"running"`
}

type MonNodeStatus struct {
	State  string `json:"state"`
	Rank   uint   `json:"rank"`
	Quorum []uint `json:"quorum"`
}

type DeviceDetail struct {
	DevName  string    `json:"devname"`
	Uuid     uuid.UUID `json:"uuid"`
	PartName string    `json:"partname"`
	FSType   string    `json:"fstype"`
	Size     float64   `json:"size"`
}
