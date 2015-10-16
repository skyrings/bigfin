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

package provider

import (
	"github.com/skyrings/bigfin/backend"
	"github.com/skyrings/skyring/tools/uuid"
)

type CephProvider struct {}

type CephMon struct {
	MonId     uuid.UUID   `json:"monid"`
	ClusterId uuid.UUID   `json:"clusterid"`
	Mon       backend.Mon `json:"mon"`
}

type CephMons []CephMon

type CephOSD struct {
	OSDId     uuid.UUID   `json:"osdid"`
	ClusterId uuid.UUID   `json:"clusterid"`
	OSD       backend.OSD `json:"osd"`
}

type CephOSDs []CephOSD

const (
	COLL_NAME_OSDS = "ceph_osds"
	COLL_NAME_MONS = "ceph_mons"
)
