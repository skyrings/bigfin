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
package bigfinmodels

const (
	OSD_STATE_IN               = "In"
	OSD_STATE_OUT              = "Out"
	OSD_STATE_PAUSED           = "Paused"
	OSD_STATE_REPAIRING        = "Repairing"
	OSD_STATE_SCRUBBING        = "Scrubbing"
	OSD_STATE_UNSET            = "Unset"
	OSD_STATE_NOUP             = "Noup"
	OSD_STATE_NODOWN           = "Nodown"
	OSD_STATE_NOOUT            = "Noout"
	OSD_STATE_NOIN             = "Noin"
	CLUSTER_TYPE               = "ceph"
	OBJECTS                    = "objects"
	NUMBER_OF_OBJECTS          = "num_objects"
	NUMBER_OF_DEGRADED_OBJECTS = "num_objects_degraded"

	NODE_SERVICE_MON = "mon"
	NODE_SERVICE_OSD = "osd"

	MONITOR_AVAILABILTY = "monitor_availability"
	OSD_AVAILABILITY    = "osd_availability"
	SLU_SERVICE_COUNT   = "SluServiceCount"
	MON_SERVICE_COUNT   = "MonServiceCount"

	OSD_ADD_MESSAGE    = "added to the cluster map"
	OSD_REMOVE_MESSAGE = "removed from the cluster map"
	OSD_DOWN_MESSAGE   = "went down"
	OSD_UP_MESSAGE     = "came up"
)

type CrushInfo struct {
	RuleSetId   int
	CrushNodeId int
}

var NOTIFICATIONS_SUPPORTED = []string{
	MONITOR_AVAILABILTY,
	OSD_AVAILABILITY,
}

var NOTIFICATIONS_UNSUPPORTED = []string{
}

type ECProfileDet struct {
	Pool      string `json:"pool"`
	PoolId    int    `json:"pool_id"`
	ECProfile string `json:"erasure_code_profile"`
}
