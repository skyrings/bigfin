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
	"fmt"
	"github.com/skyrings/bigfin/utils"
	"github.com/skyrings/bigfin/tools/task"
	"github.com/skyrings/skyring-common/models"
	"github.com/skyrings/skyring-common/tools/logger"
	"github.com/skyrings/skyring-common/tools/uuid"
	"net/http"
)

func (s *CephProvider) StopTask(req models.RpcRequest, resp *models.RpcResponse) error {
	task_id_str := req.RpcRequestVars["task-id"]
	task_id, err := uuid.Parse(task_id_str)
	logger.Get().Debug(fmt.Sprintf("Stopping sub-task: %s", task_id_str))
	if err != nil {
		logger.Get().Error("Error parsing the task id: %s. error: %v", task_id_str, err)
		*resp = utils.WriteResponse(http.StatusBadRequest, fmt.Sprintf("Error parsing the task id: %s", task_id_str))
		return err
	}

	// Stop the task
	if ok, err := task.GetTaskManager().Stop(*task_id); !ok || err != nil {
		logger.Get().Error("Failed to stop the task: %v. error: %v", task_id, err)
		*resp = utils.WriteResponse(http.StatusInternalServerError, "Failed to stop task")
		return err
	} else {
		*resp = utils.WriteResponse(http.StatusOK, "Done")
	}
	return nil
}
