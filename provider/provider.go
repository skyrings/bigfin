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
	"github.com/skyrings/bigfin/backend/salt"
	"github.com/skyrings/skyring/tools/task"
)

var (
	salt_backend = salt.New()
	TaskManager  task.Manager
)

type CephProvider struct{}

func (a *CephProvider) InitializeTaskManager() error {
	TaskManager = task.NewManager()
	return nil
}

func (a *CephProvider) GetTaskManager() *task.Manager {
	return &TaskManager
}
