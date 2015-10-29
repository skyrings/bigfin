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

package main

import (
	// "encoding/json"
	// "flag"
	"fmt"
	"github.com/natefinch/pie"
	"github.com/op/go-logging"
	"github.com/skyrings/bigfin/backend/cephapi/client"
	"github.com/skyrings/bigfin/provider"
	"github.com/skyrings/bigfin/tools/task"
	"github.com/skyrings/skyring/conf"
	"github.com/skyrings/skyring/db"
	"github.com/skyrings/skyring/tools/logger"
	"net/rpc/jsonrpc"
)

func main() {
	// Initialize the logger
	if err := logger.Init("/var/log/skyring/ceph.log", true, logging.DEBUG); err != nil {
		panic(fmt.Sprintf("log init failed. %s", err))
	}

	conf.LoadAppConfiguration("/etc/skyring/skyring.conf")
	if err := db.InitDBSession(conf.SystemConfig.DBConfig); err != nil {
		logger.Get().Fatalf("Unable to initialize DB")
	}

	// Initialize the task manager
	if err := task.InitializeTaskManager(); err != nil {
		logger.Get().Fatalf("Failed to initialize task manager: %v", err)
	}

	// Initialize ceph http client
	client.InitCephApiSession()

	provd := &provider.CephProvider{}
	p := pie.NewProvider()
	if err := p.RegisterName("ceph", provd); err != nil {
		logger.Get().Fatalf("Failed to register plugin: %s", err)
	}
	p.ServeCodec(jsonrpc.NewServerCodec)
}
