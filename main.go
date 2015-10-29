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
	"encoding/json"
	"fmt"
	"github.com/natefinch/pie"
	"github.com/op/go-logging"
	"github.com/skyrings/bigfin/backend/cephapi/client"
	"github.com/skyrings/bigfin/provider"
	"github.com/skyrings/bigfin/tools/logger"
	"github.com/skyrings/bigfin/tools/task"
	"github.com/skyrings/skyring/conf"
	"github.com/skyrings/skyring/db"
	"net/rpc/jsonrpc"
	"os"
	"path/filepath"
)

func main() {
	var config conf.SkyringCollection
	if err := json.Unmarshal([]byte(os.Args[1]), &config); err != nil {
		panic(fmt.Sprintf("Reading configurations failed. error: %v", err))
	}

	// Initialize the logger
	level, err := logging.LogLevel(config.Logging.Level.String())
	if err != nil {
		level = logging.DEBUG
	}
	if err := logger.Init(
		fmt.Sprintf("%s/ceph.log", filepath.Dir(config.Logging.Filename)),
		true,
		level); err != nil {
		panic(fmt.Sprintf("log init failed. %s", err))
	}

	//conf.LoadAppConfiguration("/etc/skyring/skyring.conf")
	if err := db.InitDBSession(config.DBConfig); err != nil {
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
