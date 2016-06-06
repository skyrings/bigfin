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
	"github.com/skyrings/bigfin/tools/task"
	"github.com/skyrings/skyring-common/conf"
	"github.com/skyrings/skyring-common/db"
	"github.com/skyrings/skyring-common/tools/logger"
	"github.com/skyrings/skyring-common/tools/schedule"
	"net/rpc/jsonrpc"
	"os"
	"path/filepath"

	bigfin_conf "github.com/skyrings/bigfin/conf"
)

func main() {
	var config conf.SkyringCollection
	if err := json.Unmarshal([]byte(os.Args[1]), &config); err != nil {
		panic(fmt.Sprintf("Reading configurations failed. error: %v", err))
	}
	conf.SystemConfig = config

	var eventTypes = make(map[string]string)
	if err := json.Unmarshal([]byte(os.Args[2]), &eventTypes); err != nil {
		panic(fmt.Sprintf("Reading event types failed. error: %v", err))
	}
	provider.EventTypes = eventTypes

	var providerConfig conf.ProviderInfo
	if err := json.Unmarshal([]byte(os.Args[3]), &providerConfig); err != nil {
		panic(fmt.Sprintf("Reading provider configurations failed. error: %v", err))
	}
	bigfin_conf.ProviderConfig = providerConfig

	// Initialize the logger
	level, err := logging.LogLevel(config.Logging.Level.String())
	if err != nil {
		level = logging.DEBUG
	}
	if err := logger.Init(
		"bigfin",
		fmt.Sprintf("%s/bigfin.log", filepath.Dir(config.Logging.Filename)),
		true,
		level); err != nil {
		panic(fmt.Sprintf("log init failed. %s", err))
	}

	if err := db.InitDBSession(config.DBConfig); err != nil {
		logger.Get().Fatalf("Unable to initialize DB. error: %v", err)
	}

	//Initialize the DB provider
	if err := provider.InitializeDb(); err != nil {
		logger.Get().Error("Unable to initialize the DB provider: %s", err)
	}

	// Initialize the task manager
	if err := task.InitializeTaskManager(); err != nil {
		logger.Get().Fatalf("Failed to initialize task manager. error: %v", err)
	}

	if err := provider.InitMonitoringManager(); err != nil {
		logger.Get().Fatalf("Error initializing the monitoring manager: %v", err)
	}

	if err := provider.InitInstaller(); err != nil {
		logger.Get().Fatalf("Error initializing the Installer: %v", err)
	}

	schedule.InitShechuleManager()
	if err := provider.ScheduleRbdEventEmitter(); err != nil {
		logger.Get().Error("Error while initializing RbdEventer scheduler: %v", err)
	}

	// Initialize ceph http client
	client.InitCephApiSession()

	provd := &provider.CephProvider{}
	p := pie.NewProvider()
	if err := p.RegisterName(bigfin_conf.ProviderName, provd); err != nil {
		logger.Get().Fatalf("Failed to register plugin. error: %v", err)
	}
	p.ServeCodec(jsonrpc.NewServerCodec)
}
