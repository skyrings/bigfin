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
	"github.com/skyrings/bigfin/provider"
	"github.com/skyrings/skyring/conf"
	"github.com/skyrings/skyring/db"
	"github.com/skyrings/skyring/tools/logger"
	"log"
	"net/rpc/jsonrpc"
)

func main() {
	conf.LoadAppConfiguration("/etc/skyring/skyring.conf")
	if err := db.InitDBSession(conf.SystemConfig.DBConfig); err != nil {
		log.Fatalf("Unable to initialize DB")
	}

	if err := logger.Init("/var/log/skyring/ceph.log", true, logging.DEBUG); err != nil {
		panic(fmt.Sprintf("log init failed. %s", err))
	}

	// Get non flag command line arguments and unmarshal in DB config struct
	// log.Println(flag.Args)
	// dbConfigStr := flag.Args()[0]
	// log.Println(dbConfigStr)
	// var dbconf conf.MongoDBConfig
	// if err := json.Unmarshal([]byte(dbConfigStr), &dbconf); err != nil {
	// 	log.Fatalf("Failed to parse DB configuration")
	// }
	// log.Println(dbconf)
	// if err := db.InitDBSession(dbconf); err != nil {
	// 	log.Fatalf("Unable to initialize DB")
	// }

	log.SetPrefix("[cep provider log] ")
	provd := &provider.CephProvider{}
	p := pie.NewProvider()
	if err := p.RegisterName("ceph", provd); err != nil {
		log.Fatalf("Failed to register plugin: %s", err)
	}
	p.ServeCodec(jsonrpc.NewServerCodec)
}
