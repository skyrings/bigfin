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
	"github.com/natefinch/pie"
	"github.com/skyrings/bigfin/provider"
	"log"
	"net/rpc/jsonrpc"
)

type CephProvider struct {}

func main() {
	log.SetPrefix("[cep provider log] ")
	provd := &provider.CephProvider{}
	p := pie.NewProvider()
	if err := p.RegisterName("ceph", provd); err != nil {
		log.Fatalf("Failed to register plugin: %s", err)
	}
	p.ServeCodec(jsonrpc.NewServerCodec)
}
