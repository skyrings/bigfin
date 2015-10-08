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
package utils

import (
	"encoding/json"
	"github.com/skyrings/skyring/models"
	"net"
)

func IsIPInSubnet(addr string, subnet string) (bool, error) {
	_, ipnet, err := net.ParseCIDR(subnet)
	if err != nil {
		return false, err
	}
	ip := net.ParseIP(addr)
	return ipnet.Contains(ip), nil
}

func WriteResponse(code int, msg string) []byte {
	var response models.RpcResponse
	response.Status.StatusCode = code
	response.Status.StatusMessage = msg
	ret_str, _ := json.Marshal(response)
	return []byte(ret_str)
}

