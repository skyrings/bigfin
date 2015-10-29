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
	"github.com/skyrings/bigfin/tools/logger"
	"github.com/skyrings/skyring/models"
	"github.com/skyrings/skyring/tools/task"
	"github.com/skyrings/skyring/tools/uuid"
	"math"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func IsIPInSubnet(addr string, subnet string) (bool, error) {
	_, ipnet, err := net.ParseCIDR(subnet)
	if err != nil {
		return false, err
	}
	ip := net.ParseIP(addr)
	return ipnet.Contains(ip), nil
}

func WriteResponse(code int, msg string) models.RpcResponse {
	var response models.RpcResponse
	response.Status.StatusCode = code
	response.Status.StatusMessage = msg
	return response
}

func WriteResponseWithData(code int, msg string, result []byte) models.RpcResponse {
	var response models.RpcResponse
	response.Status.StatusCode = code
	response.Status.StatusMessage = msg
	response.Data.RequestId = ""
	response.Data.Result = result
	return response
}

func WriteAsyncResponse(taskId uuid.UUID, msg string, result []byte) models.RpcResponse {
	var response models.RpcResponse
	response.Status.StatusCode = http.StatusAccepted
	response.Status.StatusMessage = msg
	response.Data.RequestId = taskId.String()
	response.Data.Result = result
	return response
}

func StringInSlice(value string, slice []string) bool {
	for _, item := range slice {
		if value == item {
			return true
		}
	}
	return false
}

func RandomNum(min, max int) int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(max-min) + min
}

func SizeFromStr(size string) uint64 {
	if strings.HasSuffix(size, "MB") {
		trimStr := strings.TrimSuffix(size, "MB")
		val, _ := strconv.Atoi(trimStr)
		return uint64(val * 1024)
	}
	if strings.HasSuffix(size, "mb") {
		trimStr := strings.TrimSuffix(size, "mb")
		val, _ := strconv.Atoi(trimStr)
		return uint64(val * 1024)
	}
	if strings.HasSuffix(size, "GB") {
		trimStr := strings.TrimSuffix(size, "GB")
		val, _ := strconv.Atoi(trimStr)
		return uint64(val * 1024 * 1024)
	}
	if strings.HasSuffix(size, "gb") {
		trimStr := strings.TrimSuffix(size, "gb")
		val, _ := strconv.Atoi(trimStr)
		return uint64(val * 1024 * 1024)
	}
	if strings.HasSuffix(size, "TB") {
		trimStr := strings.TrimSuffix(size, "TB")
		val, _ := strconv.Atoi(trimStr)
		return uint64(val * 1024 * 1024 * 1024)
	}
	if strings.HasSuffix(size, "tb") {
		trimStr := strings.TrimSuffix(size, "tb")
		val, _ := strconv.Atoi(trimStr)
		return uint64(val * 1024 * 1024 * 1024)
	}
	if strings.HasSuffix(size, "PB") {
		trimStr := strings.TrimSuffix(size, "PB")
		val, _ := strconv.Atoi(trimStr)
		return uint64(val * 1024 * 1024 * 1024 * 1024)
	}
	if strings.HasSuffix(size, "pb") {
		trimStr := strings.TrimSuffix(size, "pb")
		val, _ := strconv.Atoi(trimStr)
		return uint64(val * 1024 * 1024 * 1024 * 1024)
	}
	return uint64(0)
}

func NextTwosPower(num uint) uint {
	count := 1
	for {
		val := math.Pow(2, float64(count))
		if val >= float64(num) {
			return uint(val)
		}
		count += 1
	}
}

func FailTask(msg string, err error, t *task.Task) {
	logger.Get().Error("%s: %v", msg, err)
	t.UpdateStatus("Failed. error: %v", err)
	t.Done(models.TASK_STATUS_FAILURE)
}
