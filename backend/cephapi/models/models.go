// Copyright 2015 Red Hat, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package models

type CephCluster struct {
	Name       string `json:"name"`
	Id         string `json:"id"`
	UpdateTime string `json:"update_time"`
}

type CephAsyncRequest struct {
	RequestId string `json:"request_id"`
}

type CephRequestStatus struct {
	Id           string `json:"id"`
	State        string `json:"state"`
	Headline     string `json:"headline"`
	Status       string `json:"status"`
	Error        bool   `json:"error"`
	ErrorMessage string `json:"error_message"`
}
type PgSummary struct {
	ByOSD  map[string]map[string]uint64 `json:"by_osd"`
	ByPool map[string]map[string]uint64 `json:"by_pool"`
	All    map[string]uint64            `json:"all"`
}
