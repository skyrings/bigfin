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

package client

import (
	"crypto/tls"
	"net/http"
	"net/url"
)

var (
	httpSession *http.Client
)

type CephApiJar struct {
	jar map[string][]*http.Cookie
}

func (p *CephApiJar) SetCookies(u *url.URL, cookies []*http.Cookie) {
	p.jar[u.Host] = cookies
}

func (p *CephApiJar) Cookies(u *url.URL) []*http.Cookie {
	return p.jar[u.Host]
}

func InitCephApiSession() {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpSession = &http.Client{Transport: tr}
	locjar := &CephApiJar{}
	locjar.jar = make(map[string][]*http.Cookie)
	httpSession.Jar = locjar
}

func GetCephApiSession() *http.Client {
	return httpSession
}
