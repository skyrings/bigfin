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

package handler

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
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

func HttpGet(url string) (*http.Response, error) {
	return http.Get(url)
}

func HttpPost(dummyUrl string, url string, contentType string, body io.Reader) (*http.Response, error) {
	client := &http.Client{}
	locjar := &CephApiJar{}
	locjar.jar = make(map[string][]*http.Cookie)
	client.Jar = locjar

	// Get the csrf token details
	resp, err := client.Get(dummyUrl)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error getting CSRF token: %v", err))
	}
	token := csrf_token(resp)

	// Create the request
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error forming the request: %v", err))
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-CSRFTOKEN", token)

	// Invoke the request
	resp, err = client.Do(req)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error executing the request: %v", err))
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return nil, errors.New("Failed")
	}
	return resp, nil
}

func csrf_token(resp *http.Response) (token string) {
	cookies := strings.Split(resp.Header["Set-Cookie"][0], ";")
	for _, cookie := range cookies {
		cookieFieldDet := strings.Split(cookie, "=")
		if cookieFieldDet[0] == "XSRF-TOKEN" {
			token = cookieFieldDet[1]
		}
	}
	return
}
