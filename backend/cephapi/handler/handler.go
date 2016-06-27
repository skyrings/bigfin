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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/skyrings/bigfin/backend/cephapi/client"
	"github.com/skyrings/bigfin/backend/cephapi/models"
	"github.com/skyrings/skyring-common/tools/logger"
	"io"
	"net/http"
	"net/url"
	"regexp"
)

var loggedIn bool

func HttpGet(mon, url string) (*http.Response, error) {
	session := client.GetCephApiSession()

	var loginUrl = fmt.Sprintf(
		"https://%s:%d/%s/v%d/auth/login/",
		mon,
		models.CEPH_API_PORT,
		models.CEPH_API_DEFAULT_PREFIX,
		models.CEPH_API_DEFAULT_VERSION)

	if !loggedIn {
		if err := login(session, mon, loginUrl); err != nil {
			return nil, fmt.Errorf("Failed to login: %v", err)
		}
	}

	csrf_token := csrfTokenFromSession(session, loginUrl)

	resp, err := doRequest(
		session,
		csrf_token,
		"GET",
		"application/json",
		url,
		bytes.NewBuffer([]byte{}))
	if err != nil {
		return nil, fmt.Errorf("Error executing request: %v", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		resp.Body.Close()
		return nil, fmt.Errorf(resp.Status)
	} else if resp.StatusCode == http.StatusForbidden {
		logger.Get().Warning("Session seems invalidated. Trying to login again.")
		if err := login(session, mon, loginUrl); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("Failed to relogin")
		}
		return doRequest(session, csrf_token, "GET", "application/json", url, bytes.NewBuffer([]byte{}))
	}

	return resp, nil
}

func invokeUpdateRestApi(method string, mon string, url string, contentType string, body io.Reader) (*http.Response, error) {
	var loginUrl = fmt.Sprintf(
		"https://%s:%d/%s/v%d/auth/login/",
		mon,
		models.CEPH_API_PORT,
		models.CEPH_API_DEFAULT_PREFIX,
		models.CEPH_API_DEFAULT_VERSION)

	session := client.GetCephApiSession()

	if !loggedIn {
		if err := login(session, mon, loginUrl); err != nil {
			return nil, fmt.Errorf("Failed to login: %v", err)
		}
	}

	csrf_token := csrfTokenFromSession(session, loginUrl)

	resp, err := doRequest(
		session,
		csrf_token,
		method,
		contentType,
		url,
		body)
	if err != nil {
		return nil, fmt.Errorf("Error executing request: %v", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		resp.Body.Close()
		return nil, errors.New("Failed")
	} else if resp.StatusCode == http.StatusForbidden {
		logger.Get().Warning("Session seems invalidated. Trying to login again.")
		if err := login(session, mon, loginUrl); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("Failed to relogin")
		}
		return doRequest(session, csrf_token, method, contentType, url, body)
	}

	return resp, nil
}

func doRequest(
	session *http.Client,
	token string,
	method string,
	contentType string,
	url string,
	body io.Reader) (*http.Response, error) {

	// Create the request
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, fmt.Errorf("Error forming the request: %v", err)
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Referer", url)
	req.Header.Set("X-XSRF-TOKEN", token)

	// Invoke the request
	resp, err := session.Do(req)
	if err != nil {
		resp.Body.Close()
		return nil, fmt.Errorf("Error executing the request: %v", err)
	}

	return resp, nil
}

func HttpPost(mon string, url string, contentType string, body io.Reader) (*http.Response, error) {
	return invokeUpdateRestApi("POST", mon, url, contentType, body)
}

func HttpPatch(mon string, url string, contentType string, body io.Reader) (*http.Response, error) {
	return invokeUpdateRestApi("PATCH", mon, url, contentType, body)
}

func HttpDelete(mon string, url string, contentType string, body io.Reader) (*http.Response, error) {
	return invokeUpdateRestApi("DELETE", mon, url, contentType, body)
}

func csrf_token(resp *http.Response) (token string) {
	regex, _ := regexp.Compile(`XSRF-TOKEN=([^;]+)?`)
	for _, entry := range resp.Header["Set-Cookie"] {
		matches := regex.FindStringSubmatch(entry)
		if len(matches) > 1 {
			token = matches[1]
			return
		}
	}
	return
}

func csrfTokenFromSession(session *http.Client, loginUrl string) string {
	u, _ := url.Parse(loginUrl)
	cookies := session.Jar.Cookies(u)
	for _, entry := range cookies {
		if entry.Name == "XSRF-TOKEN" {
			return entry.Value
		}
	}
	return ""
}

func login(session *http.Client, mon string, loginUrl string) error {
	// Get the csrf token details
	resp, err := session.Get(loginUrl)
	if err != nil {
		resp.Body.Close()
		return fmt.Errorf("Error running dummy url to get csrf token: %v", err)
	}
	token := csrf_token(resp)
	resp.Body.Close()

	// Login
	reqData := make(map[string]interface{})
	reqData["username"] = "admin"
	reqData["password"] = "admin"
	buf, err := json.Marshal(reqData)
	if err != nil {
		return fmt.Errorf("Error forming login request: %v", err)
	}
	req, err := http.NewRequest("POST", loginUrl, bytes.NewBuffer(buf))
	if err != nil {
		return fmt.Errorf("Error forming login request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Referer", loginUrl)
	req.Header.Set("X-XSRF-TOKEN", token)
	resp1, err1 := session.Do(req)
	if err1 != nil {
		resp1.Body.Close()
		return fmt.Errorf("Error logging in: %v", err1)
	}

	resp1.Body.Close()
	loggedIn = true
	return nil
}
