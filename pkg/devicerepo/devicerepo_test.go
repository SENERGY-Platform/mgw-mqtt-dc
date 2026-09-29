/*
 * Copyright 2026 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package devicerepo

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SENERGY-Platform/device-repository/lib/model"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
)

func TestExternalErrorMessages(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/device-types/invalid-json"):
			_, _ = w.Write([]byte("{"))
		case strings.HasPrefix(r.URL.Path, "/device-types/"):
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(time.Now().Format(time.RFC3339Nano) + " upstream unavailable"))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(time.Now().Format(time.RFC3339Nano) + " not found"))
		}
	}))
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()

	repo, err := New(RepoConfig{DeviceRepositoryUrl: server.URL, CacheDuration: "1s"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	unreachable, err := New(RepoConfig{DeviceRepositoryUrl: closed.URL, CacheDuration: "1s"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	check := func(name string, err error, expected string) {
		t.Helper()
		if err == nil {
			t.Errorf("%v: expected error", name)
			return
		}
		if actual := util.MgwErrorMessage(err); actual != expected {
			t.Errorf("%v: got %q, expected %q (full error: %v)", name, actual, expected, err)
		}
	}

	_, err = repo.GetDeviceType("dt")
	check("GetDeviceType status", err, "device-repository: unexpected response status (status code 502)")
	_, err = repo.GetDeviceType("invalid-json")
	check("GetDeviceType decode", err, "device-repository: unable to decode response")
	_, err = unreachable.GetDeviceType("dt")
	check("GetDeviceType unreachable", err, "device-repository: request failed")

	_, err, _ = repo.ListDevices("", model.DeviceListOptions{Limit: 10})
	check("ListDevices status", err, "device-repository: unexpected response (status code 404)")
	_, err, _ = unreachable.ListDevices("", model.DeviceListOptions{Limit: 10})
	check("ListDevices unreachable", err, "device-repository: request failed")
	_, err, _ = unreachable.ListDeviceTypes("", model.DeviceTypeListOptions{Limit: 10})
	check("ListDeviceTypes unreachable", err, "device-repository: request failed")
}
