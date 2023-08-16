/*
 * Copyright 2022 InfAI (CC SES)
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

package generator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/devicerepo"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/devicerepo/auth"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/integrationtests/docker"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
	"github.com/SENERGY-Platform/models/go/models"
	"io"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestGetDeviceInfos(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	keycloakUrl, err := docker.Keycloak(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	managerUrl, repoUrl, searchUrl, err := docker.DeviceManagerWithDependencies(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	a := &auth.Auth{Credentials: auth.Credentials{
		AuthEndpoint:     keycloakUrl,
		AuthClientId:     "client-connector-lib",
		AuthClientSecret: "",
		Username:         "testuser",
		Password:         "testpw",
	}}

	token, err := a.EnsureAccess()
	if err != nil {
		t.Error(err)
		return
	}

	repo, err := devicerepo.New(devicerepo.RepoConfig{
		DeviceRepositoryUrl: repoUrl,
		CacheDuration:       "10s",
		FallbackFile:        "",
	}, a)
	if err != nil {
		t.Error(err)
		return
	}

	t.Run("create test iot", testGetDeviceInfos_createTestIot(a, managerUrl, searchUrl))

	t.Run("check perm-search ready", func(t *testing.T) {
		temp := []map[string]interface{}{}
		err, _ = PermissionSearch(token, searchUrl, QueryMessage{
			Resource: "device-types",
			Find: &QueryFind{
				QueryListCommons: QueryListCommons{
					Limit:  9999,
					Offset: 0,
					Rights: "r",
					SortBy: "name",
				},
				Search: "",
			},
		}, &temp)
		if err != nil {
			t.Error(err)
		}
		if len(temp) != 4 {
			t.Error(len(temp), temp)
		}
	})

	t.Run("call without filter", testGetDeviceInfos_check(repo, searchUrl, "", []string{
		"urn:infai:ses:device:found_and_used_with_attr_foo",
		"urn:infai:ses:device:found_and_used_with_attr_bar",
		"urn:infai:ses:device:found_and_used_without_attr",
	}, []string{
		"urn:infai:ses:device-type:found_and_used",
	}))

	t.Run("call without filter", testGetDeviceInfos_check(repo, searchUrl, "foo", []string{
		"urn:infai:ses:device:found_and_used_with_attr_foo",
	}, []string{
		"urn:infai:ses:device-type:found_and_used",
	}))

	t.Run("call without filter", testGetDeviceInfos_check(repo, searchUrl, "bar", []string{
		"urn:infai:ses:device:found_and_used_with_attr_bar",
	}, []string{
		"urn:infai:ses:device-type:found_and_used",
	}))
}

func testGetDeviceInfos_check(deviceRepo *devicerepo.DeviceRepo, searchUrl string, withAttrFilter string, expectedDeviceIds []string, expectedDeviceTypeIds []string) func(t *testing.T) {
	return func(t *testing.T) {
		devices, deviceTypes, err := GetDeviceInfos(deviceRepo, searchUrl, withAttrFilter)
		if err != nil {
			t.Error(err)
			return
		}
		deviceIds := util.ListMap(devices, func(from models.Device) string {
			return from.Id
		})
		deviceTypesIds := util.ListMap(deviceTypes, func(from models.DeviceType) string {
			return from.Id
		})
		sort.Strings(expectedDeviceIds)
		sort.Strings(expectedDeviceTypeIds)
		sort.Strings(deviceIds)
		sort.Strings(deviceTypesIds)
		if !reflect.DeepEqual(expectedDeviceTypeIds, deviceTypesIds) {
			t.Error("\n", expectedDeviceTypeIds, "\n", deviceTypesIds)
		}
		if !reflect.DeepEqual(expectedDeviceIds, deviceIds) {
			t.Error("\n", expectedDeviceIds, "\n", deviceIds)
		}

		for _, dt := range deviceTypes {
			if len(dt.Services) != 1 {
				t.Error(dt, dt.Services)
			}
			for _, s := range dt.Services {
				if len(s.Attributes) != 1 {
					t.Error(dt, s, s.Attributes)
				}
			}
		}
	}
}

func testGetDeviceInfos_createTestIot(auth *auth.Auth, managerUrl string, searchUrl string) func(t *testing.T) {
	return func(t *testing.T) {

		t.Run("create protocol", testGetDeviceInfos_createTestPorotocol(auth, managerUrl, models.Protocol{
			Id:      "urn:infai:ses:protocol:p1",
			Name:    "p1",
			Handler: "p1",
			ProtocolSegments: []models.ProtocolSegment{
				{
					Id:   "urn:infai:ses:protocol-segment:ps1",
					Name: "ps1",
				},
			},
		}))

		t.Run("wait for protocol cqrs", waitForCqrs(auth, searchUrl, managerUrl, "protocols", "urn:infai:ses:protocol:p1"))

		t.Run("create device-type found_and_used", testGetDeviceInfos_createTestDeviceType(auth, managerUrl, models.DeviceType{
			Id:   "urn:infai:ses:device-type:found_and_used",
			Name: "found_and_used",
			Attributes: []models.Attribute{
				{Key: AttributeUsedForGenerator, Value: "true"},
			},
			Services: []models.Service{{
				Id:          "urn:infai:ses:s1",
				LocalId:     "s1",
				Name:        "s1",
				Description: "service-desc",
				ProtocolId:  "urn:infai:ses:protocol:p1",
				Attributes: []models.Attribute{
					{Key: "test", Value: "42"},
				},
			}},
		}))

		t.Run("create device-type found", testGetDeviceInfos_createTestDeviceType(auth, managerUrl, models.DeviceType{
			Id:   "urn:infai:ses:device-type:found",
			Name: "found",
			Attributes: []models.Attribute{
				{Key: AttributeUsedForGenerator, Value: "true"},
			},
			Services: []models.Service{{
				Id:          "urn:infai:ses:s2",
				LocalId:     "s2",
				Name:        "s2",
				Description: "service-desc",
				ProtocolId:  "urn:infai:ses:protocol:p1",
				Attributes: []models.Attribute{
					{Key: "test", Value: "42"},
				},
			}},
		}))

		t.Run("create device-type used", testGetDeviceInfos_createTestDeviceType(auth, managerUrl, models.DeviceType{
			Id:         "urn:infai:ses:device-type:used",
			Name:       "used",
			Attributes: []models.Attribute{},
			Services: []models.Service{{
				Id:          "urn:infai:ses:s3",
				LocalId:     "s3",
				Name:        "s3",
				Description: "service-desc",
				ProtocolId:  "urn:infai:ses:protocol:p1",
				Attributes: []models.Attribute{
					{Key: "test", Value: "42"},
				},
			}},
		}))

		t.Run("create device-type unused", testGetDeviceInfos_createTestDeviceType(auth, managerUrl, models.DeviceType{
			Id:         "urn:infai:ses:device-type:unused",
			Name:       "unused",
			Attributes: []models.Attribute{},
			Services: []models.Service{{
				Id:          "urn:infai:ses:s4",
				LocalId:     "s4",
				Name:        "s4",
				Description: "service-desc",
				ProtocolId:  "urn:infai:ses:protocol:p1",
				Attributes: []models.Attribute{
					{Key: "test", Value: "42"},
				},
			}},
		}))

		t.Run("wait for device-type cqrs", waitForCqrs(auth, searchUrl, managerUrl, "device-types", "urn:infai:ses:device-type:unused"))

		t.Run("create device found_and_used_with_attr_foo", testGetDeviceInfos_createTestDevice(auth, managerUrl, models.Device{
			Id:           "urn:infai:ses:device:found_and_used_with_attr_foo",
			Name:         "found_and_used_with_attr_foo",
			LocalId:      "found_and_used_with_attr_foo",
			DeviceTypeId: "urn:infai:ses:device-type:found_and_used",
			Attributes: []models.Attribute{
				{Key: AttributeUsedForGenerator, Value: "foo"},
			},
		}))

		t.Run("create device found_and_used_with_attr_bar", testGetDeviceInfos_createTestDevice(auth, managerUrl, models.Device{
			Id:           "urn:infai:ses:device:found_and_used_with_attr_bar",
			Name:         "found_and_used_with_attr_bar",
			LocalId:      "found_and_used_with_attr_bar",
			DeviceTypeId: "urn:infai:ses:device-type:found_and_used",
			Attributes: []models.Attribute{
				{Key: AttributeUsedForGenerator, Value: "bar"},
			},
		}))

		t.Run("create device found_and_used_without_attr", testGetDeviceInfos_createTestDevice(auth, managerUrl, models.Device{
			Id:           "urn:infai:ses:device:found_and_used_without_attr",
			Name:         "found_and_used_without_attr",
			LocalId:      "found_and_used_without_attr",
			DeviceTypeId: "urn:infai:ses:device-type:found_and_used",
		}))

		t.Run("create device used_with_attr_foo", testGetDeviceInfos_createTestDevice(auth, managerUrl, models.Device{
			Id:           "urn:infai:ses:device:used_with_attr_foo",
			Name:         "used_with_attr_foo",
			LocalId:      "used_with_attr_foo",
			DeviceTypeId: "urn:infai:ses:device-type:used",
			Attributes: []models.Attribute{
				{Key: AttributeUsedForGenerator, Value: "foo"},
			},
		}))

		t.Run("create device used_with_attr_bar", testGetDeviceInfos_createTestDevice(auth, managerUrl, models.Device{
			Id:           "urn:infai:ses:device:used_with_attr_bar",
			Name:         "used_with_attr_bar",
			LocalId:      "used_with_attr_bar",
			DeviceTypeId: "urn:infai:ses:device-type:used",
			Attributes: []models.Attribute{
				{Key: AttributeUsedForGenerator, Value: "bar"},
			},
		}))

		t.Run("create device used_without_attr", testGetDeviceInfos_createTestDevice(auth, managerUrl, models.Device{
			Id:           "urn:infai:ses:device:used_without_attr",
			Name:         "used_without_attr",
			LocalId:      "used_without_attr",
			DeviceTypeId: "urn:infai:ses:device-type:used",
		}))

		t.Run("wait for device cqrs", waitForCqrs(auth, searchUrl, managerUrl, "devices", "urn:infai:ses:device:used_without_attr"))
	}
}

func testGetDeviceInfos_createTestPorotocol(auth *auth.Auth, managerUrl string, protocol models.Protocol) func(t *testing.T) {
	return func(t *testing.T) {
		token, err := auth.EnsureAccess()
		if err != nil {
			t.Error(err)
			return
		}
		requestBody := new(bytes.Buffer)
		err = json.NewEncoder(requestBody).Encode(protocol)
		if err != nil {
			t.Error(err)
			return
		}
		req, err := http.NewRequest("PUT", managerUrl+"/protocols/"+protocol.Id, requestBody)
		if err != nil {
			t.Error(err)
			return
		}
		req.Header.Set("Authorization", token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Error(err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			temp, _ := io.ReadAll(resp.Body)
			t.Error(resp.Status, resp.StatusCode, string(temp))
			return
		}
	}
}

func testGetDeviceInfos_createTestDeviceType(auth *auth.Auth, managerUrl string, dt models.DeviceType) func(t *testing.T) {
	return func(t *testing.T) {
		token, err := auth.EnsureAccess()
		if err != nil {
			t.Error(err)
			return
		}
		requestBody := new(bytes.Buffer)
		err = json.NewEncoder(requestBody).Encode(dt)
		if err != nil {
			t.Error(err)
			return
		}
		req, err := http.NewRequest("PUT", managerUrl+"/device-types/"+dt.Id, requestBody)
		if err != nil {
			t.Error(err)
			return
		}
		req.Header.Set("Authorization", token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Error(err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			temp, _ := io.ReadAll(resp.Body)
			t.Error(resp.Status, resp.StatusCode, string(temp))
			return
		}
	}
}

func testGetDeviceInfos_createTestDevice(auth *auth.Auth, managerUrl string, device models.Device) func(t *testing.T) {
	return func(t *testing.T) {
		token, err := auth.EnsureAccess()
		if err != nil {
			t.Error(err)
			return
		}
		requestBody := new(bytes.Buffer)
		err = json.NewEncoder(requestBody).Encode(device)
		if err != nil {
			t.Error(err)
			return
		}
		req, err := http.NewRequest("PUT", managerUrl+"/devices/"+device.Id, requestBody)
		if err != nil {
			t.Error(err)
			return
		}
		req.Header.Set("Authorization", token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Error(err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			temp, _ := io.ReadAll(resp.Body)
			t.Error(resp.Status, resp.StatusCode, string(temp))
			return
		}
	}
}

func waitForCqrs(auth *auth.Auth, searchUrl string, managerUrl string, resource string, id string) func(t *testing.T) {
	return func(t *testing.T) {
		var err error
		for i := 0; i < 10; i++ {
			err = headPermissionSearch(auth, searchUrl, resource, id)
			if err != nil {
				time.Sleep(1 * time.Second)
			} else {
				break
			}
		}
		if err != nil {
			t.Error(err)
			return
		}
		for i := 0; i < 10; i++ {
			err = headDeviceManager(auth, managerUrl, resource, id)
			if err != nil {
				time.Sleep(1 * time.Second)
			} else {
				break
			}
		}
		if err != nil {
			t.Error(err)
			return
		}
	}
}

func headPermissionSearch(auth *auth.Auth, searchUrl string, resource string, id string) error {
	endpoint := searchUrl + "/v3/resources/" + resource + "/" + url.PathEscape(id)
	log.Println("HEAD", endpoint)
	token, err := auth.EnsureAccess()
	if err != nil {
		return err
	}
	req, err := http.NewRequest("HEAD", endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return errors.New(resp.Status)
	}
	return nil
}

func headDeviceManager(auth *auth.Auth, managerUrl string, resource string, id string) error {
	endpoint := managerUrl + "/" + resource + "/" + url.PathEscape(id)
	log.Println("GET", endpoint)
	token, err := auth.EnsureAccess()
	if err != nil {
		return err
	}
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return errors.New(resp.Status)
	}
	return nil
}
