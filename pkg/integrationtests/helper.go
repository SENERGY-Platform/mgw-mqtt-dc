/*
 * Copyright 2023 InfAI (CC SES)
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

package integrationtests

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/SENERGY-Platform/models/go/models"
	"io"
	"log"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func createTestMetadata(token string, managerUrl string, characteristics []models.Characteristic, concepts []models.Concept, functions []models.Function, protocols []models.Protocol, dts []models.DeviceType, devices []models.Device) func(t *testing.T) {
	return func(t *testing.T) {
		for _, c := range characteristics {
			t.Run("create characteristic "+c.Name, createTestCharacteristic(token, managerUrl, c))
		}

		for _, c := range concepts {
			t.Run("create concept "+c.Name, createTestConcept(token, managerUrl, c))
		}

		for _, p := range protocols {
			t.Run("create protocol "+p.Name, createTestProtocol(token, managerUrl, p))
		}

		for _, f := range functions {
			t.Run("create function "+f.Name, createTestFunction(token, managerUrl, f))
		}

		for _, dt := range dts {
			t.Run("create device-type "+dt.Name, createTestDeviceType(token, managerUrl, dt))
		}

		for _, d := range devices {
			t.Run("create device "+d.Name, createTestDevice(token, managerUrl, d))
		}
	}
}

func createTestProtocol(token string, managerUrl string, protocol models.Protocol) func(t *testing.T) {
	return func(t *testing.T) {
		requestBody := new(bytes.Buffer)
		err := json.NewEncoder(requestBody).Encode(protocol)
		if err != nil {
			t.Error(err)
			return
		}
		req, err := http.NewRequest("PUT", managerUrl+"/protocols/"+protocol.Id+"?wait=true", requestBody)
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

func createTestCharacteristic(token string, managerUrl string, c models.Characteristic) func(t *testing.T) {
	return func(t *testing.T) {
		requestBody := new(bytes.Buffer)
		err := json.NewEncoder(requestBody).Encode(c)
		if err != nil {
			t.Error(err)
			return
		}
		req, err := http.NewRequest("PUT", managerUrl+"/characteristics/"+c.Id+"?wait=true", requestBody)
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

func createTestConcept(token string, managerUrl string, c models.Concept) func(t *testing.T) {
	return func(t *testing.T) {
		requestBody := new(bytes.Buffer)
		err := json.NewEncoder(requestBody).Encode(c)
		if err != nil {
			t.Error(err)
			return
		}
		req, err := http.NewRequest("PUT", managerUrl+"/concepts/"+c.Id+"?wait=true", requestBody)
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

func createTestFunction(token string, managerUrl string, f models.Function) func(t *testing.T) {
	return func(t *testing.T) {
		requestBody := new(bytes.Buffer)
		err := json.NewEncoder(requestBody).Encode(f)
		if err != nil {
			t.Error(err)
			return
		}
		req, err := http.NewRequest("PUT", managerUrl+"/functions/"+f.Id+"?wait=true", requestBody)
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

func createTestDeviceType(token string, managerUrl string, dt models.DeviceType) func(t *testing.T) {
	return func(t *testing.T) {
		requestBody := new(bytes.Buffer)
		err := json.NewEncoder(requestBody).Encode(dt)
		if err != nil {
			t.Error(err)
			return
		}
		req, err := http.NewRequest("PUT", managerUrl+"/device-types/"+dt.Id+"?wait=true", requestBody)
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

func createTestDevice(token string, managerUrl string, device models.Device) func(t *testing.T) {
	return func(t *testing.T) {
		requestBody := new(bytes.Buffer)
		err := json.NewEncoder(requestBody).Encode(device)
		if err != nil {
			t.Error(err)
			return
		}
		req, err := http.NewRequest("PUT", managerUrl+"/devices/"+device.Id+"?wait=true", requestBody)
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

func waitForCqrs(token string, searchUrl string, managerUrl string, resource string, id string) func(t *testing.T) {
	return func(t *testing.T) {
		var err error
		for i := 0; i < 10; i++ {
			err = headPermissionSearch(token, searchUrl, resource, id)
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
			err = headDeviceManager(token, managerUrl, resource, id)
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

func headPermissionSearch(token string, searchUrl string, resource string, id string) error {
	endpoint := searchUrl + "/v3/resources/" + resource + "/" + url.PathEscape(id)
	log.Println("HEAD", endpoint)
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

func headDeviceManager(token string, managerUrl string, resource string, id string) error {
	endpoint := managerUrl + "/" + resource + "/" + url.PathEscape(id)
	log.Println("GET", endpoint)
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
