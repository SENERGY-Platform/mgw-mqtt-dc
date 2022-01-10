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
	"encoding/json"
	"errors"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/topicdescription/generator/iotmodel"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
	"log"
	"net/http"
	"net/url"
	"runtime/debug"
)

const AttributeUsedForGenerator = "senergy/local-mqtt"

// GetDeviceInfos returns all device-types with attribute AttributeUsedForGenerator
// and devices matching a device-type
// if filterDevicesByAttribute != "" the devices will be filtered by AttributeUsedForGenerator == filterDevicesByAttribute
func GetDeviceInfos(token string, searchUrl string, repoUrl string, filterDevicesByAttribute string) (devices []iotmodel.Device, deviceTypes []iotmodel.DeviceType, err error) {
	type IdWrapper struct {
		Id string `json:"id"`
	}
	wrappedDeviceTypeIds := []IdWrapper{}
	err, _ = PermissionSearch(token, searchUrl, QueryMessage{
		Resource: "device-types",
		Find: &QueryFind{
			QueryListCommons: QueryListCommons{
				Limit:  9999,
				Offset: 0,
				Rights: "r",
				SortBy: "name",
			},
			Filter: &Selection{
				Condition: &ConditionConfig{
					Feature:   "features.attributes.key",
					Operation: QueryEqualOperation,
					Value:     AttributeUsedForGenerator,
				},
			},
		},
	}, &wrappedDeviceTypeIds)
	if err != nil {
		return devices, deviceTypes, err
	}

	dtIds := util.ListMap(wrappedDeviceTypeIds, func(from IdWrapper) string {
		return from.Id
	})

	if dtIds == nil {
		dtIds = []string{}
	}

	permsearchDevices := []iotmodel.Device{}
	deviceFilter := Selection{
		Condition: &ConditionConfig{
			Feature:   "features.device_type_id",
			Operation: QueryAnyValueInFeatureOperation,
			Value:     dtIds,
		},
	}
	if filterDevicesByAttribute != "" {
		deviceFilter = Selection{
			And: []Selection{
				deviceFilter,
				{
					Condition: &ConditionConfig{
						Feature:   "features.attributes.key",
						Operation: QueryEqualOperation,
						Value:     AttributeUsedForGenerator,
					},
				},
				{
					Condition: &ConditionConfig{
						Feature:   "features.attributes.value",
						Operation: QueryEqualOperation,
						Value:     filterDevicesByAttribute,
					},
				},
			},
		}
	}
	err, _ = PermissionSearch(token, searchUrl, QueryMessage{
		Resource: "devices",
		Find: &QueryFind{
			QueryListCommons: QueryListCommons{
				Limit:  9999,
				Offset: 0,
				Rights: "r",
				SortBy: "name",
			},
			Filter: &deviceFilter,
		},
	}, &permsearchDevices)

	if err != nil {
		return devices, deviceTypes, err
	}

	//local filter because filtering in permission-search may not be complete if device attributes contain Attributes{{Key:"foo", Value: filterDevicesByAttribute}, {Key:AttributeUsedForGenerator, Value: "bar"}}
	devices = util.ListFilter(permsearchDevices, func(d iotmodel.Device) bool {
		return filterDevicesByAttribute == "" || util.ListContains(d.Attributes, func(a iotmodel.Attribute) bool {
			return a.Key == AttributeUsedForGenerator && a.Value == filterDevicesByAttribute
		})
	})

	usedDeviceTypeIds := map[string]bool{}
	for _, d := range devices {
		usedDeviceTypeIds[d.DeviceTypeId] = true
	}

	for dtId, _ := range usedDeviceTypeIds {
		dt, err, _ := GetDeviceType(token, repoUrl, dtId)
		if err != nil {
			return devices, deviceTypes, err
		}
		deviceTypes = append(deviceTypes, dt)
	}
	return devices, deviceTypes, nil
}

func GetDeviceType(token string, repoUrl string, id string) (result iotmodel.DeviceType, err error, code int) {
	req, err := http.NewRequest("GET", repoUrl+"/device-types/"+url.PathEscape(id), nil)
	if err != nil {
		debug.PrintStack()
		return result, err, http.StatusInternalServerError
	}
	req.Header.Set("Authorization", token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		debug.PrintStack()
		return result, err, http.StatusInternalServerError
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)
		err = errors.New(buf.String())
		log.Println("ERROR: ", resp.StatusCode, err)
		debug.PrintStack()
		return result, err, resp.StatusCode
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		debug.PrintStack()
		return result, err, http.StatusInternalServerError
	}

	return result, nil, http.StatusOK
}
