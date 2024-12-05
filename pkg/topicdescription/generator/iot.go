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
	"github.com/SENERGY-Platform/device-repository/lib/client"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/devicerepo"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/service-commons/pkg/jwt"
	"log"
)

const AttributeUsedForGenerator = "senergy/local-mqtt"

// GetDeviceInfos returns all device-types with attribute AttributeUsedForGenerator
// and devices matching a device-type
// if filterDevicesByAttribute != "" the devices will be filtered by AttributeUsedForGenerator == filterDevicesByAttribute
func GetDeviceInfos(repo *devicerepo.DeviceRepo, filterDevicesByAttribute string) (devices []models.Device, deviceTypes []models.DeviceType, err error) {
	token, err := repo.GetToken()
	if err != nil {
		return devices, deviceTypes, err
	}
	deviceTypes, err, _ = repo.ListDeviceTypes(token, client.DeviceTypeListOptions{
		Limit:         9999,
		Offset:        0,
		AttributeKeys: []string{AttributeUsedForGenerator},
	})
	if err != nil {
		return devices, deviceTypes, err
	}

	dtIds := util.ListMap(deviceTypes, func(from models.DeviceType) string {
		return from.Id
	})

	if dtIds == nil {
		dtIds = []string{}
	}

	deviceListOptions := client.DeviceListOptions{
		DeviceTypeIds:   dtIds,
		Limit:           9999,
		Offset:          0,
		AttributeKeys:   nil,
		AttributeValues: nil,
	}
	if filterDevicesByAttribute != "" {
		deviceListOptions.AttributeKeys = append(deviceListOptions.AttributeKeys, AttributeUsedForGenerator)
		deviceListOptions.AttributeValues = append(deviceListOptions.AttributeValues, filterDevicesByAttribute)
	}
	devices, err, _ = repo.ListDevices(token, deviceListOptions)
	if err != nil {
		return devices, deviceTypes, err
	}

	expectedOwnerJwt, err := jwt.Parse(token)
	if err != nil {
		return devices, deviceTypes, err
	}
	expectedOwnerId := expectedOwnerJwt.GetUserId()

	log.Println("filter devices with different owner as", expectedOwnerId)
	devices = util.ListFilter(devices, func(d models.Device) bool {
		keep := d.OwnerId == expectedOwnerId
		if !keep {
			log.Println("ignore", d.Id, d.LocalId, d.Name, "because", d.OwnerId, "!=", expectedOwnerId)
		}
		return keep
	})

	//local filter because filtering in permission-search may not be complete if device attributes contain Attributes{{Key:"foo", Value: filterDevicesByAttribute}, {Key:AttributeUsedForGenerator, Value: "bar"}}
	devices = util.ListFilter(devices, func(d models.Device) bool {
		return filterDevicesByAttribute == "" || util.ListContains(d.Attributes, func(a models.Attribute) bool {
			return a.Key == AttributeUsedForGenerator && a.Value == filterDevicesByAttribute
		})
	})

	usedDeviceTypeIds := map[string]bool{}
	for _, d := range devices {
		usedDeviceTypeIds[d.DeviceTypeId] = true
	}
	deviceTypes = util.ListFilter(deviceTypes, func(d models.DeviceType) bool {
		return usedDeviceTypeIds[d.Id]
	})
	return devices, deviceTypes, nil
}
