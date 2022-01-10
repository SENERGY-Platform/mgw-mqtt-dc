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
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/topicdescription/generator/iotmodel"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/topicdescription/model"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
	"log"
	"text/template"
)

const CommandAttribute = "senergy/local-mqtt/cmd-topic-tmpl"
const ResponseAttribute = "senergy/local-mqtt/resp-topic-tmpl"
const EventAttribute = "senergy/local-mqtt/event-topic-tmpl"

var TemplateLocalDeviceIdPlaceholders = []string{"Device", "LocalDeviceId"}
var TemplateLocalServiceIdPlaceholders = []string{"Service", "LocalServiceId"}

func GenerateTopicDescriptions(devices []iotmodel.Device, deviceTypes []iotmodel.DeviceType) (result []model.TopicDescription) {
	dtIndex := map[string]iotmodel.DeviceType{}
	for _, dt := range deviceTypes {
		dtIndex[dt.Id] = dt
	}
	for _, d := range devices {
		dt, ok := dtIndex[d.DeviceTypeId]
		if ok {
			result = append(result, GenerateDeviceTopicDescriptions(d, dt)...)
		}
	}
	util.ListSort(result, func(a model.TopicDescription, b model.TopicDescription) bool {
		return a.GetTopic() < b.GetTopic()
	})
	return result
}

func GenerateDeviceTopicDescriptions(device iotmodel.Device, deviceType iotmodel.DeviceType) (result []model.TopicDescription) {
	for _, service := range deviceType.Services {
		result = append(result, GenerateServiceTopicDescriptions(device, service)...)
	}
	return result
}

func GenerateServiceTopicDescriptions(device iotmodel.Device, service iotmodel.Service) (result []model.TopicDescription) {
	result = append(result, GenerateEventServiceTopicDescriptions(device, service)...)
	result = append(result, GenerateCommandServiceTopicDescriptions(device, service)...)
	return result
}

func GenerateCommandServiceTopicDescriptions(device iotmodel.Device, service iotmodel.Service) (result []model.TopicDescription) {
	cmdTopicTempl, found := GetAttributeValue(service.Attributes, CommandAttribute)
	if !found {
		return result
	}
	cmdTopic, err := GenerateTopic(cmdTopicTempl, device.LocalId, service.LocalId)
	if err != nil {
		log.Println("WARNING: invalid command topic template", cmdTopicTempl, "in", device.Name, device.Id, device.LocalId, service.Name, service.Id, service.LocalId)
		return result
	}
	temp := model.TopicDescription{
		CmdTopic:       cmdTopic,
		RespTopic:      "",
		DeviceTypeId:   device.DeviceTypeId,
		DeviceLocalId:  device.LocalId,
		ServiceLocalId: service.LocalId,
		DeviceName:     device.Name,
	}
	respTopic, found := GetAttributeValue(service.Attributes, ResponseAttribute)
	if found {
		temp.RespTopic, err = GenerateTopic(respTopic, device.LocalId, service.LocalId)
		if err != nil {
			log.Println("WARNING: invalid response topic template", cmdTopicTempl, "in", device.Name, device.Id, device.LocalId, service.Name, service.Id, service.LocalId)
			return result
		}
	}
	return []model.TopicDescription{temp}
}

func GenerateEventServiceTopicDescriptions(device iotmodel.Device, service iotmodel.Service) (result []model.TopicDescription) {
	eventTopicTempl, found := GetAttributeValue(service.Attributes, EventAttribute)
	if !found {
		return result
	}
	eventTopic, err := GenerateTopic(eventTopicTempl, device.LocalId, service.LocalId)
	if err != nil {
		log.Println("WARNING: invalid event topic template", eventTopic, "in", device.Name, device.Id, device.LocalId, service.Name, service.Id, service.LocalId)
		return result
	}
	return []model.TopicDescription{{
		EventTopic:     eventTopic,
		DeviceTypeId:   device.DeviceTypeId,
		DeviceLocalId:  device.LocalId,
		ServiceLocalId: service.LocalId,
		DeviceName:     device.Name,
	}}
}

func GetAttributeValue(attributes []iotmodel.Attribute, key string) (result string, found bool) {
	for _, attr := range attributes {
		if attr.Key == key {
			return attr.Value, true
		}
	}
	return result, false
}

func GenerateTopic(topicTemplate string, deviceId string, serviceId string) (result string, err error) {
	values := map[string]string{}
	for _, placeholder := range TemplateLocalDeviceIdPlaceholders {
		values[placeholder] = deviceId
	}
	for _, placeholder := range TemplateLocalServiceIdPlaceholders {
		values[placeholder] = serviceId
	}
	var temp bytes.Buffer
	err = template.Must(template.New("").Parse(topicTemplate)).Execute(&temp, values)
	if err != nil {
		return "", err
	}
	return temp.String(), nil
}
