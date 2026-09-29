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
	"log/slog"
	"slices"
	"strings"
	"text/template"
	"text/template/parse"

	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/topicdescription/model"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
	"github.com/SENERGY-Platform/models/go/models"
)

const CommandAttribute = "senergy/local-mqtt/cmd-topic-tmpl"
const ResponseAttribute = "senergy/local-mqtt/resp-topic-tmpl"
const EventAttribute = "senergy/local-mqtt/event-topic-tmpl"

var TemplateLocalDeviceIdPlaceholders = []string{"Device", "LocalDeviceId"}
var TemplateLocalServiceIdPlaceholders = []string{"Service", "LocalServiceId"}

func GenerateTopicDescriptions(devices []models.Device, deviceTypes []models.DeviceType, truncateDevicePrefix string) (result []model.TopicDescription) {
	dtIndex := map[string]models.DeviceType{}
	for _, dt := range deviceTypes {
		dtIndex[dt.Id] = dt
	}
	for _, d := range devices {
		dt, ok := dtIndex[d.DeviceTypeId]
		if ok {
			result = append(result, GenerateDeviceTopicDescriptions(d, dt, truncateDevicePrefix)...)
		}
	}
	util.ListSort(result, func(a model.TopicDescription, b model.TopicDescription) bool {
		return a.GetTopic() < b.GetTopic()
	})
	return FilterDuplicates(result)
}

func GenerateDeviceTopicDescriptions(device models.Device, deviceType models.DeviceType, truncateDevicePrefix string) (result []model.TopicDescription) {
	for _, service := range deviceType.Services {
		result = append(result, GenerateServiceTopicDescriptions(device, service, truncateDevicePrefix)...)
	}
	return result
}

func GenerateServiceTopicDescriptions(device models.Device, service models.Service, truncateDevicePrefix string) (result []model.TopicDescription) {
	result = append(result, GenerateEventServiceTopicDescriptions(device, service, truncateDevicePrefix)...)
	result = append(result, GenerateCommandServiceTopicDescriptions(device, service, truncateDevicePrefix)...)
	return result
}

const DisplayNameAttributeName = "shared/nickname"

func GenerateCommandServiceTopicDescriptions(device models.Device, service models.Service, truncateDevicePrefix string) (result []model.TopicDescription) {
	cmdTopicTempl, found := GetAttributeValue(service.Attributes, CommandAttribute)
	if !found {
		return result
	}
	cmdTopic, missingKeys, err := GenerateTopic(cmdTopicTempl, device.LocalId, service.LocalId, truncateDevicePrefix, device.Attributes)
	if err != nil {
		slog.Warn("invalid command topic template", "error", err, "template", cmdTopicTempl, "device", device.Name, "device_id", device.Id, "local_device_id", device.LocalId, "service", service.Name, "service_id", service.Id, "local_service_id", service.LocalId)
		return result
	}
	if len(missingKeys) > 0 {
		slog.Warn("command topic template uses missing device attributes", "missing_keys", missingKeys, "template", cmdTopicTempl, "topic", cmdTopic, "device", device.Name, "device_id", device.Id, "local_device_id", device.LocalId, "service", service.Name, "service_id", service.Id, "local_service_id", service.LocalId)
	}
	temp := model.TopicDescription{
		CmdTopic:       cmdTopic,
		RespTopic:      "",
		DeviceTypeId:   device.DeviceTypeId,
		DeviceLocalId:  device.LocalId,
		ServiceLocalId: service.LocalId,
		DeviceName:     device.Name,
	}
	if temp.DeviceName == "" {
		for _, attr := range service.Attributes {
			if attr.Key == DisplayNameAttributeName {
				temp.DeviceName = attr.Value
				break
			}
		}
	}
	if temp.DeviceName == "" {
		temp.DeviceName = "unknown name"
	}
	for _, attr := range service.Attributes {
		if attr.Key == model.TransformerJsonUnwrapInput || attr.Key == model.TransformerJsonUnwrapOutput {
			paths := strings.Split(attr.Value, ",")
			for _, path := range paths {
				path = strings.TrimSpace(path)
				temp.Transformations = append(temp.Transformations, model.Transformation{
					Path:           path,
					Transformation: attr.Key,
				})
			}
		}
	}
	slices.SortFunc(temp.Transformations, func(a, b model.Transformation) int {
		return strings.Compare(a.Path, b.Path)
	})
	respTopic, found := GetAttributeValue(service.Attributes, ResponseAttribute)
	if found {
		temp.RespTopic, missingKeys, err = GenerateTopic(respTopic, device.LocalId, service.LocalId, truncateDevicePrefix, device.Attributes)
		if err != nil {
			slog.Warn("invalid response topic template", "error", err, "template", respTopic, "device", device.Name, "device_id", device.Id, "local_device_id", device.LocalId, "service", service.Name, "service_id", service.Id, "local_service_id", service.LocalId)
			return result
		}
		if len(missingKeys) > 0 {
			slog.Warn("response topic template uses missing device attributes", "missing_keys", missingKeys, "template", respTopic, "topic", temp.RespTopic, "device", device.Name, "device_id", device.Id, "local_device_id", device.LocalId, "service", service.Name, "service_id", service.Id, "local_service_id", service.LocalId)
		}
	}
	return []model.TopicDescription{temp}
}

func GenerateEventServiceTopicDescriptions(device models.Device, service models.Service, truncateDevicePrefix string) (result []model.TopicDescription) {
	eventTopicTempl, found := GetAttributeValue(service.Attributes, EventAttribute)
	if !found {
		return result
	}
	eventTopic, missingKeys, err := GenerateTopic(eventTopicTempl, device.LocalId, service.LocalId, truncateDevicePrefix, device.Attributes)
	if err != nil {
		slog.Warn("invalid event topic template", "error", err, "template", eventTopicTempl, "device", device.Name, "device_id", device.Id, "local_device_id", device.LocalId, "service", service.Name, "service_id", service.Id, "local_service_id", service.LocalId)
		return result
	}
	if len(missingKeys) > 0 {
		slog.Warn("event topic template uses missing device attributes", "missing_keys", missingKeys, "template", eventTopicTempl, "topic", eventTopic, "device", device.Name, "device_id", device.Id, "local_device_id", device.LocalId, "service", service.Name, "service_id", service.Id, "local_service_id", service.LocalId)
	}
	temp := model.TopicDescription{
		EventTopic:     eventTopic,
		DeviceTypeId:   device.DeviceTypeId,
		DeviceLocalId:  device.LocalId,
		ServiceLocalId: service.LocalId,
		DeviceName:     device.Name,
	}
	if temp.DeviceName == "" {
		for _, attr := range service.Attributes {
			if attr.Key == DisplayNameAttributeName {
				temp.DeviceName = attr.Value
				break
			}
		}
	}
	if temp.DeviceName == "" {
		temp.DeviceName = "unknown name"
	}
	for _, attr := range service.Attributes {
		if attr.Key == model.TransformerJsonUnwrapInput || attr.Key == model.TransformerJsonUnwrapOutput {
			paths := strings.Split(attr.Value, ",")
			for _, path := range paths {
				path = strings.TrimSpace(path)
				temp.Transformations = append(temp.Transformations, model.Transformation{
					Path:           path,
					Transformation: attr.Key,
				})
			}
		}
	}
	slices.SortFunc(temp.Transformations, func(a, b model.Transformation) int {
		return strings.Compare(a.Path, b.Path)
	})
	return []model.TopicDescription{temp}
}

func GetAttributeValue(attributes []models.Attribute, key string) (result string, found bool) {
	for _, attr := range attributes {
		if attr.Key == key {
			return attr.Value, true
		}
	}
	return result, false
}

// GenerateTopic fills the topic template. placeholders without a value are replaced by an empty string
// and returned as missingKeys, so that the caller can report them.
func GenerateTopic(topicTemplate string, deviceId string, serviceId string, truncateDevicePrefix string, attributes []models.Attribute) (result string, missingKeys []string, err error) {
	values := map[string]string{}
	for _, placeholder := range TemplateLocalDeviceIdPlaceholders {
		temp := deviceId
		if strings.HasPrefix(deviceId, truncateDevicePrefix) {
			temp = deviceId[len(truncateDevicePrefix):]
		}
		values[placeholder] = temp
	}
	for _, placeholder := range TemplateLocalServiceIdPlaceholders {
		values[placeholder] = serviceId
	}

	for _, attr := range attributes {
		if isValidPlaceholder(attr.Key) {
			values[attr.Key] = attr.Value
		}
	}

	var temp bytes.Buffer
	t, err := template.New("").Option("missingkey=zero").Parse(topicTemplate)
	if err != nil {
		return "", nil, err
	}
	for _, key := range templateFieldKeys(t.Tree.Root) {
		if _, ok := values[key]; !ok && !slices.Contains(missingKeys, key) {
			missingKeys = append(missingKeys, key)
		}
	}
	err = t.Execute(&temp, values)
	if err != nil {
		return "", nil, err
	}
	return temp.String(), missingKeys, nil
}

// templateFieldKeys returns the first identifier of every field (e.g. "CmdPrefix" for {{.CmdPrefix}}) used in the template
func templateFieldKeys(node parse.Node) (result []string) {
	switch n := node.(type) {
	case *parse.ListNode:
		if n == nil {
			return nil
		}
		for _, child := range n.Nodes {
			result = append(result, templateFieldKeys(child)...)
		}
	case *parse.ActionNode:
		result = append(result, templateFieldKeys(n.Pipe)...)
	case *parse.PipeNode:
		if n == nil {
			return nil
		}
		for _, cmd := range n.Cmds {
			result = append(result, templateFieldKeys(cmd)...)
		}
	case *parse.CommandNode:
		for _, arg := range n.Args {
			result = append(result, templateFieldKeys(arg)...)
		}
	case *parse.FieldNode:
		if len(n.Ident) > 0 {
			result = append(result, n.Ident[0])
		}
	case *parse.IfNode:
		result = append(result, templateFieldKeys(n.Pipe)...)
		result = append(result, templateFieldKeys(n.List)...)
		result = append(result, templateFieldKeys(n.ElseList)...)
	}
	return result
}

func isValidPlaceholder(key string) bool {
	_, err := template.New("").Parse("{{." + key + "}}")
	return err == nil
}
