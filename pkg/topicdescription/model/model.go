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

package model

const TransformerJsonUnwrapInput = "json-unwrap-input"
const TransformerJsonUnwrapOutput = "json-unwrap-output"

type TopicDescription struct {
	CmdTopic        string           `json:"cmd_topic" yaml:"cmd_topic"`
	EventTopic      string           `json:"event_topic" yaml:"event_topic"`
	RespTopic       string           `json:"resp_topic" yaml:"resp_topic"`
	DeviceTypeId    string           `json:"device_type_id" yaml:"device_type_id"`
	DeviceLocalId   string           `json:"device_local_id" yaml:"device_local_id"`
	ServiceLocalId  string           `json:"service_local_id" yaml:"service_local_id"`
	Transformations []Transformation `json:"transformations" yaml:"transformations"`
	DeviceName      string           `json:"device_name" yaml:"device_name"`
}

type Transformation struct {
	Path           string `json:"path" yaml:"path"`
	Transformation string `json:"transformation" yaml:"transformation"`
}

func (this TopicDescription) GetTopic() string {
	if this.EventTopic != "" {
		return this.EventTopic
	}
	return this.CmdTopic
}

func (this TopicDescription) GetEventTopic() string {
	return this.EventTopic
}

func (this TopicDescription) GetCmdTopic() string {
	return this.CmdTopic
}

func (this TopicDescription) GetDeviceName() string {
	return this.DeviceName
}

func (this TopicDescription) GetResponseTopic() string {
	return this.RespTopic
}

func (this TopicDescription) GetDeviceTypeId() string {
	return this.DeviceTypeId
}

func (this TopicDescription) GetLocalDeviceId() string {
	return this.DeviceLocalId
}

func (this TopicDescription) GetLocalServiceId() string {
	return this.ServiceLocalId
}

func (this TopicDescription) HasTransformations() bool {
	return len(this.Transformations) > 0
}

func (this TopicDescription) GetTransformations(kind string) (result []string) {
	for _, trans := range this.Transformations {
		if trans.Transformation == kind {
			result = append(result, trans.Path)
		}
	}
	return result
}
