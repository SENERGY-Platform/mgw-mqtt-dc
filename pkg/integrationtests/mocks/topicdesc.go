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

package mocks

import "strings"

type TopicDesc struct {
	DeviceName      string
	DeviceType      string
	DeviceId        string
	ServiceId       string
	EventTopic      string
	CmdTopic        string
	RespTopic       string
	Transformations []Transformation
}

type Transformation struct {
	Path           string
	Transformation string
}

func (this TopicDesc) GetDeviceName() string {
	return this.DeviceName
}

func (this TopicDesc) GetDeviceTypeId() string {
	return this.DeviceType
}

func (this TopicDesc) GetLocalDeviceId() string {
	return this.DeviceId
}

func (this TopicDesc) GetEventTopic() string {
	return this.EventTopic
}

func (this TopicDesc) GetCmdTopic() string {
	return this.CmdTopic
}

func (this TopicDesc) GetResponseTopic() string {
	return this.RespTopic
}

func (this TopicDesc) GetLocalServiceId() string {
	return this.ServiceId
}

func (this TopicDesc) HasTransformations() bool {
	return len(this.Transformations) > 0
}

func (this TopicDesc) GetTransformations(kind string) (result []string) {
	for _, trans := range this.Transformations {
		if trans.Transformation == kind {
			paths := strings.Split(trans.Path, ",")
			for _, path := range paths {
				result = append(result, strings.TrimSpace(path))
			}
		}
	}
	return result
}
