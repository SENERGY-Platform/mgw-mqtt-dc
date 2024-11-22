/*
 * Copyright 2021 InfAI (CC SES)
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

package connector

import (
	"context"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/configuration"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/devicerepo"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/mgw"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
)

type GenericMgwFactory[T MgwClient] func(ctx context.Context, config configuration.Config, refreshNotifier func()) (T, error)
type MgwFactory = GenericMgwFactory[MgwClient]

type GenericTopicDescriptionProvider[T TopicDescription] func(config configuration.Config, deviceRepo *devicerepo.DeviceRepo) ([]T, error)
type TopicDescriptionProvider = GenericTopicDescriptionProvider[TopicDescription]

type GenericMqttFactory[T MqttClient] func(ctx context.Context, brokerUrl string, clientId string, username string, password string, insecureSkipVerify bool) (T, error)
type MqttFactory = GenericMqttFactory[MqttClient]

type MgwClient interface {
	ListenToDeviceCommands(deviceId string, commandHandler mgw.DeviceCommandHandler) error
	StopListenToDeviceCommands(deviceId string) error
	SetDevice(deviceId string, name string, deviceTypeid string, state string) error
	RemoveDevice(deviceId string) error
	SendEvent(deviceId string, serviceId string, value []byte) error
	Respond(deviceId string, serviceId string, response mgw.Command) error

	SendClientError(message string)
	SendDeviceError(localDeviceId string, message string)
	SendCommandError(correlationId string, message string)
}

type TopicDescription interface {
	DeviceDescription
	GetEventTopic() string
	GetCmdTopic() string
	GetResponseTopic() string
	GetLocalServiceId() string
}

type DeviceDescription interface {
	GetDeviceName() string
	GetDeviceTypeId() string
	GetLocalDeviceId() string
}

func EqualTopicDesc(old TopicDescription, topic TopicDescription) bool {
	if EqualDeviceDesc(old, topic) &&
		old.GetEventTopic() == topic.GetEventTopic() &&
		old.GetResponseTopic() == topic.GetResponseTopic() &&
		old.GetCmdTopic() == topic.GetCmdTopic() &&
		old.GetLocalServiceId() == topic.GetLocalServiceId() {
		return true
	}
	return false
}

func EqualDeviceDesc(old DeviceDescription, topic DeviceDescription) bool {
	if old.GetDeviceName() == topic.GetDeviceName() &&
		old.GetLocalDeviceId() == topic.GetLocalDeviceId() &&
		old.GetDeviceTypeId() == topic.GetDeviceTypeId() {
		return true
	}
	return false
}

type MqttClient interface {
	Subscribe(topic string, qos byte, handler func(topic string, retained bool, payload []byte)) error
	Unsubscribe(topic string) error
	Publish(topic string, qos byte, retained bool, payload []byte) error
}

func TopicDescriptionsConverter[T TopicDescription](from []T) []TopicDescription {
	return util.ListMap(from, func(element T) TopicDescription { return element })
}

func NewTopicDescriptionProvider[T TopicDescription](f GenericTopicDescriptionProvider[T]) (result TopicDescriptionProvider) {
	return util.FMap2(f, TopicDescriptionsConverter[T])
}

func NewMgwFactory[MgwClientType MgwClient](f GenericMgwFactory[MgwClientType]) (result MgwFactory) {
	return util.FMap3(f, func(element MgwClientType) MgwClient { return element })
}

func NewMqttFactory[MqttClientType MqttClient](f GenericMqttFactory[MqttClientType]) (result MqttFactory) {
	return util.FMap6(f, func(element MqttClientType) MqttClient { return element })
}
