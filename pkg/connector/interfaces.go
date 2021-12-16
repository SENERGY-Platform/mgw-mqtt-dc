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
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/mgw"
)

//before refactoring:
/*
type MgwFactory func(ctx context.Context, config configuration.Config, refreshNotifier func()) (MgwClient, error)
type TopicDescriptionProvider func(config configuration.Config) ([]TopicDescription, error)

type GenericTopicDescriptionProvider[T TopicDescription] func(config configuration.Config) ([]T, error)
type GenericMgwFactory[T MgwClient] func(ctx context.Context, config configuration.Config, refreshNotifier func()) (T, error)
*/

// after refactoring

type GenericMgwFactory[T MgwClient] func(ctx context.Context, config configuration.Config, refreshNotifier func()) (T, error)
type MgwFactory = GenericMgwFactory[MgwClient]

type GenericTopicDescriptionProvider[T TopicDescription] func(config configuration.Config) ([]T, error)
type TopicDescriptionProvider = GenericTopicDescriptionProvider[TopicDescription]

type GenericMqttFactory[T MqttClient] func(ctx context.Context, config configuration.Config) (T, error)
type MqttFactory = GenericMqttFactory[MqttClient]

type MgwClient interface {
	ListenToDeviceCommands(deviceId string, commandHandler mgw.DeviceCommandHandler) error
	StopListenToDeviceCommands(deviceId string) error
	SetDevice(deviceId string, name string, deviceTypeid string, state string) error
	RemoveDevice(deviceId string) error
}

type TopicDescription interface {
	DeviceDescription
	GetTopic() string
	GetResponseTopic() string
	GetLocalServiceId() string
}

type DeviceDescription interface {
	GetDeviceName() string
	GetDeviceTypeId() string
	GetLocalDeviceId() string
}

type MqttClient interface {
	Subscribe(topic string, qos byte, handler func(topic string, payload []byte)) error
	Unsubscribe(topic string) error
	Publish(topic string, qos byte, retained bool, payload []byte) error
}

//interface converters

func ListMap[From any, To any](from []From, converter func(From) To) (to []To) {
	if from != nil {
		to = make([]To, len(from))
	}
	for i, e := range from {
		to[i] = converter(e)
	}
	return
}

func FMap1[I1 any, ResultType any, NewResultType any](f func(in I1) (ResultType, error), c func(ResultType) NewResultType) func(in I1) (NewResultType, error) {
	return func(in I1) (result NewResultType, err error) {
		temp, err := f(in)
		if err != nil {
			return result, err
		}
		result = c(temp)
		return result, err
	}
}

func FMap2[I1 any, I2 any, ResultType any, NewResultType any](f func(in1 I1, in2 I2) (ResultType, error), c func(ResultType) NewResultType) func(in1 I1, in2 I2) (NewResultType, error) {
	return func(in1 I1, in2 I2) (result NewResultType, err error) {
		temp, err := f(in1, in2)
		if err != nil {
			return result, err
		}
		result = c(temp)
		return result, err
	}
}

func FMap3[I1 any, I2 any, I3 any, ResultType any, NewResultType any](f func(in1 I1, in2 I2, in3 I3) (ResultType, error), c func(ResultType) NewResultType) func(in1 I1, in2 I2, in3 I3) (NewResultType, error) {
	return func(in1 I1, in2 I2, in3 I3) (result NewResultType, err error) {
		temp, err := f(in1, in2, in3)
		if err != nil {
			return result, err
		}
		result = c(temp)
		return result, err
	}
}

func TopicDescriptionsConverter[T TopicDescription](from []T) []TopicDescription {
	return ListMap(from, func(element T) TopicDescription { return element })
}

func NewTopicDescriptionProvider[T TopicDescription](f GenericTopicDescriptionProvider[T]) (result TopicDescriptionProvider) {
	return FMap1(f, TopicDescriptionsConverter[T])
}

func NewMgwFactory[MgwClientType MgwClient](f GenericMgwFactory[MgwClientType]) (result MgwFactory) {
	return FMap3(f, func(element MgwClientType) MgwClient { return element })
}

func NewMqttFactory[MqttClientType MqttClient](f GenericMqttFactory[MqttClientType]) (result MqttFactory) {
	return FMap2(f, func(element MqttClientType) MqttClient { return element })
}
