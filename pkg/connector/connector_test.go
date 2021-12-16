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
	"encoding/json"
	"fmt"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/configuration"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/mgw"
	"log"
	"testing"
)

func TestConnectorInit(t *testing.T) {
	temp, err := NewWithFactories(context.Background(), configuration.Config{
		ConnectorId:     "",
		MgwMqttBroker:   "",
		MgwMqttUser:     "",
		MgwMqttPw:       "",
		MgwMqttClientId: "",
		Debug:           false,
	}, NewTopicDescriptionProvider(func(config configuration.Config) (desc []MockDesc, err error) {
		return []MockDesc{
			"a",
			"foo",
			"b",
		}, nil
	}), NewMgwFactory(newMgwMock), NewMqttFactory(newMqttMock))
	fmt.Println(err, *temp)
	j, err := json.Marshal(temp)
	fmt.Println(err, string(j))
}

func TestConnectorAlternative(t *testing.T) {
	temp, err := New(context.Background(), configuration.Config{
		ConnectorId:     "",
		MgwMqttBroker:   "",
		MgwMqttUser:     "",
		MgwMqttPw:       "",
		MgwMqttClientId: "",
		Debug:           false,
	})
	fmt.Println(err, temp)
	j, err := json.Marshal(temp)
	fmt.Println(err, string(j))
}

type MockDesc string

func (this MockDesc) GetTopic() string {
	return string(this)
}

func (this MockDesc) GetDeviceName() string {
	return "name"
}

func (this MockDesc) GetResponseTopic() string {
	return "resp"
}

func (this MockDesc) GetDeviceTypeId() string {
	return "dtid"
}

func (this MockDesc) GetLocalDeviceId() string {
	return "dlid"
}

func (this MockDesc) GetLocalServiceId() string {
	return "slid"
}

func newMgwMock(ctx context.Context, config configuration.Config, refreshNotifier func()) (*MgwMock, error) {
	return &MgwMock{config: config}, nil
}

type MgwMock struct {
	config configuration.Config
}

func (this *MgwMock) StopListenToDeviceCommands(deviceId string) error {
	log.Println("StopListenToDeviceCommands", deviceId)
	return nil
}

func (this *MgwMock) RemoveDevice(deviceId string) error {
	log.Println("RemoveDevice", deviceId)
	return nil
}

func (this *MgwMock) ListenToDeviceCommands(deviceId string, commandHandler mgw.DeviceCommandHandler) error {
	log.Println("ListenToDeviceCommands", deviceId)
	return nil
}

func (this *MgwMock) SetDevice(deviceId string, name string, deviceTypeid string, state string) error {
	log.Println("SetDevice", deviceId, name, deviceTypeid, state)
	return nil
}

func newMqttMock(ctx context.Context, config configuration.Config) (MqttMock, error) {
	return MqttMock{}, nil
}

type MqttMock struct {
}

func (this MqttMock) Subscribe(topic string, qos byte, handler func(topic string, payload []byte)) error {
	log.Println("Subscribe", topic)
	return nil
}

func (this MqttMock) Unsubscribe(topic string) error {
	log.Println("Unsubscribe", topic)
	return nil
}

func (this MqttMock) Publish(topic string, qos byte, retained bool, payload []byte) error {
	log.Println("publish", topic, payload)
	return nil
}
