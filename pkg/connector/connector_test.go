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
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/devicerepo"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/mgw"
	"log"
	"strings"
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
	}, NewTopicDescriptionProvider(func(config configuration.Config, repo *devicerepo.DeviceRepo) (desc []MockDesc, err error) {
		return []MockDesc{
			"c:a",
			"e:foo",
			"c:b",
			"c:b",
		}, nil
	}), NewMgwFactory(newMgwMock), NewMqttFactory(newMqttMock))
	if err != nil {
		t.Error(err)
		return
	}
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
	if strings.HasPrefix(string(this), "e:") || strings.HasPrefix(string(this), "c:") {
		return string(this)[2:]
	}
	return string(this)
}

func (this MockDesc) GetEventTopic() string {
	if strings.HasPrefix(string(this), "e:") {
		return string(this)[2:]
	}
	return ""
}

func (this MockDesc) GetCmdTopic() string {
	if strings.HasPrefix(string(this), "c:") {
		return string(this)[2:]
	}
	return ""
}

func (this MockDesc) GetDeviceName() string {
	return "name"
}

func (this MockDesc) GetResponseTopic() string {
	if strings.HasPrefix(string(this), "e:") {
		return ""
	}
	return this.GetCmdTopic() + "/resp"
}

func (this MockDesc) GetDeviceTypeId() string {
	return "dtid"
}

func (this MockDesc) GetLocalDeviceId() string {
	return string(this) + "_dlid"
}

func (this MockDesc) GetLocalServiceId() string {
	return "slid"
}

func (this MockDesc) HasTransformations() bool {
	return false
}

func (this MockDesc) GetTransformations(kind string) (result []string) {
	return nil
}

func newMgwMock(ctx context.Context, config configuration.Config, refreshNotifier func()) (*MgwMock, error) {
	return &MgwMock{config: config}, nil
}

type MgwMock struct {
	config configuration.Config
}

func (this *MgwMock) Respond(deviceId string, serviceId string, response mgw.Command) error {
	log.Println("Respond", deviceId, serviceId, response)
	return nil
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

func (this *MgwMock) SendEvent(deviceId string, serviceId string, value []byte) error {
	log.Println("SendEvent", deviceId, serviceId, string(value))
	return nil
}

func newMqttMock(ctx context.Context, brokerUrl string, clientId string, username string, password string, insecureSkipVerify bool) (MqttMock, error) {
	return MqttMock{}, nil
}

func (this *MgwMock) SendClientError(message string) {
	log.Println("SendClientError", message)
}

func (this *MgwMock) SendDeviceError(localDeviceId string, message string) {
	log.Println("SendDeviceError", message)
}

func (this *MgwMock) SendCommandError(correlationId string, message string) {
	log.Println("SendCommandError", message)
}

type MqttMock struct {
}

func (this MqttMock) Subscribe(topic string, qos byte, handler func(topic string, retained bool, payload []byte)) error {
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
