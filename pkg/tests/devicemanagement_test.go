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

package tests

import (
	"context"
	"encoding/json"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/configuration"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/connector"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/mgw"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/mqtt"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/tests/docker"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/tests/mocks"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestInitialDeviceInfo(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mqttPort, _, err := docker.Mqtt(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	conf := configuration.Config{
		ConnectorId:           "test",
		MgwMqttBroker:         "tcp://localhost:" + mqttPort,
		MgwMqttUser:           "",
		MgwMqttPw:             "",
		MgwMqttClientId:       "mgwclientid",
		Debug:                 true,
		UpdatePeriod:          "",
		DeviceDescriptionsDir: "",
		MqttPw:                "",
		MqttUser:              "",
		MqttClientId:          "mqttclientid",
		MqttBroker:            "tcp://localhost:" + mqttPort,
		DeleteDevices:         false,
	}

	mqttListener, err := mqtt.New(ctx, conf.MqttBroker, "testlistener", "", "")
	if err != nil {
		t.Error(err)
		return
	}
	mqttMessages := util.NewSyncMap[[]string]()
	err = mqttListener.Subscribe("#", 2, func(topic string, payload []byte) {
		mqttMessages.Update(topic, func(messages []string) []string {
			return append(messages, string(payload))
		})
	})
	if err != nil {
		t.Error(err)
		return
	}

	_, err = connector.NewWithFactories(ctx, conf, connector.NewTopicDescriptionProvider(func(config configuration.Config) ([]mocks.TopicDesc, error) {
		return []mocks.TopicDesc{
			{
				DeviceName: "d1",
				DeviceType: "dt1",
				DeviceId:   "1",
				ServiceId:  "1",
				EventTopic: "1/1",
				CmdTopic:   "",
				RespTopic:  "",
			},
			{
				DeviceName: "d1",
				DeviceType: "dt1",
				DeviceId:   "1",
				ServiceId:  "2",
				EventTopic: "1/2",
				CmdTopic:   "",
				RespTopic:  "",
			},
			{
				DeviceName: "d1",
				DeviceType: "dt1",
				DeviceId:   "1",
				ServiceId:  "3",
				EventTopic: "",
				CmdTopic:   "1/3/cmd",
				RespTopic:  "",
			},
			{
				DeviceName: "d1",
				DeviceType: "dt1",
				DeviceId:   "1",
				ServiceId:  "4",
				EventTopic: "",
				CmdTopic:   "1/4/cmd",
				RespTopic:  "1/4",
			},
			{
				DeviceName: "d1",
				DeviceType: "dt1",
				DeviceId:   "1",
				ServiceId:  "5",
				EventTopic: "",
				CmdTopic:   "1/5/cmd",
				RespTopic:  "1/5",
			},
			{
				DeviceName: "d1",
				DeviceType: "dt1",
				DeviceId:   "1",
				ServiceId:  "5",
				EventTopic: "1/5",
				CmdTopic:   "",
				RespTopic:  "",
			},
			{
				DeviceName: "d2",
				DeviceType: "dt1",
				DeviceId:   "2",
				ServiceId:  "1",
				EventTopic: "2/1",
				CmdTopic:   "",
				RespTopic:  "",
			},
			{
				DeviceName: "d3",
				DeviceType: "dt2",
				DeviceId:   "3",
				ServiceId:  "1",
				EventTopic: "3/1",
				CmdTopic:   "",
				RespTopic:  "",
			},
		}, nil
	}), connector.NewMgwFactory(mgw.New), connector.NewMqttFactory(mqtt.New))
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(1 * time.Second)

	mqttMessages.Do(func(m *map[string][]string) {
		j, err := json.Marshal(*m)
		t.Log(err, string(j))
	})

	deviceInfoMessages, ok := mqttMessages.Get(mgw.DeviceManagerTopic + "/" + conf.ConnectorId)
	if !ok {
		t.Error("missing device infos")
		return
	}

	deviceInfos := util.ListMap(deviceInfoMessages, func(from string) (to mgw.DeviceInfoUpdate) {
		err := json.Unmarshal([]byte(from), &to)
		if err != nil {
			t.Error(err)
		}
		return
	})

	expected := []mgw.DeviceInfoUpdate{}
	if !reflect.DeepEqual(deviceInfos, expected) {
		a, _ := json.Marshal(deviceInfos)
		e, _ := json.Marshal(expected)
		t.Error(string(a), "\n", string(e))
	}
}

func TestInitialDeviceInfoWithDelete(t *testing.T) {

}

func TestTimedDeviceInfoUpdate(t *testing.T) {

}

func TestTimedDeviceInfoUpdateWithDelete(t *testing.T) {

}

func TestSignaledDeviceInfoUpdate(t *testing.T) {

}

func TestSignaledDeviceInfoUpdateWithDelete(t *testing.T) {

}
