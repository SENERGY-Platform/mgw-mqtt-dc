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

package integrationtests

import (
	"context"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/configuration"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/connector"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/integrationtests/docker"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/integrationtests/mocks"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/mgw"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/mqtt"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestEventForwarding(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mqttPort, _, err := docker.Mqtt(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	mgwPort, _, err := docker.Mqtt(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	conf := configuration.Config{
		ConnectorId:           "test",
		MgwMqttBroker:         "tcp://localhost:" + mgwPort,
		MgwMqttUser:           "",
		MgwMqttPw:             "",
		MgwMqttClientId:       "mgwclientid",
		Debug:                 true,
		UpdatePeriod:          "",
		DeviceDescriptionsDir: "",
		MqttPw:                "",
		MqttUser:              "",
		MqttCmdClientId:       "mqttcmdclientid",
		MqttEventClientId:     "mqtteventclientid",
		MqttBroker:            "tcp://localhost:" + mqttPort,
		DeleteDevices:         false,
		MaxCorrelationIdAge:   "1m",
	}

	mqttPublisher, err := mqtt.New(ctx, conf.MqttBroker, "testpublisher", "", "")
	if err != nil {
		t.Error(err)
		return
	}

	mgwListener, err := mqtt.New(ctx, conf.MgwMqttBroker, "testlistener", "", "")
	if err != nil {
		t.Error(err)
		return
	}
	mgwMessages := util.NewSyncMap[[]string]()
	err = mgwListener.Subscribe("#", 2, func(topic string, _ bool, payload []byte) {
		if topic != "device-manager/device/test" {
			mgwMessages.Update(topic, func(messages []string) []string {
				return append(messages, string(payload))
			})
		}
	})
	if err != nil {
		t.Error(err)
		return
	}

	topicDescriptions := []mocks.TopicDesc{
		{
			DeviceName: "d1",
			DeviceType: "dt1",
			DeviceId:   "1",
			ServiceId:  "1",
			EventTopic: "d1/s1",
			CmdTopic:   "",
			RespTopic:  "",
		},
		{
			DeviceName: "d1",
			DeviceType: "dt1",
			DeviceId:   "1",
			ServiceId:  "2",
			EventTopic: "d1/s2",
			CmdTopic:   "",
			RespTopic:  "",
		},
		{
			DeviceName: "d1",
			DeviceType: "dt1",
			DeviceId:   "1",
			ServiceId:  "3",
			EventTopic: "",
			CmdTopic:   "d1/s3/cmd",
			RespTopic:  "",
		},
		{
			DeviceName: "d1",
			DeviceType: "dt1",
			DeviceId:   "1",
			ServiceId:  "4",
			EventTopic: "",
			CmdTopic:   "d1/s4/cmd",
			RespTopic:  "d1/s4",
		},
		{
			DeviceName: "d1",
			DeviceType: "dt1",
			DeviceId:   "1",
			ServiceId:  "5",
			EventTopic: "",
			CmdTopic:   "d1/s5/cmd",
			RespTopic:  "d1/s5",
		},
		{
			DeviceName: "d1",
			DeviceType: "dt1",
			DeviceId:   "1",
			ServiceId:  "5",
			EventTopic: "d1/s5",
			CmdTopic:   "",
			RespTopic:  "",
		},
		{
			DeviceName: "d2",
			DeviceType: "dt1",
			DeviceId:   "2",
			ServiceId:  "1",
			EventTopic: "d2/s1",
			CmdTopic:   "",
			RespTopic:  "",
		},
		{
			DeviceName: "d3",
			DeviceType: "dt2",
			DeviceId:   "3",
			ServiceId:  "1",
			EventTopic: "d3/s1",
			CmdTopic:   "",
			RespTopic:  "",
		},
	}

	_, err = connector.NewWithFactories(ctx, conf, connector.NewTopicDescriptionProvider(func(config configuration.Config) ([]mocks.TopicDesc, error) {
		return topicDescriptions, nil
	}), connector.NewMgwFactory(mgw.New), connector.NewMqttFactory(mqtt.New))
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(1 * time.Second)

	expected := map[string][]string{}
	for _, desc := range topicDescriptions {
		if desc.EventTopic != "" {
			topic := "event/" + desc.DeviceId + "/" + desc.ServiceId
			msg := "event:" + desc.EventTopic
			expected[topic] = append(expected[topic], msg)
			err = mqttPublisher.Publish(desc.EventTopic, 2, false, []byte(msg))
			if err != nil {
				t.Error(err)
				return
			}
		}
	}

	time.Sleep(2 * time.Second)

	mgwMessages.Do(func(m *map[string][]string) {
		if !reflect.DeepEqual(*m, expected) {
			t.Error("\n", *m, "\n", expected)
		}
	})

}
