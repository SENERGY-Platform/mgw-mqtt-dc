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
	"encoding/json"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/configuration"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/connector"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/devicerepo"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/integrationtests/docker"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/integrationtests/mocks"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/mgw"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/mqtt"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCorrelationIdMatching(t *testing.T) {
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
		MaxCorrelationIdAge:   "5s",
	}

	mqttClient, err := mqtt.New(ctx, conf.MqttBroker, "testpublisher", "", "")
	if err != nil {
		t.Error(err)
		return
	}
	mqttMessages := util.NewSyncMap[[]string]()
	err = mqttClient.Subscribe("#", 2, func(topic string, _ bool, payload []byte) {
		mqttMessages.Update(topic, func(messages []string) []string {
			return append(messages, string(payload))
		})
	})
	if err != nil {
		t.Error(err)
		return
	}

	mgwMqttClient, err := mqtt.New(ctx, conf.MgwMqttBroker, "testlistener", "", "")
	if err != nil {
		t.Error(err)
		return
	}
	mgwMessages := util.NewSyncMap[[]string]()
	err = mgwMqttClient.Subscribe("#", 2, func(topic string, _ bool, payload []byte) {
		if topic != "device-manager/device/test" && !strings.HasPrefix(topic, "command/") && !strings.HasPrefix(topic, "event/") {
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
	}

	_, err = connector.NewWithFactories(ctx, conf, connector.NewTopicDescriptionProvider(func(config configuration.Config, repo *devicerepo.DeviceRepo) ([]mocks.TopicDesc, error) {
		return topicDescriptions, nil
	}), connector.NewMgwFactory(mgw.New), connector.NewMqttFactory(mqtt.New))
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(1 * time.Second)

	expectedMqttMsg := map[string][]string{}
	expectedMgwMsg := map[string][]string{}
	sendCommands := func(correlationIdSuffix string, msgSuffix string, withRespExpectation bool) {
		for _, desc := range topicDescriptions {
			if desc.CmdTopic != "" {
				cmdObj := mgw.Command{
					CommandId: "cmd:" + desc.CmdTopic + "_" + correlationIdSuffix,
					Data:      "cmd:" + desc.CmdTopic + "_" + msgSuffix,
				}
				cmdMsg, _ := json.Marshal(cmdObj)
				cmdTopic := "command/" + desc.DeviceId + "/" + desc.ServiceId
				expectedMqttMsg[desc.CmdTopic] = append(expectedMqttMsg[desc.CmdTopic], cmdObj.Data)
				err = mgwMqttClient.Publish(cmdTopic, 2, false, cmdMsg)
				if err != nil {
					t.Error(err)
					return
				}
				if withRespExpectation {
					respTopic := "response/" + desc.DeviceId + "/" + desc.ServiceId
					respObj := mgw.Command{
						CommandId: cmdObj.CommandId,
						Data:      "",
					}
					if desc.RespTopic != "" {
						respObj.Data = "resp:" + desc.RespTopic + "_" + msgSuffix
					}
					respMsg, _ := json.Marshal(respObj)
					expectedMgwMsg[respTopic] = append(expectedMgwMsg[respTopic], string(respMsg))
				}
			}
		}
	}

	sendResponses := func(msgSuffix string) {
		for _, desc := range topicDescriptions {
			if desc.CmdTopic != "" && desc.RespTopic != "" {
				respMsg := "resp:" + desc.RespTopic + "_" + msgSuffix
				expectedMqttMsg[desc.RespTopic] = append(expectedMqttMsg[desc.RespTopic], respMsg)
				err = mqttClient.Publish(desc.RespTopic, 2, false, []byte(respMsg))
				if err != nil {
					t.Error(err)
					return
				}
			}
		}
	}

	//expect responses
	sendCommands("expectResponse1", "1", true)
	sendCommands("expectResponse2", "2", true)
	sendCommands("expectResponse3", "3", true)
	time.Sleep(2 * time.Second)
	sendResponses("1")
	sendResponses("2")
	sendResponses("3")

	//expect fail
	sendCommands("expectFail1", "f1", false)
	time.Sleep(10 * time.Second)

	//expect responses
	sendCommands("expectResponse4", "4", true)
	sendCommands("expectResponse5", "5", true)
	sendCommands("expectResponse6", "6", true)
	time.Sleep(2 * time.Second)
	sendResponses("4")
	sendResponses("5")
	sendResponses("6")

	//expect fail
	sendCommands("expectFail2", "f2", false)
	time.Sleep(10 * time.Second)
	sendResponses("f2")

	//expect responses
	sendCommands("expectResponse7", "7", true)
	sendCommands("expectResponse8", "8", true)
	sendCommands("expectResponse9", "9", true)
	time.Sleep(2 * time.Second)
	sendResponses("7")
	sendResponses("8")
	sendResponses("9")

	time.Sleep(2 * time.Second)
	mqttMessages.Do(func(m *map[string][]string) {
		if !reflect.DeepEqual(*m, expectedMqttMsg) {
			t.Error("\n", *m, "\n", expectedMqttMsg)
		}
	})
	mgwMessages.Do(func(m *map[string][]string) {
		if !reflect.DeepEqual(*m, expectedMgwMsg) {
			t.Error("\n", *m, "\n", expectedMgwMsg)
		}
	})
}
