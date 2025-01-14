/*
 * Copyright 2025 InfAI (CC SES)
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
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/devicerepo"
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

func TestEventTransformer(t *testing.T) {
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

	mqttPublisher, err := mqtt.New(ctx, conf.MqttBroker, "testpublisher", "", "", conf.MqttInsecureSkipVerify)
	if err != nil {
		t.Error(err)
		return
	}

	mgwListener, err := mqtt.New(ctx, conf.MgwMqttBroker, "testlistener", "", "", conf.MqttInsecureSkipVerify)
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
			DeviceName:      "device_transform_root",
			DeviceType:      "dt_transform_root",
			DeviceId:        "device_transform_root",
			ServiceId:       "device_transform_root_event",
			EventTopic:      "device_transform_root/event",
			CmdTopic:        "",
			RespTopic:       "",
			Transformations: []mocks.Transformation{{Transformation: connector.TransformerJsonUnwrapOutput, Path: ""}},
		},
		{
			DeviceName:      "device_transform_list",
			DeviceType:      "dt_transform_list",
			DeviceId:        "device_transform_list",
			ServiceId:       "device_transform_list_event",
			EventTopic:      "device_transform_list/event",
			CmdTopic:        "",
			RespTopic:       "",
			Transformations: []mocks.Transformation{{Transformation: connector.TransformerJsonUnwrapOutput, Path: "0,2"}},
		},
		{
			DeviceName:      "device_transform_list",
			DeviceType:      "dt_transform_list",
			DeviceId:        "device_transform_list",
			ServiceId:       "device_transform_list_event_sub1",
			EventTopic:      "device_transform_list/event_sub1",
			CmdTopic:        "",
			RespTopic:       "",
			Transformations: []mocks.Transformation{{Transformation: connector.TransformerJsonUnwrapOutput, Path: "0.i,2.i"}},
		},
		{
			DeviceName:      "device_transform_list",
			DeviceType:      "dt_transform_list",
			DeviceId:        "device_transform_list",
			ServiceId:       "device_transform_list_event_sub2",
			EventTopic:      "device_transform_list/event_sub2",
			CmdTopic:        "",
			RespTopic:       "",
			Transformations: []mocks.Transformation{{Transformation: connector.TransformerJsonUnwrapOutput, Path: "i.0,i.2"}},
		},
		{
			DeviceName:      "device_transform_list_any",
			DeviceType:      "dt_transform_list",
			DeviceId:        "device_transform_list_any",
			ServiceId:       "device_transform_list_any_event",
			EventTopic:      "device_transform_list_any/event",
			CmdTopic:        "",
			RespTopic:       "",
			Transformations: []mocks.Transformation{{Transformation: connector.TransformerJsonUnwrapOutput, Path: "*"}},
		},
		{
			DeviceName:      "device_transform_list_any",
			DeviceType:      "dt_transform_list",
			DeviceId:        "device_transform_list_any",
			ServiceId:       "device_transform_list_any_event_sub1",
			EventTopic:      "device_transform_list_any/event_sub1",
			CmdTopic:        "",
			RespTopic:       "",
			Transformations: []mocks.Transformation{{Transformation: connector.TransformerJsonUnwrapOutput, Path: "*.i"}},
		},
		{
			DeviceName:      "device_transform_list_any",
			DeviceType:      "dt_transform_list",
			DeviceId:        "device_transform_list_any",
			ServiceId:       "device_transform_list_any_event_sub2",
			EventTopic:      "device_transform_list_any/event_sub2",
			CmdTopic:        "",
			RespTopic:       "",
			Transformations: []mocks.Transformation{{Transformation: connector.TransformerJsonUnwrapOutput, Path: "i.*"}},
		},
		{
			DeviceName:      "device_transform_field",
			DeviceType:      "dt_transform_field",
			DeviceId:        "device_transform_field",
			ServiceId:       "device_transform_field_event",
			EventTopic:      "device_transform_field/event",
			CmdTopic:        "",
			RespTopic:       "",
			Transformations: []mocks.Transformation{{Transformation: connector.TransformerJsonUnwrapOutput, Path: "i,f,b"}},
		},
		{
			DeviceName:      "device_transform_sub_field",
			DeviceType:      "dt_transform_sub_field",
			DeviceId:        "device_transform_sub_field",
			ServiceId:       "device_transform_sub_field_event",
			EventTopic:      "device_transform_sub_field/event",
			CmdTopic:        "",
			RespTopic:       "",
			Transformations: []mocks.Transformation{{Transformation: connector.TransformerJsonUnwrapOutput, Path: "sub.i,sub.f,sub.b"}},
		},
	}

	toBeSend := map[string][]string{
		"device_transform_root/event": {
			`"true"`,
			`"false"`,
			`"42"`,
			`"1.3"`,
			`"{\"name\":\"test\"}"`,
		},
		"device_transform_list/event": {
			`["42", "name", "true"]`,
		},
		"device_transform_list/event_sub1": {
			`[{"i":"42","ignore":"ignore"}, "name", {"i":"1.3"}]`,
		},
		"device_transform_list/event_sub2": {
			`{"i":["42", "name", "true"],"ignore":"ignore"}`,
		},
		"device_transform_list_any/event": {
			`["42", "true", "{\"name\":\"test\"}"]`,
		},
		"device_transform_list_any/event_sub1": {
			`[{"i":"42","ignore":"ignore"},{"i":"1.3","ignore":"ignore"}]`,
		},
		"device_transform_list_any/event_sub2": {
			`{"i":["42","true"],"ignore":"ignore"}`,
		},
		"device_transform_field/event": {
			`{"ignore": "foobar", "i": "42", "f": "1.3", "b":"true"}`,
		},
		"device_transform_sub_field/event": {
			`{"ignore1":"foobar","sub":{"ignore2":"foobar","i":"42","f":"1.3","b":"true"}}`,
		},
	}

	expected := map[string][]string{
		"event/device_transform_root/device_transform_root_event": {
			`true`,
			`false`,
			`42`,
			`1.3`,
			`{"name":"test"}`,
		},
		"event/device_transform_list/device_transform_list_event": {
			`[42,"name",true]`,
		},
		"event/device_transform_list/device_transform_list_event_sub1": {
			`[{"i":42,"ignore":"ignore"},"name",{"i":1.3}]`,
		},
		"event/device_transform_list/device_transform_list_event_sub2": {
			`{"i":[42,"name",true],"ignore":"ignore"}`,
		},
		"event/device_transform_list_any/device_transform_list_any_event": {
			`[42,true,{"name":"test"}]`,
		},
		"event/device_transform_list_any/device_transform_list_any_event_sub1": {
			`[{"i":42,"ignore":"ignore"},{"i":1.3,"ignore":"ignore"}]`,
		},
		"event/device_transform_list_any/device_transform_list_any_event_sub2": {
			`{"i":[42,true],"ignore":"ignore"}`,
		},
		"event/device_transform_field/device_transform_field_event": {
			`{"b":true,"f":1.3,"i":42,"ignore":"foobar"}`,
		},
		"event/device_transform_sub_field/device_transform_sub_field_event": {
			`{"ignore1":"foobar","sub":{"b":true,"f":1.3,"i":42,"ignore2":"foobar"}}`,
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

	for topic, msgList := range toBeSend {
		for _, msg := range msgList {
			err = mqttPublisher.Publish(topic, 2, false, []byte(msg))
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
