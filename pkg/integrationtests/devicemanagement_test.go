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
		MqttCmdClientId:       "mqttcmdclientid",
		MqttEventClientId:     "mqtteventclientid",
		MqttBroker:            "tcp://localhost:" + mqttPort,
		DeleteDevices:         false,
		MaxCorrelationIdAge:   "1m",
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

	expected := []mgw.DeviceInfoUpdate{
		{
			Method:   "set",
			DeviceId: "1",
			Data: mgw.DeviceInfo{
				Name:       "d1",
				DeviceType: "dt1",
				State:      "online",
			},
		},
		{
			Method:   "set",
			DeviceId: "2",
			Data: mgw.DeviceInfo{
				Name:       "d2",
				DeviceType: "dt1",
				State:      "online",
			},
		},
		{
			Method:   "set",
			DeviceId: "3",
			Data: mgw.DeviceInfo{
				Name:       "d3",
				DeviceType: "dt2",
				State:      "online",
			},
		},
	}

	compareDeviceInfo := func(a mgw.DeviceInfoUpdate, b mgw.DeviceInfoUpdate) bool {
		return a.DeviceId+a.Method < b.DeviceId+b.Method
	}
	util.ListSort(expected, compareDeviceInfo)
	util.ListSort(deviceInfos, compareDeviceInfo)

	if !reflect.DeepEqual(deviceInfos, expected) {
		a, _ := json.Marshal(deviceInfos)
		e, _ := json.Marshal(expected)
		t.Error(string(a), "\n", string(e))
	}
}

func TestTimedDeviceInfoUpdate(t *testing.T) {
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
		UpdatePeriod:          "2s",
		DeviceDescriptionsDir: "",
		MqttPw:                "",
		MqttUser:              "",
		MqttCmdClientId:       "mqttcmdclientid",
		MqttEventClientId:     "mqtteventclientid",
		MqttBroker:            "tcp://localhost:" + mqttPort,
		DeleteDevices:         false,
		MaxCorrelationIdAge:   "1m",
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

	topicDescProviderCalls := 0
	_, err = connector.NewWithFactories(ctx, conf, connector.NewTopicDescriptionProvider(func(config configuration.Config) ([]mocks.TopicDesc, error) {
		topicDescProviderCalls = topicDescProviderCalls + 1
		base := []mocks.TopicDesc{
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
		}
		extended := append([]mocks.TopicDesc{}, base...)
		extended = append(extended, mocks.TopicDesc{
			DeviceName: "d4",
			DeviceType: "dt2",
			DeviceId:   "4",
			ServiceId:  "1",
			EventTopic: "4/1",
			CmdTopic:   "",
			RespTopic:  "",
		})
		switch topicDescProviderCalls {
		case 1:
			fallthrough
		case 2:
			fallthrough
		case 3:
			return base, nil
		case 4:
			return extended, nil
		case 5:
			return base, nil
		default:
			return base, nil
		}

	}), connector.NewMgwFactory(mgw.New), connector.NewMqttFactory(mqtt.New))
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(11 * time.Second)

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

	expectedBase := []mgw.DeviceInfoUpdate{
		{
			Method:   "set",
			DeviceId: "1",
			Data: mgw.DeviceInfo{
				Name:       "d1",
				DeviceType: "dt1",
				State:      "online",
			},
		},
		{
			Method:   "set",
			DeviceId: "2",
			Data: mgw.DeviceInfo{
				Name:       "d2",
				DeviceType: "dt1",
				State:      "online",
			},
		},
		{
			Method:   "set",
			DeviceId: "3",
			Data: mgw.DeviceInfo{
				Name:       "d3",
				DeviceType: "dt2",
				State:      "online",
			},
		},
	}

	expected := append([]mgw.DeviceInfoUpdate{}, expectedBase...)
	expected = append(expected, expectedBase...)
	expected = append(expected, expectedBase...)
	expected = append(expected, mgw.DeviceInfoUpdate{
		Method:   "set",
		DeviceId: "4",
		Data: mgw.DeviceInfo{
			Name:       "d4",
			DeviceType: "dt2",
			State:      "online",
		},
	})
	expected = append(expected, expectedBase...)
	/*
		expected = append(expected, mgw.DeviceInfoUpdate{
			Method:   "delete",
			DeviceId: "4",
			Data: mgw.DeviceInfo{
				Name:       "d4",
				DeviceType: "dt2",
			},
		})
	*/
	for len(expected) < len(deviceInfos) {
		expected = append(expected, expectedBase...)
	}

	compareDeviceInfo := func(a mgw.DeviceInfoUpdate, b mgw.DeviceInfoUpdate) bool {
		return a.DeviceId+a.Method < b.DeviceId+b.Method
	}
	util.ListSort(expected, compareDeviceInfo)
	util.ListSort(deviceInfos, compareDeviceInfo)

	if !reflect.DeepEqual(deviceInfos, expected) {
		a, _ := json.Marshal(deviceInfos)
		e, _ := json.Marshal(expected)
		t.Error(string(a), "\n", string(e))
	}
}

func TestTimedDeviceInfoUpdateWithDelete(t *testing.T) {
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
		UpdatePeriod:          "2s",
		DeviceDescriptionsDir: "",
		MqttPw:                "",
		MqttUser:              "",
		MqttCmdClientId:       "mqttcmdclientid",
		MqttEventClientId:     "mqtteventclientid",
		MqttBroker:            "tcp://localhost:" + mqttPort,
		DeleteDevices:         true,
		MaxCorrelationIdAge:   "1m",
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

	topicDescProviderCalls := 0
	_, err = connector.NewWithFactories(ctx, conf, connector.NewTopicDescriptionProvider(func(config configuration.Config) ([]mocks.TopicDesc, error) {
		topicDescProviderCalls = topicDescProviderCalls + 1
		base := []mocks.TopicDesc{
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
		}
		extended := append([]mocks.TopicDesc{}, base...)
		extended = append(extended, mocks.TopicDesc{
			DeviceName: "d4",
			DeviceType: "dt2",
			DeviceId:   "4",
			ServiceId:  "1",
			EventTopic: "4/1",
			CmdTopic:   "",
			RespTopic:  "",
		})
		switch topicDescProviderCalls {
		case 1:
			fallthrough
		case 2:
			fallthrough
		case 3:
			return base, nil
		case 4:
			return extended, nil
		case 5:
			return base, nil
		default:
			return base, nil
		}

	}), connector.NewMgwFactory(mgw.New), connector.NewMqttFactory(mqtt.New))
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(11 * time.Second)

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

	expectedBase := []mgw.DeviceInfoUpdate{
		{
			Method:   "set",
			DeviceId: "1",
			Data: mgw.DeviceInfo{
				Name:       "d1",
				DeviceType: "dt1",
				State:      "online",
			},
		},
		{
			Method:   "set",
			DeviceId: "2",
			Data: mgw.DeviceInfo{
				Name:       "d2",
				DeviceType: "dt1",
				State:      "online",
			},
		},
		{
			Method:   "set",
			DeviceId: "3",
			Data: mgw.DeviceInfo{
				Name:       "d3",
				DeviceType: "dt2",
				State:      "online",
			},
		},
	}

	expected := append([]mgw.DeviceInfoUpdate{}, expectedBase...)
	expected = append(expected, expectedBase...)
	expected = append(expected, expectedBase...)
	expected = append(expected, mgw.DeviceInfoUpdate{
		Method:   "set",
		DeviceId: "4",
		Data: mgw.DeviceInfo{
			Name:       "d4",
			DeviceType: "dt2",
			State:      "online",
		},
	})
	expected = append(expected, expectedBase...)
	expected = append(expected, mgw.DeviceInfoUpdate{
		Method:   "delete",
		DeviceId: "4",
	})

	for len(expected) < len(deviceInfos) {
		expected = append(expected, expectedBase...)
	}

	compareDeviceInfo := func(a mgw.DeviceInfoUpdate, b mgw.DeviceInfoUpdate) bool {
		return a.DeviceId+a.Method < b.DeviceId+b.Method
	}
	util.ListSort(expected, compareDeviceInfo)
	util.ListSort(deviceInfos, compareDeviceInfo)

	if !reflect.DeepEqual(deviceInfos, expected) {
		a, _ := json.Marshal(deviceInfos)
		e, _ := json.Marshal(expected)
		t.Error(string(a), "\n", string(e))
	}
}

func TestSignaledDeviceInfoUpdate(t *testing.T) {
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
		MqttCmdClientId:       "mqttcmdclientid",
		MqttEventClientId:     "mqtteventclientid",
		MqttBroker:            "tcp://localhost:" + mqttPort,
		DeleteDevices:         false,
		MaxCorrelationIdAge:   "1m",
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

	topicDescProviderCalls := 0
	_, err = connector.NewWithFactories(ctx, conf, connector.NewTopicDescriptionProvider(func(config configuration.Config) ([]mocks.TopicDesc, error) {
		topicDescProviderCalls = topicDescProviderCalls + 1
		base := []mocks.TopicDesc{
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
		}
		extended := append([]mocks.TopicDesc{}, base...)
		extended = append(extended, mocks.TopicDesc{
			DeviceName: "d4",
			DeviceType: "dt2",
			DeviceId:   "4",
			ServiceId:  "1",
			EventTopic: "4/1",
			CmdTopic:   "",
			RespTopic:  "",
		})
		switch topicDescProviderCalls {
		case 1:
			fallthrough
		case 2:
			fallthrough
		case 3:
			return base, nil
		case 4:
			return extended, nil
		case 5:
			return base, nil
		default:
			return base, nil
		}

	}), connector.NewMgwFactory(mgw.New), connector.NewMqttFactory(mqtt.New))
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(1 * time.Second)

	for i := 0; i < 5; i++ {
		err = mqttListener.Publish("device-manager/refresh", 2, false, []byte("1"))
		if err != nil {
			t.Error(err)
			return
		}
		time.Sleep(2 * time.Second)
	}

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

	expectedBase := []mgw.DeviceInfoUpdate{
		{
			Method:   "set",
			DeviceId: "1",
			Data: mgw.DeviceInfo{
				Name:       "d1",
				DeviceType: "dt1",
				State:      "online",
			},
		},
		{
			Method:   "set",
			DeviceId: "2",
			Data: mgw.DeviceInfo{
				Name:       "d2",
				DeviceType: "dt1",
				State:      "online",
			},
		},
		{
			Method:   "set",
			DeviceId: "3",
			Data: mgw.DeviceInfo{
				Name:       "d3",
				DeviceType: "dt2",
				State:      "online",
			},
		},
	}

	expected := append([]mgw.DeviceInfoUpdate{}, expectedBase...)
	expected = append(expected, expectedBase...)
	expected = append(expected, expectedBase...)
	expected = append(expected, mgw.DeviceInfoUpdate{
		Method:   "set",
		DeviceId: "4",
		Data: mgw.DeviceInfo{
			Name:       "d4",
			DeviceType: "dt2",
			State:      "online",
		},
	})
	expected = append(expected, expectedBase...)
	for len(expected) < len(deviceInfos) {
		expected = append(expected, expectedBase...)
	}

	compareDeviceInfo := func(a mgw.DeviceInfoUpdate, b mgw.DeviceInfoUpdate) bool {
		return a.DeviceId+a.Method < b.DeviceId+b.Method
	}
	util.ListSort(expected, compareDeviceInfo)
	util.ListSort(deviceInfos, compareDeviceInfo)

	if !reflect.DeepEqual(deviceInfos, expected) {
		a, _ := json.Marshal(deviceInfos)
		e, _ := json.Marshal(expected)
		t.Error(string(a), "\n", string(e))
	}
}

func TestSignaledDeviceInfoUpdateWithDelete(t *testing.T) {
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
		MqttCmdClientId:       "mqttcmdclientid",
		MqttEventClientId:     "mqtteventclientid",
		MqttBroker:            "tcp://localhost:" + mqttPort,
		DeleteDevices:         true,
		MaxCorrelationIdAge:   "1m",
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

	topicDescProviderCalls := 0
	_, err = connector.NewWithFactories(ctx, conf, connector.NewTopicDescriptionProvider(func(config configuration.Config) ([]mocks.TopicDesc, error) {
		topicDescProviderCalls = topicDescProviderCalls + 1
		base := []mocks.TopicDesc{
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
		}
		extended := append([]mocks.TopicDesc{}, base...)
		extended = append(extended, mocks.TopicDesc{
			DeviceName: "d4",
			DeviceType: "dt2",
			DeviceId:   "4",
			ServiceId:  "1",
			EventTopic: "4/1",
			CmdTopic:   "",
			RespTopic:  "",
		})
		switch topicDescProviderCalls {
		case 1:
			fallthrough
		case 2:
			fallthrough
		case 3:
			return base, nil
		case 4:
			return extended, nil
		case 5:
			return base, nil
		default:
			return base, nil
		}

	}), connector.NewMgwFactory(mgw.New), connector.NewMqttFactory(mqtt.New))
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(1 * time.Second)

	for i := 0; i < 5; i++ {
		err = mqttListener.Publish("device-manager/refresh", 2, false, []byte("1"))
		if err != nil {
			t.Error(err)
			return
		}
		time.Sleep(2 * time.Second)
	}

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

	expectedBase := []mgw.DeviceInfoUpdate{
		{
			Method:   "set",
			DeviceId: "1",
			Data: mgw.DeviceInfo{
				Name:       "d1",
				DeviceType: "dt1",
				State:      "online",
			},
		},
		{
			Method:   "set",
			DeviceId: "2",
			Data: mgw.DeviceInfo{
				Name:       "d2",
				DeviceType: "dt1",
				State:      "online",
			},
		},
		{
			Method:   "set",
			DeviceId: "3",
			Data: mgw.DeviceInfo{
				Name:       "d3",
				DeviceType: "dt2",
				State:      "online",
			},
		},
	}

	expected := append([]mgw.DeviceInfoUpdate{}, expectedBase...)
	expected = append(expected, expectedBase...)
	expected = append(expected, expectedBase...)
	expected = append(expected, mgw.DeviceInfoUpdate{
		Method:   "set",
		DeviceId: "4",
		Data: mgw.DeviceInfo{
			Name:       "d4",
			DeviceType: "dt2",
			State:      "online",
		},
	})
	expected = append(expected, expectedBase...)
	expected = append(expected, mgw.DeviceInfoUpdate{
		Method:   "delete",
		DeviceId: "4",
	})

	for len(expected) < len(deviceInfos) {
		expected = append(expected, expectedBase...)
	}

	compareDeviceInfo := func(a mgw.DeviceInfoUpdate, b mgw.DeviceInfoUpdate) bool {
		return a.DeviceId+a.Method < b.DeviceId+b.Method
	}

	util.ListSort(expected, compareDeviceInfo)
	util.ListSort(deviceInfos, compareDeviceInfo)

	if !reflect.DeepEqual(deviceInfos, expected) {
		a, _ := json.Marshal(deviceInfos)
		e, _ := json.Marshal(expected)
		t.Error(string(a), "\n", string(e))
	}
}
