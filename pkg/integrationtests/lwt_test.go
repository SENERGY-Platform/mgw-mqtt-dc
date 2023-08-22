/*
 * Copyright 2023 InfAI (CC SES)
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
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/mqtt"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
	"github.com/SENERGY-Platform/models/go/models"
	"log"
	"path"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestLwt(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	keycloakUrl, err := docker.Keycloak(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	managerUrl, repoUrl, searchUrl, err := docker.DeviceManagerWithDependencies(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

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

	config, err := configuration.Load("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	config.MqttBroker = "tcp://localhost:" + mqttPort
	config.MgwMqttBroker = "tcp://localhost:" + mgwPort
	config.GeneratorDeviceRepositoryUrl = repoUrl
	config.GeneratorPermissionSearchUrl = searchUrl
	config.ConnectorId = "test"
	config.MgwMqttClientId = "mgwclientid"
	config.MqttCmdClientId = "mqttcmdclientid"
	config.MqttEventClientId = "mqtteventclientid"
	config.GeneratorAuthUsername = "testuser"
	config.GeneratorAuthPassword = "testpw"
	config.GeneratorAuthEndpoint = keycloakUrl
	config.GeneratorUse = true

	tempDir := t.TempDir()

	config.FallbackFile = path.Join(tempDir, "fallback.json")
	config.GeneratorDeviceDescriptionsDir = tempDir
	config.DeviceDescriptionsDir = tempDir

	t.Logf("%#v", config)

	protocols := []models.Protocol{
		{
			Id:      "urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
			Name:    "standard-connector",
			Handler: "connector",
			ProtocolSegments: []models.ProtocolSegment{
				{
					Id:   "urn:infai:ses:protocol-segment:9956d8b5-46fa-4381-a227-c1df69808997",
					Name: "metadata",
				},
				{
					Id:   "urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65",
					Name: "data",
				},
			},
			Constraints: []string{"senergy_connector_local_id"},
		},
		{
			Id:      "urn:infai:ses:protocol:p1",
			Name:    "p1",
			Handler: "p1",
			ProtocolSegments: []models.ProtocolSegment{
				{
					Id:   "urn:infai:ses:protocol-segment:ps1",
					Name: "ps1",
				},
			},
		},
	}

	dtGosund := models.DeviceType{}
	err = json.Unmarshal([]byte(simplifiedGosundTestDeviceType), &dtGosund)
	if err != nil {
		t.Error(err)
		return
	}

	dtGosundWithoutLwt := models.DeviceType{}
	err = json.Unmarshal([]byte(simplifiedGosundTestDeviceTypeWithoutLwt), &dtGosundWithoutLwt)
	if err != nil {
		t.Error(err)
		return
	}

	deviceTypes := []models.DeviceType{dtGosund, dtGosundWithoutLwt}

	gosundDevice := models.Device{
		Id:           "urn:infai:ses:device:7e9201ee-3a64-4959-9a19-4429cf9b93d9",
		Name:         "Plug Kühlschrank",
		LocalId:      "gosund_sp1_02",
		DeviceTypeId: "urn:infai:ses:device-type:f4bb792a-b8d3-41d6-98a8-4407b5192d0e",
		Attributes: []models.Attribute{
			{Key: "GosundCmdPrefix", Value: "cmnd/"},
			{Key: "GosundEventPrefix", Value: "tele/"},
			{Key: "GosundRespPrefix", Value: "stat/"},
		},
	}

	gosundDeviceWithoutLwt := models.Device{
		Id:           "urn:infai:ses:device:7e9201ee-3a64-4959-9a19-4429cf9b93d0",
		Name:         "Plug Kühlschrank without LWT",
		LocalId:      "gosund_sp1_03",
		DeviceTypeId: "urn:infai:ses:device-type:f4bb792a-b8d3-41d6-98a8-4407b5192d00",
		Attributes: []models.Attribute{
			{Key: "GosundCmdPrefix", Value: "cmnd/"},
			{Key: "GosundEventPrefix", Value: "tele/"},
			{Key: "GosundRespPrefix", Value: "stat/"},
		},
	}

	devices := []models.Device{gosundDevice, gosundDeviceWithoutLwt}

	characteristics := []models.Characteristic{
		{
			Id:                 "urn:infai:ses:characteristic:819cb017-2331-40f2-8537-15508d6b82c5",
			Name:               "Binary State (\"0\"/\"1\")",
			Type:               "https://schema.org/Text",
			SubCharacteristics: []models.Characteristic{},
		},
		{
			Id:                 "urn:infai:ses:characteristic:c0353532-a8fb-4553-a00b-418cb8a80a65",
			Name:               "Binary State (0/1)",
			SubCharacteristics: []models.Characteristic{},
			Type:               "https://schema.org/Integer",
		},
		{
			Id:                 "urn:infai:ses:characteristic:bc03ef2e-51d5-4034-8bec-75df78e3afee",
			Name:               "Binary State (online/offline)",
			SubCharacteristics: []models.Characteristic{},
			Type:               "https://schema.org/Text",
		},
		{
			Id:                 "urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
			Name:               "Boolean",
			SubCharacteristics: []models.Characteristic{},
			Type:               "https://schema.org/Boolean",
		},
	}
	concepts := []models.Concept{{
		Id:   "urn:infai:ses:concept:85e11726-620a-4584-96a2-3a6fe4141b2d",
		Name: "Connection State",
		CharacteristicIds: []string{
			"urn:infai:ses:characteristic:819cb017-2331-40f2-8537-15508d6b82c5",
			"urn:infai:ses:characteristic:c0353532-a8fb-4553-a00b-418cb8a80a65",
			"urn:infai:ses:characteristic:bc03ef2e-51d5-4034-8bec-75df78e3afee",
			"urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
		},
		BaseCharacteristicId: "urn:infai:ses:characteristic:bc03ef2e-51d5-4034-8bec-75df78e3afee",
		Conversions: []models.ConverterExtension{
			{
				From:            "urn:infai:ses:characteristic:bc03ef2e-51d5-4034-8bec-75df78e3afee",
				To:              "urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
				Distance:        1,
				Formula:         "x == \"online\"",
				PlaceholderName: "x",
			},
			{
				From:            "urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
				To:              "urn:infai:ses:characteristic:bc03ef2e-51d5-4034-8bec-75df78e3afee",
				Distance:        1,
				Formula:         "x ? \"online\" : \"offline\"",
				PlaceholderName: "x",
			},
			{
				From:            "urn:infai:ses:characteristic:c0353532-a8fb-4553-a00b-418cb8a80a65",
				To:              "urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
				Distance:        1,
				Formula:         "x == 1",
				PlaceholderName: "x",
			},
			{
				From:            "urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
				To:              "urn:infai:ses:characteristic:c0353532-a8fb-4553-a00b-418cb8a80a65",
				Distance:        1,
				Formula:         "x ? 1 : 0",
				PlaceholderName: "x",
			},
			{
				From:            "urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
				To:              "urn:infai:ses:characteristic:819cb017-2331-40f2-8537-15508d6b82c5",
				Distance:        1,
				Formula:         "x ? \"1\" : \"0\"",
				PlaceholderName: "x",
			},
			{
				From:            "urn:infai:ses:characteristic:819cb017-2331-40f2-8537-15508d6b82c5",
				To:              "urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
				Distance:        1,
				Formula:         "x == \"1\"",
				PlaceholderName: "x",
			},
		},
	}}
	functions := []models.Function{{
		Id:          "urn:infai:ses:measuring-function:b8791b17-cf01-467f-87cf-da2271fffb6d",
		Name:        "Connection Status",
		DisplayName: "Connection Status",
		ConceptId:   "urn:infai:ses:concept:85e11726-620a-4584-96a2-3a6fe4141b2d",
		RdfType:     "https://senergy.infai.org/ontology/MeasuringFunction",
	}}

	t.Run("init device repo data", createTestMetadata(docker.TestToken, managerUrl, searchUrl, characteristics, concepts, functions, protocols, deviceTypes, devices))

	mqttClient, err := mqtt.New(ctx, config.MqttBroker, "testlistener", "", "")
	if err != nil {
		t.Error(err)
		return
	}

	mgwListener, err := mqtt.New(ctx, config.MgwMqttBroker, "testmgwlistener", "", "")
	if err != nil {
		t.Error(err)
		return
	}
	mgwMessages := util.NewSyncMap[[]string]()
	err = mgwListener.Subscribe("#", 2, func(topic string, _ bool, payload []byte) {
		log.Println("mgw", topic, string(payload))
		mgwMessages.Update(topic, func(messages []string) []string {
			return append(messages, string(payload))
		})
	})
	if err != nil {
		t.Error(err)
		return
	}

	_, err = connector.New(ctx, config)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(time.Second)

	lwtTopic := "tele/gosund_sp1_02/LWT"

	t.Run("send online lwt", func(t *testing.T) {
		err = mqttClient.Publish(lwtTopic, 2, true, []byte("online"))
		if err != nil {
			t.Error(err)
			return
		}
	})

	time.Sleep(time.Second)

	t.Run("send offline lwt", func(t *testing.T) {
		err = mqttClient.Publish(lwtTopic, 2, true, []byte("offline"))
		if err != nil {
			t.Error(err)
			return
		}
	})

	time.Sleep(time.Second)

	t.Run("send refresh request", func(t *testing.T) {
		err = mgwListener.Publish("device-manager/refresh", 2, false, []byte("1"))
		if err != nil {
			t.Error(err)
			return
		}
	})

	time.Sleep(time.Second)

	t.Run("send online lwt", func(t *testing.T) {
		err = mqttClient.Publish(lwtTopic, 2, true, []byte("online"))
		if err != nil {
			t.Error(err)
			return
		}
	})

	time.Sleep(time.Second)

	t.Run("send refresh request 2", func(t *testing.T) {
		err = mgwListener.Publish("device-manager/refresh", 2, false, []byte("1"))
		if err != nil {
			t.Error(err)
			return
		}
	})

	time.Sleep(time.Second)

	t.Run("check mgw messages", func(t *testing.T) {
		list, _ := mgwMessages.Get("device-manager/device/test")
		actual := map[string][]string{}
		for _, pl := range list {
			msg := testMsgType{}
			err = json.Unmarshal([]byte(pl), &msg)
			if err != nil {
				t.Error(err)
				return
			}
			actual[msg.DeviceId] = append(actual[msg.DeviceId], msg.Data.State)
		}
		expected := map[string][]string{
			"gosund_sp1_03": {"online", "online", "online"},
			"gosund_sp1_02": {"offline", "online", "offline", "offline", "online", "online"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("ln1=expected, ln2=actual\n%#v\n%#v\n", expected, actual)
		}
	})
}

func TestLwt2(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	keycloakUrl, err := docker.Keycloak(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	managerUrl, repoUrl, searchUrl, err := docker.DeviceManagerWithDependencies(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

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

	config, err := configuration.Load("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	config.MqttBroker = "tcp://localhost:" + mqttPort
	config.MgwMqttBroker = "tcp://localhost:" + mgwPort
	config.GeneratorDeviceRepositoryUrl = repoUrl
	config.GeneratorPermissionSearchUrl = searchUrl
	config.ConnectorId = "test"
	config.MgwMqttClientId = "mgwclientid"
	config.MqttCmdClientId = "mqttcmdclientid"
	config.MqttEventClientId = "mqtteventclientid"
	config.GeneratorAuthUsername = "testuser"
	config.GeneratorAuthPassword = "testpw"
	config.GeneratorAuthEndpoint = keycloakUrl
	config.GeneratorUse = true

	tempDir := t.TempDir()

	config.FallbackFile = path.Join(tempDir, "fallback.json")
	config.GeneratorDeviceDescriptionsDir = tempDir
	config.DeviceDescriptionsDir = tempDir

	t.Logf("%#v", config)

	protocols := []models.Protocol{
		{
			Id:      "urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
			Name:    "standard-connector",
			Handler: "connector",
			ProtocolSegments: []models.ProtocolSegment{
				{
					Id:   "urn:infai:ses:protocol-segment:9956d8b5-46fa-4381-a227-c1df69808997",
					Name: "metadata",
				},
				{
					Id:   "urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65",
					Name: "data",
				},
			},
			Constraints: []string{"senergy_connector_local_id"},
		},
		{
			Id:      "urn:infai:ses:protocol:p1",
			Name:    "p1",
			Handler: "p1",
			ProtocolSegments: []models.ProtocolSegment{
				{
					Id:   "urn:infai:ses:protocol-segment:ps1",
					Name: "ps1",
				},
			},
		},
	}

	dtGosund := models.DeviceType{}
	err = json.Unmarshal([]byte(simplifiedGosundTestDeviceType), &dtGosund)
	if err != nil {
		t.Error(err)
		return
	}

	dtGosundWithoutLwt := models.DeviceType{}
	err = json.Unmarshal([]byte(simplifiedGosundTestDeviceTypeWithoutLwt), &dtGosundWithoutLwt)
	if err != nil {
		t.Error(err)
		return
	}

	deviceTypes := []models.DeviceType{dtGosund, dtGosundWithoutLwt}

	gosundDevice := models.Device{
		Id:           "urn:infai:ses:device:7e9201ee-3a64-4959-9a19-4429cf9b93d9",
		Name:         "Plug Kühlschrank",
		LocalId:      "gosund_sp1_02",
		DeviceTypeId: "urn:infai:ses:device-type:f4bb792a-b8d3-41d6-98a8-4407b5192d0e",
		Attributes: []models.Attribute{
			{Key: "GosundCmdPrefix", Value: "cmnd/"},
			{Key: "GosundEventPrefix", Value: "tele/"},
			{Key: "GosundRespPrefix", Value: "stat/"},
		},
	}

	gosundDeviceWithoutLwt := models.Device{
		Id:           "urn:infai:ses:device:7e9201ee-3a64-4959-9a19-4429cf9b93d0",
		Name:         "Plug Kühlschrank without LWT",
		LocalId:      "gosund_sp1_03",
		DeviceTypeId: "urn:infai:ses:device-type:f4bb792a-b8d3-41d6-98a8-4407b5192d00",
		Attributes: []models.Attribute{
			{Key: "GosundCmdPrefix", Value: "cmnd/"},
			{Key: "GosundEventPrefix", Value: "tele/"},
			{Key: "GosundRespPrefix", Value: "stat/"},
		},
	}

	devices := []models.Device{gosundDevice, gosundDeviceWithoutLwt}

	characteristics := []models.Characteristic{
		{
			Id:                 "urn:infai:ses:characteristic:819cb017-2331-40f2-8537-15508d6b82c5",
			Name:               "Binary State (\"0\"/\"1\")",
			Type:               "https://schema.org/Text",
			SubCharacteristics: []models.Characteristic{},
		},
		{
			Id:                 "urn:infai:ses:characteristic:c0353532-a8fb-4553-a00b-418cb8a80a65",
			Name:               "Binary State (0/1)",
			SubCharacteristics: []models.Characteristic{},
			Type:               "https://schema.org/Integer",
		},
		{
			Id:                 "urn:infai:ses:characteristic:bc03ef2e-51d5-4034-8bec-75df78e3afee",
			Name:               "Binary State (online/offline)",
			SubCharacteristics: []models.Characteristic{},
			Type:               "https://schema.org/Text",
		},
		{
			Id:                 "urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
			Name:               "Boolean",
			SubCharacteristics: []models.Characteristic{},
			Type:               "https://schema.org/Boolean",
		},
	}
	concepts := []models.Concept{{
		Id:   "urn:infai:ses:concept:85e11726-620a-4584-96a2-3a6fe4141b2d",
		Name: "Connection State",
		CharacteristicIds: []string{
			"urn:infai:ses:characteristic:819cb017-2331-40f2-8537-15508d6b82c5",
			"urn:infai:ses:characteristic:c0353532-a8fb-4553-a00b-418cb8a80a65",
			"urn:infai:ses:characteristic:bc03ef2e-51d5-4034-8bec-75df78e3afee",
			"urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
		},
		BaseCharacteristicId: "urn:infai:ses:characteristic:bc03ef2e-51d5-4034-8bec-75df78e3afee",
		Conversions: []models.ConverterExtension{
			{
				From:            "urn:infai:ses:characteristic:bc03ef2e-51d5-4034-8bec-75df78e3afee",
				To:              "urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
				Distance:        1,
				Formula:         "x == \"online\"",
				PlaceholderName: "x",
			},
			{
				From:            "urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
				To:              "urn:infai:ses:characteristic:bc03ef2e-51d5-4034-8bec-75df78e3afee",
				Distance:        1,
				Formula:         "x ? \"online\" : \"offline\"",
				PlaceholderName: "x",
			},
			{
				From:            "urn:infai:ses:characteristic:c0353532-a8fb-4553-a00b-418cb8a80a65",
				To:              "urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
				Distance:        1,
				Formula:         "x == 1",
				PlaceholderName: "x",
			},
			{
				From:            "urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
				To:              "urn:infai:ses:characteristic:c0353532-a8fb-4553-a00b-418cb8a80a65",
				Distance:        1,
				Formula:         "x ? 1 : 0",
				PlaceholderName: "x",
			},
			{
				From:            "urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
				To:              "urn:infai:ses:characteristic:819cb017-2331-40f2-8537-15508d6b82c5",
				Distance:        1,
				Formula:         "x ? \"1\" : \"0\"",
				PlaceholderName: "x",
			},
			{
				From:            "urn:infai:ses:characteristic:819cb017-2331-40f2-8537-15508d6b82c5",
				To:              "urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
				Distance:        1,
				Formula:         "x == \"1\"",
				PlaceholderName: "x",
			},
		},
	}}
	functions := []models.Function{{
		Id:          "urn:infai:ses:measuring-function:b8791b17-cf01-467f-87cf-da2271fffb6d",
		Name:        "Connection Status",
		DisplayName: "Connection Status",
		ConceptId:   "urn:infai:ses:concept:85e11726-620a-4584-96a2-3a6fe4141b2d",
		RdfType:     "https://senergy.infai.org/ontology/MeasuringFunction",
	}}

	t.Run("init device repo data", createTestMetadata(docker.TestToken, managerUrl, searchUrl, characteristics, concepts, functions, protocols, deviceTypes, devices))

	mqttClient, err := mqtt.New(ctx, config.MqttBroker, "testlistener", "", "")
	if err != nil {
		t.Error(err)
		return
	}

	mgwListener, err := mqtt.New(ctx, config.MgwMqttBroker, "testmgwlistener", "", "")
	if err != nil {
		t.Error(err)
		return
	}
	mgwMessages := util.NewSyncMap[[]string]()
	err = mgwListener.Subscribe("#", 2, func(topic string, _ bool, payload []byte) {
		log.Println("mgw", topic, string(payload))
		mgwMessages.Update(topic, func(messages []string) []string {
			return append(messages, string(payload))
		})
	})
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(time.Second)

	lwtTopic := "tele/gosund_sp1_02/LWT"

	t.Run("send online lwt", func(t *testing.T) {
		err = mqttClient.Publish(lwtTopic, 2, true, []byte("online"))
		if err != nil {
			t.Error(err)
			return
		}
	})

	time.Sleep(5 * time.Second)

	t.Run("start connector", func(t *testing.T) {
		_, err = connector.New(ctx, config)
		if err != nil {
			t.Error(err)
			return
		}
	})

	time.Sleep(5 * time.Second)

	t.Run("send offline lwt", func(t *testing.T) {
		err = mqttClient.Publish(lwtTopic, 2, true, []byte("offline"))
		if err != nil {
			t.Error(err)
			return
		}
	})

	time.Sleep(time.Second)

	t.Run("send refresh request", func(t *testing.T) {
		err = mgwListener.Publish("device-manager/refresh", 2, false, []byte("1"))
		if err != nil {
			t.Error(err)
			return
		}
	})

	time.Sleep(time.Second)

	t.Run("send online lwt", func(t *testing.T) {
		err = mqttClient.Publish(lwtTopic, 2, true, []byte("online"))
		if err != nil {
			t.Error(err)
			return
		}
	})

	time.Sleep(time.Second)

	t.Run("send refresh request 2", func(t *testing.T) {
		err = mgwListener.Publish("device-manager/refresh", 2, false, []byte("1"))
		if err != nil {
			t.Error(err)
			return
		}
	})

	time.Sleep(time.Second)

	t.Run("check mgw messages", func(t *testing.T) {
		list, _ := mgwMessages.Get("device-manager/device/test")
		actual := map[string][]string{}
		for _, pl := range list {
			msg := testMsgType{}
			err = json.Unmarshal([]byte(pl), &msg)
			if err != nil {
				t.Error(err)
				return
			}
			actual[msg.DeviceId] = append(actual[msg.DeviceId], msg.Data.State)
		}
		expected := map[string][]string{
			"gosund_sp1_03": {"online", "online", "online"},
			"gosund_sp1_02": {"offline", "online", "offline", "offline", "online", "online"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("ln1=expected, ln2=actual\n%#v\n%#v\n", expected, actual)
		}
	})
}

type testMsgType struct {
	Method   string `json:"method"`
	DeviceId string `json:"device_id"`
	Data     struct {
		Name       string `json:"name"`
		State      string `json:"state"`
		DeviceType string `json:"device_type"`
	} `json:"data"`
}

const simplifiedGosundTestDeviceType = `{
    "id": "urn:infai:ses:device-type:f4bb792a-b8d3-41d6-98a8-4407b5192d0e",
    "name": "Gosund SP111",
    "description": "Gosund SP111 with Tasmota Firmware",
    "service_groups": [],
    "services": [
        {
            "id": "urn:infai:ses:service:97805820-ca0a-46c5-9dcf-16c2e386b050",
            "local_id": "SENSOR",
            "name": "Get Energy Consumption",
            "description": "",
            "interaction": "event",
            "protocol_id": "urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
            "inputs": [],
            "outputs": [
                {
                    "id": "urn:infai:ses:content:7d991834-64c8-4662-b2b7-f5251abc9841",
                    "content_variable": {
                        "id": "urn:infai:ses:content-variable:08f2f783-4af4-46fb-b416-e368c83b2a88",
                        "name": "sensor",
                        "is_void": false,
                        "omit_empty": false,
                        "type": "https://schema.org/StructuredValue",
                        "sub_content_variables": [
                            {
                                "id": "urn:infai:ses:content-variable:83fe8935-084f-4fb4-82f8-ec80c6bb6aef",
                                "name": "ENERGY",
                                "is_void": false,
                                "omit_empty": false,
                                "type": "https://schema.org/StructuredValue",
                                "sub_content_variables": [
                                    {
                                        "id": "urn:infai:ses:content-variable:d8b41ae2-d15e-4bdd-bb41-e8fb83932605",
                                        "name": "Total",
                                        "is_void": false,
                                        "omit_empty": false,
                                        "type": "https://schema.org/Float",
                                        "sub_content_variables": null,
                                        "characteristic_id": "urn:infai:ses:characteristic:3febed55-ba9b-43dc-8709-9c73bae3716e",
                                        "value": null,
                                        "serialization_options": null,
                                        "function_id": "urn:infai:ses:measuring-function:57dfd369-92db-462c-aca4-a767b52c972e",
                                        "aspect_id": "urn:infai:ses:aspect:fdc999eb-d366-44e8-9d24-bfd48d5fece1"
                                    }
                                ],
                                "characteristic_id": "",
                                "value": null,
                                "serialization_options": null
                            }
                        ],
                        "characteristic_id": "",
                        "value": null,
                        "serialization_options": null
                    },
                    "serialization": "json",
                    "protocol_segment_id": "urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
                }
            ],
            "attributes": [
                {
                    "key": "senergy/time_path",
                    "value": "",
                    "origin": "web-ui"
                },
                {
                    "key": "senergy/local-mqtt/event-topic-tmpl",
                    "value": "{{.GosundEventPrefix}}{{.Device}}/{{.Service}}",
                    "origin": "web-ui"
                }
            ],
            "service_group_key": ""
        },
        {
            "id": "urn:infai:ses:service:f0fa2af9-8348-4449-8eff-91a5f2e79b08",
            "local_id": "LWT",
            "name": "Get Lwt",
            "description": "",
            "interaction": "event",
            "protocol_id": "urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
            "inputs": [],
            "outputs": [
                {
                    "id": "urn:infai:ses:content:604669ef-dbc0-4ddc-bf68-b1b596f69d3d",
                    "content_variable": {
                        "id": "urn:infai:ses:content-variable:65a7a401-7d51-4fec-976c-16d1ad3d5024",
                        "name": "lwt",
                        "is_void": false,
                        "omit_empty": false,
                        "type": "https://schema.org/Text",
                        "sub_content_variables": null,
                        "characteristic_id": "urn:infai:ses:characteristic:bc03ef2e-51d5-4034-8bec-75df78e3afee",
                        "value": null,
                        "serialization_options": null,
                        "function_id": "urn:infai:ses:measuring-function:b8791b17-cf01-467f-87cf-da2271fffb6d",
                        "aspect_id": "urn:infai:ses:aspect:861227f6-1523-46a7-b8ab-a4e76f0bdd32"
                    },
                    "serialization": "plain-text",
                    "protocol_segment_id": "urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
                }
            ],
            "attributes": [
                {
                    "key": "senergy/time_path",
                    "value": "",
                    "origin": "web-ui"
                },
                {
                    "key": "senergy/local-mqtt/event-topic-tmpl",
                    "value": "{{.GosundEventPrefix}}{{.Device}}/{{.Service}}",
                    "origin": "web-ui"
                }
            ],
            "service_group_key": ""
        },
        {
            "id": "urn:infai:ses:service:8900391e-28b6-49c2-b90d-a57a6ba02315",
            "local_id": "STATE",
            "name": "Get State",
            "description": "",
            "interaction": "event",
            "protocol_id": "urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
            "inputs": [],
            "outputs": [
                {
                    "id": "urn:infai:ses:content:369fb506-e6d5-4f6f-ae2c-e08a8f19b5b0",
                    "content_variable": {
                        "id": "urn:infai:ses:content-variable:e8fd8ec5-224f-4614-b1ab-fe0bcae799a9",
                        "name": "sensor",
                        "is_void": false,
                        "omit_empty": false,
                        "type": "https://schema.org/StructuredValue",
                        "sub_content_variables": [
                            {
                                "id": "urn:infai:ses:content-variable:2b73ce2f-be09-41bd-adbc-fdf788cf8f21",
                                "name": "Uptime",
                                "is_void": false,
                                "omit_empty": false,
                                "type": "https://schema.org/Text",
                                "sub_content_variables": null,
                                "characteristic_id": "",
                                "value": null,
                                "serialization_options": null
                            }
                        ],
                        "characteristic_id": "",
                        "value": null,
                        "serialization_options": null
                    },
                    "serialization": "json",
                    "protocol_segment_id": "urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
                }
            ],
            "attributes": [
                {
                    "key": "senergy/time_path",
                    "value": "",
                    "origin": "web-ui"
                },
                {
                    "key": "senergy/local-mqtt/event-topic-tmpl",
                    "value": "{{.GosundEventPrefix}}{{.Device}}/{{.Service}}",
                    "origin": "web-ui"
                }
            ],
            "service_group_key": ""
        },
        {
            "id": "urn:infai:ses:service:34fea1b1-791a-488c-9bfc-ff56b67cc8a0",
            "local_id": "POWER",
            "name": "Set Off",
            "description": "",
            "interaction": "request",
            "protocol_id": "urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
            "inputs": [
                {
                    "id": "urn:infai:ses:content:b6d653fe-e1d3-4170-8d12-f1be25e173bd",
                    "content_variable": {
                        "id": "urn:infai:ses:content-variable:43a2a87a-8c0a-4d00-b350-f22f41036215",
                        "name": "state",
                        "is_void": false,
                        "omit_empty": false,
                        "type": "https://schema.org/Text",
                        "sub_content_variables": null,
                        "characteristic_id": "urn:infai:ses:characteristic:7621686a-56bc-402d-b4cc-5b266d39736f",
                        "value": "OFF",
                        "serialization_options": null,
                        "function_id": "urn:infai:ses:controlling-function:2f35150b-9df7-4cad-95bc-165fa00219fd",
                        "aspect_id": "urn:infai:ses:aspect:861227f6-1523-46a7-b8ab-a4e76f0bdd32"
                    },
                    "serialization": "plain-text",
                    "protocol_segment_id": "urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
                }
            ],
            "outputs": [],
            "attributes": [
                {
                    "key": "senergy/time_path",
                    "value": "",
                    "origin": "web-ui"
                },
                {
                    "key": "senergy/local-mqtt/cmd-topic-tmpl",
                    "value": "{{.GosundCmdPrefix}}{{.Device}}/{{.Service}}",
                    "origin": "web-ui"
                },
                {
                    "key": "senergy/local-mqtt/resp-topic-tmpl",
                    "value": "{{.GosundRespPrefix}}{{.Device}}/{{.Service}}",
                    "origin": "web-ui"
                }
            ],
            "service_group_key": ""
        },
        {
            "id": "urn:infai:ses:service:421b7e4c-0288-4a63-a5c5-53137fe17325",
            "local_id": "POWER",
            "name": "Set On",
            "description": "",
            "interaction": "request",
            "protocol_id": "urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
            "inputs": [
                {
                    "id": "urn:infai:ses:content:9f5b9f8e-3279-413b-ac88-05ddf0e72f10",
                    "content_variable": {
                        "id": "urn:infai:ses:content-variable:4d0fec19-741f-4b0a-8f8f-b34271af4617",
                        "name": "state",
                        "is_void": false,
                        "omit_empty": false,
                        "type": "https://schema.org/Text",
                        "sub_content_variables": null,
                        "characteristic_id": "urn:infai:ses:characteristic:7621686a-56bc-402d-b4cc-5b266d39736f",
                        "value": "ON",
                        "serialization_options": null,
                        "function_id": "urn:infai:ses:controlling-function:79e7914b-f303-4a7d-90af-dee70db05fd9",
                        "aspect_id": "urn:infai:ses:aspect:861227f6-1523-46a7-b8ab-a4e76f0bdd32"
                    },
                    "serialization": "plain-text",
                    "protocol_segment_id": "urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
                }
            ],
            "outputs": [],
            "attributes": [
                {
                    "key": "senergy/time_path",
                    "value": "",
                    "origin": "web-ui"
                },
                {
                    "key": "senergy/local-mqtt/cmd-topic-tmpl",
                    "value": "{{.GosundCmdPrefix}}{{.Device}}/{{.Service}}",
                    "origin": "web-ui"
                },
                {
                    "key": "senergy/local-mqtt/resp-topic-tmpl",
                    "value": "{{.GosundRespPrefix}}{{.Device}}/{{.Service}}",
                    "origin": "web-ui"
                }
            ],
            "service_group_key": ""
        }
    ],
    "device_class_id": "urn:infai:ses:device-class:79de1bd9-b933-412d-b98e-4cfe19aa3250",
    "attributes": [
        {
            "key": "senergy/local-mqtt",
            "value": "true",
            "origin": "web-ui"
        }
    ]
}`
const simplifiedGosundTestDeviceTypeWithoutLwt = `{
    "id": "urn:infai:ses:device-type:f4bb792a-b8d3-41d6-98a8-4407b5192d00",
    "name": "Gosund SP111 without lwt",
    "description": "Gosund SP111 with Tasmota Firmware",
    "service_groups": [],
    "services": [
        {
            "id": "urn:infai:ses:service:97805820-ca0a-46c5-9dcf-16c2e386b051",
            "local_id": "SENSOR",
            "name": "Get Energy Consumption",
            "description": "",
            "interaction": "event",
            "protocol_id": "urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
            "inputs": [],
            "outputs": [
                {
                    "id": "urn:infai:ses:content:7d991834-64c8-4662-b2b7-f5251abc9840",
                    "content_variable": {
                        "id": "urn:infai:ses:content-variable:08f2f783-4af4-46fb-b416-e368c83b2a80",
                        "name": "sensor",
                        "is_void": false,
                        "omit_empty": false,
                        "type": "https://schema.org/StructuredValue",
                        "sub_content_variables": [
                            {
                                "id": "urn:infai:ses:content-variable:83fe8935-084f-4fb4-82f8-ec80c6bb6ae0",
                                "name": "ENERGY",
                                "is_void": false,
                                "omit_empty": false,
                                "type": "https://schema.org/StructuredValue",
                                "sub_content_variables": [
                                    {
                                        "id": "urn:infai:ses:content-variable:d8b41ae2-d15e-4bdd-bb41-e8fb83932600",
                                        "name": "Total",
                                        "is_void": false,
                                        "omit_empty": false,
                                        "type": "https://schema.org/Float",
                                        "sub_content_variables": null,
                                        "characteristic_id": "urn:infai:ses:characteristic:3febed55-ba9b-43dc-8709-9c73bae3716e",
                                        "value": null,
                                        "serialization_options": null,
                                        "function_id": "urn:infai:ses:measuring-function:57dfd369-92db-462c-aca4-a767b52c972e",
                                        "aspect_id": "urn:infai:ses:aspect:fdc999eb-d366-44e8-9d24-bfd48d5fece1"
                                    }
                                ],
                                "characteristic_id": "",
                                "value": null,
                                "serialization_options": null
                            }
                        ],
                        "characteristic_id": "",
                        "value": null,
                        "serialization_options": null
                    },
                    "serialization": "json",
                    "protocol_segment_id": "urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
                }
            ],
            "attributes": [
                {
                    "key": "senergy/time_path",
                    "value": "",
                    "origin": "web-ui"
                },
                {
                    "key": "senergy/local-mqtt/event-topic-tmpl",
                    "value": "{{.GosundEventPrefix}}{{.Device}}/{{.Service}}",
                    "origin": "web-ui"
                }
            ],
            "service_group_key": ""
        },
        {
            "id": "urn:infai:ses:service:8900391e-28b6-49c2-b90d-a57a6ba02310",
            "local_id": "STATE",
            "name": "Get State",
            "description": "",
            "interaction": "event",
            "protocol_id": "urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
            "inputs": [],
            "outputs": [
                {
                    "id": "urn:infai:ses:content:369fb506-e6d5-4f6f-ae2c-e08a8f19b5b1",
                    "content_variable": {
                        "id": "urn:infai:ses:content-variable:e8fd8ec5-224f-4614-b1ab-fe0bcae799a9",
                        "name": "sensor",
                        "is_void": false,
                        "omit_empty": false,
                        "type": "https://schema.org/StructuredValue",
                        "sub_content_variables": [
                            {
                                "id": "urn:infai:ses:content-variable:2b73ce2f-be09-41bd-adbc-fdf788cf8f20",
                                "name": "Uptime",
                                "is_void": false,
                                "omit_empty": false,
                                "type": "https://schema.org/Text",
                                "sub_content_variables": null,
                                "characteristic_id": "",
                                "value": null,
                                "serialization_options": null
                            }
                        ],
                        "characteristic_id": "",
                        "value": null,
                        "serialization_options": null
                    },
                    "serialization": "json",
                    "protocol_segment_id": "urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
                }
            ],
            "attributes": [
                {
                    "key": "senergy/time_path",
                    "value": "",
                    "origin": "web-ui"
                },
                {
                    "key": "senergy/local-mqtt/event-topic-tmpl",
                    "value": "{{.GosundEventPrefix}}{{.Device}}/{{.Service}}",
                    "origin": "web-ui"
                }
            ],
            "service_group_key": ""
        },
        {
            "id": "urn:infai:ses:service:34fea1b1-791a-488c-9bfc-ff56b67cc8a1",
            "local_id": "POWER",
            "name": "Set Off",
            "description": "",
            "interaction": "request",
            "protocol_id": "urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
            "inputs": [
                {
                    "id": "urn:infai:ses:content:b6d653fe-e1d3-4170-8d12-f1be25e173b0",
                    "content_variable": {
                        "id": "urn:infai:ses:content-variable:43a2a87a-8c0a-4d00-b350-f22f41036210",
                        "name": "state",
                        "is_void": false,
                        "omit_empty": false,
                        "type": "https://schema.org/Text",
                        "sub_content_variables": null,
                        "characteristic_id": "urn:infai:ses:characteristic:7621686a-56bc-402d-b4cc-5b266d39736f",
                        "value": "OFF",
                        "serialization_options": null,
                        "function_id": "urn:infai:ses:controlling-function:2f35150b-9df7-4cad-95bc-165fa00219fd",
                        "aspect_id": "urn:infai:ses:aspect:861227f6-1523-46a7-b8ab-a4e76f0bdd32"
                    },
                    "serialization": "plain-text",
                    "protocol_segment_id": "urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
                }
            ],
            "outputs": [],
            "attributes": [
                {
                    "key": "senergy/time_path",
                    "value": "",
                    "origin": "web-ui"
                },
                {
                    "key": "senergy/local-mqtt/cmd-topic-tmpl",
                    "value": "{{.GosundCmdPrefix}}{{.Device}}/{{.Service}}",
                    "origin": "web-ui"
                },
                {
                    "key": "senergy/local-mqtt/resp-topic-tmpl",
                    "value": "{{.GosundRespPrefix}}{{.Device}}/{{.Service}}",
                    "origin": "web-ui"
                }
            ],
            "service_group_key": ""
        },
        {
            "id": "urn:infai:ses:service:421b7e4c-0288-4a63-a5c5-53137fe17320",
            "local_id": "POWER",
            "name": "Set On",
            "description": "",
            "interaction": "request",
            "protocol_id": "urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
            "inputs": [
                {
                    "id": "urn:infai:ses:content:9f5b9f8e-3279-413b-ac88-05ddf0e72f11",
                    "content_variable": {
                        "id": "urn:infai:ses:content-variable:4d0fec19-741f-4b0a-8f8f-b34271af4610",
                        "name": "state",
                        "is_void": false,
                        "omit_empty": false,
                        "type": "https://schema.org/Text",
                        "sub_content_variables": null,
                        "characteristic_id": "urn:infai:ses:characteristic:7621686a-56bc-402d-b4cc-5b266d39736f",
                        "value": "ON",
                        "serialization_options": null,
                        "function_id": "urn:infai:ses:controlling-function:79e7914b-f303-4a7d-90af-dee70db05fd9",
                        "aspect_id": "urn:infai:ses:aspect:861227f6-1523-46a7-b8ab-a4e76f0bdd32"
                    },
                    "serialization": "plain-text",
                    "protocol_segment_id": "urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
                }
            ],
            "outputs": [],
            "attributes": [
                {
                    "key": "senergy/time_path",
                    "value": "",
                    "origin": "web-ui"
                },
                {
                    "key": "senergy/local-mqtt/cmd-topic-tmpl",
                    "value": "{{.GosundCmdPrefix}}{{.Device}}/{{.Service}}",
                    "origin": "web-ui"
                },
                {
                    "key": "senergy/local-mqtt/resp-topic-tmpl",
                    "value": "{{.GosundRespPrefix}}{{.Device}}/{{.Service}}",
                    "origin": "web-ui"
                }
            ],
            "service_group_key": ""
        }
    ],
    "device_class_id": "urn:infai:ses:device-class:79de1bd9-b933-412d-b98e-4cfe19aa3250",
    "attributes": [
        {
            "key": "senergy/local-mqtt",
            "value": "true",
            "origin": "web-ui"
        }
    ]
}`
