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
	"errors"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/configuration"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/mgw"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/mqtt"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/topicdescription"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

type Connector struct {
	mgwClient            MgwClient
	topicRegister        map[string]TopicDescription
	topicRegisterMux     sync.Mutex
	config               configuration.Config
	updateTickerDuration time.Duration
	updateTicker         *time.Ticker
	topicDescProvider    TopicDescriptionProvider
	mqtt                 MqttClient
	updateTopicsMux      sync.Mutex
}

func New(ctx context.Context, config configuration.Config) (result *Connector, err error) {
	return NewWithFactories(ctx, config, NewTopicDescriptionProvider(topicdescription.Load), NewMgwFactory(mgw.New), NewMqttFactory(mqtt.New))
}

func NewWithFactories(ctx context.Context, config configuration.Config, topicDescProvider TopicDescriptionProvider, mgwFactory MgwFactory, mqttFactory MqttFactory) (result *Connector, err error) {
	mqttClient, err := mqttFactory(ctx, config)
	if err != nil {
		return result, err
	}
	mgwClient, err := mgwFactory(ctx, config, result.NotifyRefresh)
	if err != nil {
		return result, err
	}
	return NewWithInterfaces(ctx, config, topicDescProvider, mgwClient, mqttClient)
}

func NewWithInterfaces(ctx context.Context, config configuration.Config, topicDescProvider TopicDescriptionProvider, mgwClient MgwClient, mqttClient MqttClient) (result *Connector, err error) {
	result = &Connector{
		config:            config,
		topicRegister:     map[string]TopicDescription{},
		topicDescProvider: topicDescProvider,
		mqtt:              mqttClient,
		mgwClient:         mgwClient,
	}
	return result, result.start(ctx)
}

func (this *Connector) NotifyRefresh() {
	err := this.updateTopics()
	if err != nil {
		log.Println("ERROR: unable to update device registry after refresh notification:", err)
	}
	return
}

func (this *Connector) start(ctx context.Context) (err error) {
	err = this.startPeriodicalTopicRegistryUpdate(ctx)
	if err != nil {
		return err
	}
	return this.updateTopics()
}

func (this *Connector) startPeriodicalTopicRegistryUpdate(ctx context.Context) (err error) {
	if this.config.UpdatePeriod != "" && this.config.UpdatePeriod != "-" {
		this.updateTickerDuration, err = time.ParseDuration(this.config.UpdatePeriod)
		if err != nil {
			log.Println("ERROR: unable to parse update period as duration")
			return err
		}
		this.updateTicker = time.NewTicker(this.updateTickerDuration)

		go func() {
			<-ctx.Done()
			this.updateTicker.Stop()
		}()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-this.updateTicker.C:
					err = this.updateTopics()
					if err != nil {
						log.Println("ERROR:", err)
						debug.PrintStack()
					}
				}
			}
		}()
	}
	return nil
}

func (this *Connector) validateTopicDescriptions(topics []TopicDescription) error {
	deviceToName := map[string]string{}
	deviceToDeviceType := map[string]string{}
	for _, topic := range topics {
		event := topic.GetEventTopic()
		cmd := topic.GetCmdTopic()
		resp := topic.GetResponseTopic()
		t := topic.GetTopic()
		deviceId := topic.GetLocalDeviceId()
		deviceName := topic.GetDeviceName()
		deviceTypeId := topic.GetDeviceTypeId()
		if t == "" || cmd == event || (cmd != "" && event != "") {
			j, _ := json.Marshal(map[string]string{"t": t, "e": event, "c": cmd, "r": resp})
			return errors.New("invalid topic description: expect either event or command topic: " + string(j))
		}
		if known, exists := deviceToName[deviceId]; exists && known != deviceName {
			return errors.New("device " + deviceId + " has multiple name assignments: " + known + " and " + deviceName)
		} else {
			deviceToName[deviceId] = deviceName
		}
		if known, exists := deviceToDeviceType[deviceId]; exists && known != deviceTypeId {
			return errors.New("device " + deviceId + " has multiple device-type-id assignments: " + known + " and " + deviceTypeId)
		} else {
			deviceToDeviceType[deviceId] = deviceTypeId
		}
		if event != "" && resp != "" {
			j, _ := json.Marshal(map[string]string{"t": t, "e": event, "c": cmd, "r": resp})
			log.Println("WARNING: response topic will not be used if event topic is set", string(j))
		}
	}
	return nil
}

func (this *Connector) EventHandler(topic string, payload []byte) {
	//TODO
}

func (this *Connector) CommandHandler(deviceId string, serviceId string, command mgw.Command) {
	//TODO
}

func (this *Connector) ResponseHandler(topic string, payload []byte) {
	//TODO
}
