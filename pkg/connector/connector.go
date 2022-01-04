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
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/mqtt"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/topicdescription"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

type Connector struct {
	mgwClient             MgwClient
	config                configuration.Config
	updateTickerDuration  time.Duration
	updateTicker          *time.Ticker
	topicDescProvider     TopicDescriptionProvider
	mqtt                  MqttClient
	updateTopicsMux       sync.Mutex
	eventTopicRegister    *util.SyncMap[TopicDescription]
	responseTopicRegister *util.SyncMap[TopicDescription]
	commandTopicRegister  *util.SyncMap[TopicDescription]
	correlationStore      *util.SyncMap[[]string]
}

func New(ctx context.Context, config configuration.Config) (result *Connector, err error) {
	return NewWithFactories(ctx, config, NewTopicDescriptionProvider(topicdescription.Load), NewMgwFactory(mgw.New), NewMqttFactory(mqtt.New))
}

func NewWithFactories(ctx context.Context, config configuration.Config, topicDescProvider TopicDescriptionProvider, mgwFactory MgwFactory, mqttFactory MqttFactory) (result *Connector, err error) {
	mqttClient, err := mqttFactory(ctx, config.MqttBroker, config.MqttClientId, config.MqttUser, config.MqttPw)
	if err != nil {
		return result, err
	}

	result = &Connector{
		config:                config,
		topicDescProvider:     topicDescProvider,
		mqtt:                  mqttClient,
		eventTopicRegister:    util.NewSyncMap[TopicDescription](),
		responseTopicRegister: util.NewSyncMap[TopicDescription](),
		commandTopicRegister:  util.NewSyncMap[TopicDescription](),
		correlationStore:      util.NewSyncMap[[]string](),
	}

	result.mgwClient, err = mgwFactory(ctx, config, result.NotifyRefresh)
	if err != nil {
		return result, err
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

func (this *Connector) splitTopicDescriptions(topics []TopicDescription) (events []TopicDescription, commands []TopicDescription, responses []TopicDescription) {
	for _, topic := range topics {
		if topic.GetEventTopic() != "" {
			events = append(events, topic)
		}
		if topic.GetResponseTopic() != "" && topic.GetCmdTopic() != "" {
			responses = append(responses, topic)
		}
		if topic.GetCmdTopic() != "" {
			commands = append(commands, topic)
		}
	}
	return
}
