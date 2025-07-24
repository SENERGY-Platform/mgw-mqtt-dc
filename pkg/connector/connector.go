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
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/connector/onlinechecker"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/devicerepo"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/devicerepo/auth"
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
	commandMqttClient     MqttClient
	eventMqttClient       MqttClient
	updateTopicsMux       sync.Mutex
	eventTopicRegister    *util.SyncMap[TopicDescription]
	responseTopicRegister *util.SyncMap[TopicDescription]
	commandTopicRegister  *util.SyncMap[TopicDescription]
	correlationStore      *util.SyncMap[[]CorrelationId]
	MaxCorrelationIdAge   time.Duration
	onlineCheck           OnlineChecker
	devicerepo            *devicerepo.DeviceRepo
}

type OnlineChecker interface {
	Preprocess(events []TopicDescription) error
	LoadState(desc TopicDescription) (state mgw.State, found bool)
	CheckAndStoreState(desc TopicDescription, retained bool, payload []byte) (state mgw.State, ignore bool)
}

func New(ctx context.Context, config configuration.Config) (result *Connector, err error) {
	return NewWithFactories(ctx, config, NewTopicDescriptionProvider(topicdescription.Load), NewMgwFactory(mgw.New), NewMqttFactory(mqtt.New))
}

func NewWithFactories(ctx context.Context, config configuration.Config, topicDescProvider TopicDescriptionProvider, mgwFactory MgwFactory, mqttFactory MqttFactory) (result *Connector, err error) {
	if config.MaxCorrelationIdAge == "" {
		config.MaxCorrelationIdAge = "90s"
	}
	if config.DeviceRepoCacheDuration == "" {
		config.DeviceRepoCacheDuration = "10m"
	}

	a := &auth.Auth{Credentials: auth.Credentials{
		MgwCertManagerUrl: config.GeneratorMgwCertManagerUrl,
		AuthEndpoint:      config.GeneratorAuthEndpoint,
		AuthClientId:      config.GeneratorAuthClientId,
		AuthClientSecret:  config.GeneratorAuthClientSecret,
		Username:          config.GeneratorAuthUsername,
		Password:          config.GeneratorAuthPassword,
	}}
	repo, err := devicerepo.New(devicerepo.RepoConfig{
		DeviceRepositoryUrl: config.GeneratorDeviceRepositoryUrl,
		CacheDuration:       config.DeviceRepoCacheDuration,
		FallbackFile:        config.FallbackFile,
	}, a)
	if err != nil {
		return result, err
	}

	checker, err := onlinechecker.New[TopicDescription](config, repo)
	if err != nil {
		return result, err
	}

	commandMqttClient, err := mqttFactory(ctx, config.MqttBroker, config.MqttCmdClientId, config.MqttUser, config.MqttPw, config.MqttInsecureSkipVerify)
	if err != nil {
		return result, err
	}

	eventMqttClient, err := mqttFactory(ctx, config.MqttBroker, config.MqttEventClientId, config.MqttUser, config.MqttPw, config.MqttInsecureSkipVerify)
	if err != nil {
		return result, err
	}

	result = &Connector{
		config:                config,
		topicDescProvider:     topicDescProvider,
		commandMqttClient:     commandMqttClient,
		eventMqttClient:       eventMqttClient,
		eventTopicRegister:    util.NewSyncMap[TopicDescription](),
		responseTopicRegister: util.NewSyncMap[TopicDescription](),
		commandTopicRegister:  util.NewSyncMap[TopicDescription](),
		correlationStore:      util.NewSyncMap[[]CorrelationId](),
		onlineCheck:           checker,
		devicerepo:            repo,
	}
	result.MaxCorrelationIdAge, err = time.ParseDuration(config.MaxCorrelationIdAge)
	if err != nil {
		return result, err
	}

	result.mgwClient, err = mgwFactory(ctx, config, result.RefreshDeviceInfo)
	if err != nil {
		return result, err
	}

	return result, result.start(ctx)
}

func (this *Connector) RefreshDeviceInfo() {
	err := this.updateTopics()
	if err != nil {
		log.Println("ERROR: unable to update device registry after refresh notification:", err)
		this.mgwClient.SendClientError("unable to update device registry after refresh notification: " + err.Error())
	}
	return
}

func (this *Connector) start(ctx context.Context) (err error) {
	err = this.startPeriodicalTopicRegistryUpdate(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (this *Connector) startPeriodicalTopicRegistryUpdate(ctx context.Context) (err error) {
	if this.config.UpdatePeriod != "" && this.config.UpdatePeriod != "-" {
		this.updateTickerDuration, err = time.ParseDuration(this.config.UpdatePeriod)
		if err != nil {
			log.Println("ERROR: unable to parse update period as duration")
			this.mgwClient.SendClientError("unable to parse update period as duration: " + err.Error())
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
						this.mgwClient.SendClientError(err.Error())
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
