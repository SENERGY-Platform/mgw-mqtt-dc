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

package mqtt

import (
	"context"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/configuration"
	paho "github.com/eclipse/paho.mqtt.golang"
	"log"
	"sync"
)

func New(ctx context.Context, config configuration.Config) (client *Mqtt, err error) {
	client = &Mqtt{
		config:           config,
		subscriptions:    map[string]paho.MessageHandler{},
		subscriptionsMux: sync.Mutex{},
		mqtt:             nil,
	}
	return client, client.init(ctx)
}

type Mqtt struct {
	config           configuration.Config
	subscriptions    map[string]paho.MessageHandler
	subscriptionsMux sync.Mutex
	mqtt             paho.Client
}

func (this *Mqtt) init(ctx context.Context) error {
	options := paho.NewClientOptions().
		SetPassword(this.config.MqttPw).
		SetUsername(this.config.MqttUser).
		SetAutoReconnect(true).
		SetCleanSession(true).
		SetClientID(this.config.MqttClientId).
		AddBroker(this.config.MqttBroker).
		SetResumeSubs(true).
		SetConnectionLostHandler(func(_ paho.Client, err error) {
			log.Println("connection to mqtt broker lost")
		}).
		SetOnConnectHandler(func(_ paho.Client) {
			log.Println("connected to mqtt broker")
			err := this.loadOldSubscriptions()
			if err != nil {
				log.Fatal("FATAL: ", err)
			}
		})

	this.mqtt = paho.NewClient(options)
	if token := this.mqtt.Connect(); token.Wait() && token.Error() != nil {
		log.Println("Error on MqttStart.Connect(): ", token.Error())
		return token.Error()
	}

	go func() {
		<-ctx.Done()
		this.mqtt.Disconnect(0)
	}()
	return nil
}
