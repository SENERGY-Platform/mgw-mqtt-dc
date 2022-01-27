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

package mgw

import (
	"context"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/configuration"
	paho "github.com/eclipse/paho.mqtt.golang"
	"log"
	"sync"
	"time"
)

const DeviceManagerTopic = "device-manager/device"

type Client struct {
	mqtt                         paho.Client
	debug                        bool
	connectorId                  string
	subscriptions                map[string]paho.MessageHandler
	subscriptionsMux             sync.Mutex
	deviceManagerRefreshNotifier func()
}

func New(ctx context.Context, config configuration.Config, refreshNotifier func()) (*Client, error) {
	client := &Client{
		connectorId:                  config.ConnectorId,
		debug:                        config.Debug,
		deviceManagerRefreshNotifier: refreshNotifier,
		subscriptions:                map[string]paho.MessageHandler{},
	}
	lwt := "device-manager/device/" + config.ConnectorId + "/lw"
	options := paho.NewClientOptions().
		SetPassword(config.MgwMqttPw).
		SetUsername(config.MgwMqttUser).
		SetAutoReconnect(true).
		SetCleanSession(true).
		SetClientID(config.MgwMqttClientId).
		AddBroker(config.MgwMqttBroker).
		SetResumeSubs(true).
		SetWriteTimeout(10*time.Second).
		SetOrderMatters(false).
		SetConnectionLostHandler(func(_ paho.Client, err error) {
			log.Println("connection to mgw broker lost")
		}).
		SetOnConnectHandler(func(_ paho.Client) {
			log.Println("connected to mgw broker")
			err := client.initSubscriptions()
			if err != nil {
				log.Fatal("FATAL: ", err)
			}
			if client.deviceManagerRefreshNotifier != nil {
				client.deviceManagerRefreshNotifier()
			}
		}).SetWill(lwt, "offline", 2, false)

	client.mqtt = paho.NewClient(options)
	if token := client.mqtt.Connect(); token.Wait() && token.Error() != nil {
		log.Println("Error on MqttStart.Connect(): ", token.Error())
		return nil, token.Error()
	}

	go func() {
		<-ctx.Done()
		client.mqtt.Disconnect(0)
	}()

	return client, nil
}

func (this *Client) NotifyDeviceManagerRefresh(f func()) {
	this.deviceManagerRefreshNotifier = f
}
