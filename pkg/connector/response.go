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
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/mgw"
	"log"
)

func (this *Connector) ResponseHandler(topic string, payload []byte) {
	go func() {
		desc, isRegistered := this.responseTopicRegister.Get(topic)
		if !isRegistered {
			log.Println("ERROR: handling of unregistered response topic", topic)
			return
		}
		deviceId := desc.GetLocalDeviceId()
		serviceId := desc.GetLocalServiceId()
		cmdId := getCommandId(deviceId, serviceId)
		correlationId, correlationExists := this.popCorrelationId(cmdId)
		if !correlationExists {
			log.Println("ERROR: no correlation id stored for response", topic, cmdId)
			return
		}
		err := this.mgwClient.Respond(deviceId, serviceId, mgw.Command{
			CommandId: correlationId,
			Data:      string(payload),
		})
		if err != nil {
			log.Println("ERROR: unable to send response", err)
		}
	}()
}

func (this *Connector) addResponse(topicDesc TopicDescription) (err error) {
	responseTopic := topicDesc.GetResponseTopic()
	err = this.mqtt.Subscribe(responseTopic, 2, this.ResponseHandler)
	if err != nil {
		return err
	}
	this.responseTopicRegister.Set(responseTopic, topicDesc)
	return nil
}

func (this *Connector) updateResponse(topic TopicDescription) error {
	err := this.removeResponse(topic.GetResponseTopic())
	if err != nil {
		return err
	}
	return this.addResponse(topic)
}

func (this *Connector) removeResponse(topic string) (err error) {
	desc, exists := this.responseTopicRegister.Get(topic)
	if !exists {
		return nil
	}
	err = this.mqtt.Unsubscribe(desc.GetResponseTopic())
	if err != nil {
		return err
	}
	this.responseTopicRegister.Remove(topic)
	return nil
}
