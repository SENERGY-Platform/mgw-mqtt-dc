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
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
)

func (this *Connector) ResponseHandler(topic string, retained bool, payload []byte) {
	go func() {
		desc, isRegistered := this.responseTopicRegister.Get(topic)
		if !isRegistered {
			return
		}

		deviceId := desc.GetLocalDeviceId()
		serviceId := desc.GetLocalServiceId()
		cmdId := getCommandId(deviceId, serviceId)
		correlationId, correlationExists := this.popCorrelationId(cmdId)

		if desc.HasTransformations() {
			var err error
			payload, err = this.handleTransformations(desc, TransformerJsonUnwrapOutput, payload)
			if err != nil {
				this.config.GetLogger().Error("unable to transform response", "deviceId", deviceId, "serviceId", serviceId, "error", err)
				this.mgwClient.SendDeviceError(desc.GetLocalDeviceId(), "unable to transform response: "+util.MgwErrorMessage(err))
				return
			}
		}

		if !correlationExists {
			this.config.GetLogger().Debug("no correlation id stored for response", "topic", topic, "cmdId", cmdId)
			return
		}
		err := this.mgwClient.Respond(deviceId, serviceId, mgw.Command{
			CommandId: correlationId,
			Data:      string(payload),
		})
		if err != nil {
			this.config.GetLogger().Error("unable to send response", "deviceId", deviceId, "serviceId", serviceId, "error", err)
			this.mgwClient.SendCommandError(correlationId, "unable to send response: "+util.MgwErrorMessage(err))
		}
	}()
}

func (this *Connector) addResponse(topicDesc TopicDescription) (err error) {
	this.config.GetLogger().Debug("add response listener", "topic", topicDesc.GetResponseTopic())
	responseTopic := topicDesc.GetResponseTopic()
	err = this.commandMqttClient.Subscribe(responseTopic, 2, this.ResponseHandler)
	if err != nil {
		return err
	}
	this.responseTopicRegister.Set(responseTopic, topicDesc)
	return nil
}

func (this *Connector) updateResponse(topic TopicDescription) error {
	this.config.GetLogger().Debug("update response listener", "topic", topic.GetResponseTopic())
	err := this.removeResponse(topic.GetResponseTopic())
	if err != nil {
		return err
	}
	return this.addResponse(topic)
}

func (this *Connector) removeResponse(topic string) (err error) {
	this.config.GetLogger().Debug("remove response listener", "topic", topic)
	desc, exists := this.responseTopicRegister.Get(topic)
	if !exists {
		return nil
	}
	err = this.commandMqttClient.Unsubscribe(desc.GetResponseTopic())
	if err != nil {
		return err
	}
	this.responseTopicRegister.Remove(topic)
	return nil
}
