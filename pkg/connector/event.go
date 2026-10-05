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

import "github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"

func (this *Connector) EventHandler(topic string, retained bool, payload []byte) {
	desc, ok := this.eventTopicRegister.Get(topic)
	if !ok {
		this.config.GetLogger().Debug("got event for unknown device description", "topic", topic, "payload", string(payload))
		return
	}
	this.config.GetLogger().Debug("receive event", "topic", topic, "payload", string(payload))
	if desc.HasTransformations() {
		var err error
		payload, err = this.handleTransformations(desc, TransformerJsonUnwrapOutput, payload)
		if err != nil {
			this.config.GetLogger().Error("unable to transform event", "topic", topic, "error", err)
			this.mgwClient.SendDeviceError(desc.GetLocalDeviceId(), "unable to transform event: "+util.MgwErrorMessage(err))
			return
		}
	}
	if this.deviceIsActive(desc.GetLocalDeviceId()) {
		this.forwardEvent(topic, desc, retained, payload)
	} else {
		go func() {
			active, err := this.activateDevice(desc)
			if err != nil {
				this.config.GetLogger().Error("unable to activate device", "topic", topic, "error", err)
				this.mgwClient.SendClientError("unable to activate device: " + util.MgwErrorMessage(err))
				return
			}
			if !active {
				this.config.GetLogger().Debug("drop event of removed device description", "topic", topic)
				return
			}
			this.forwardEvent(topic, desc, retained, payload)
		}()
	}
}

func (this *Connector) forwardEvent(topic string, desc TopicDescription, retained bool, payload []byte) {
	go func() {
		err := this.mgwClient.SendEvent(desc.GetLocalDeviceId(), desc.GetLocalServiceId(), payload)
		if err != nil {
			this.config.GetLogger().Error("unable to send event to mgw", "topic", topic, "error", err)
			this.mgwClient.SendDeviceError(desc.GetLocalDeviceId(), "unable to send event to mgw: "+util.MgwErrorMessage(err))
		}
	}()
	go func() {
		state, ignore := this.onlineCheck.CheckAndStoreState(desc, retained, payload)
		if !ignore {
			err := this.mgwClient.SetDevice(desc.GetLocalDeviceId(), desc.GetDeviceName(), desc.GetDeviceTypeId(), string(state))
			if err != nil {
				this.config.GetLogger().Error("unable to send device info to mgw", "error", err)
				this.mgwClient.SendClientError("unable to send device info to mgw: " + util.MgwErrorMessage(err))
			}
		}
	}()
}

func (this *Connector) addEvent(topicDesc TopicDescription) (err error) {
	this.config.GetLogger().Debug("add event listener", "topic", topicDesc.GetEventTopic())
	eventTopic := topicDesc.GetEventTopic()
	this.eventTopicRegister.Set(eventTopic, topicDesc)
	err = this.eventMqttClient.Subscribe(eventTopic, 2, this.EventHandler)
	if err != nil {
		return err
	}
	return nil
}

func (this *Connector) updateEvent(topic TopicDescription) error {
	this.config.GetLogger().Debug("update event listener", "topic", topic.GetEventTopic())
	err := this.removeEvent(topic.GetEventTopic())
	if err != nil {
		return err
	}
	return this.addEvent(topic)
}

func (this *Connector) removeEvent(topic string) (err error) {
	this.config.GetLogger().Debug("remove event listener", "topic", topic)
	desc, exists := this.eventTopicRegister.Get(topic)
	if !exists {
		return nil
	}
	err = this.eventMqttClient.Unsubscribe(desc.GetEventTopic())
	if err != nil {
		return err
	}
	this.eventTopicRegister.Remove(topic)
	return nil
}
