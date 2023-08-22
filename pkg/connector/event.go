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

import "log"

func (this *Connector) EventHandler(topic string, retained bool, payload []byte) {
	desc, ok := this.eventTopicRegister.Get(topic)
	if !ok {
		if this.config.Debug {
			log.Println("DEBUG: ignore unregistered event", topic, string(payload))
		}
		return
	}
	if this.config.Debug {
		log.Println("DEBUG: receive event", topic, string(payload))
	}
	go func() {
		err := this.mgwClient.SendEvent(desc.GetLocalDeviceId(), desc.GetLocalServiceId(), payload)
		if err != nil {
			log.Println("ERROR: unable to send event to mgw", err)
			this.mgwClient.SendDeviceError(desc.GetLocalDeviceId(), "unable to send event to mgw: "+err.Error())
		}
	}()
	go func() {
		state, ignore := this.onlineCheck.CheckAndStoreState(desc, retained, payload)
		if !ignore {
			err := this.mgwClient.SetDevice(desc.GetLocalDeviceId(), desc.GetDeviceName(), desc.GetDeviceTypeId(), string(state))
			if err != nil {
				log.Println("ERROR: unable to send device info to mgw", err)
				this.mgwClient.SendClientError("unable to send device info to mgw: " + err.Error())
			}
		}
	}()
}

func (this *Connector) addEvent(topicDesc TopicDescription) (err error) {
	if this.config.Debug {
		log.Println("DEBUG: add event listener", topicDesc)
	}
	eventTopic := topicDesc.GetEventTopic()
	this.eventTopicRegister.Set(eventTopic, topicDesc)
	err = this.eventMqttClient.Subscribe(eventTopic, 2, this.EventHandler)
	if err != nil {
		return err
	}
	return nil
}

func (this *Connector) updateEvent(topic TopicDescription) error {
	if this.config.Debug {
		log.Println("DEBUG: update event listener", topic)
	}
	err := this.removeEvent(topic.GetEventTopic())
	if err != nil {
		return err
	}
	return this.addEvent(topic)
}

func (this *Connector) removeEvent(topic string) (err error) {
	if this.config.Debug {
		log.Println("DEBUG: remove event listener", topic)
	}
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
