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

func (this *Connector) EventHandler(topic string, payload []byte) {
	go func() {
		desc, ok := this.eventTopicRegister.Get(topic)
		if !ok {
			log.Println("WARNING: got event for unknown topic description", topic)
			return
		}
		err := this.mgwClient.SendEvent(desc.GetLocalDeviceId(), desc.GetLocalServiceId(), payload)
		if err != nil {
			log.Println("ERROR: unable to send event to mgw", err)
		}
	}()
}

func (this *Connector) addEvent(topicDesc TopicDescription) (err error) {
	eventTopic := topicDesc.GetEventTopic()
	err = this.mqtt.Subscribe(eventTopic, 2, this.EventHandler)
	if err != nil {
		return err
	}
	this.eventTopicRegister.Set(eventTopic, topicDesc)
	return nil
}

func (this *Connector) updateEvent(topic TopicDescription) error {
	err := this.removeEvent(topic.GetEventTopic())
	if err != nil {
		return err
	}
	return this.addEvent(topic)
}

func (this *Connector) removeEvent(topic string) (err error) {
	desc, exists := this.eventTopicRegister.Get(topic)
	if !exists {
		return nil
	}
	err = this.mqtt.Unsubscribe(desc.GetEventTopic())
	if err != nil {
		return err
	}
	this.eventTopicRegister.Remove(topic)
	return nil
}
