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
	"errors"
	"log"
	"sort"
)

func (this *Connector) updateTopics() (err error) {
	this.updateTopicsMux.Lock()
	defer this.updateTopicsMux.Unlock()
	if this.topicDescProvider == nil {
		return errors.New("missing topicDescProvider")
	}
	topics, err := this.topicDescProvider(this.config)
	if err != nil {
		return err
	}

	sort.Slice(topics, func(i, j int) bool {
		return topics[i].GetTopic() < topics[j].GetTopic()
	})

	err = this.validateTopicDescriptions(topics)
	if err != nil {
		return err
	}

	old := this.topicRegisterGetAll()
	oldDevices := map[string]TopicDescription{}
	for _, topic := range old {
		oldDevices[topic.GetLocalDeviceId()] = topic
	}

	usedDevices := map[string]TopicDescription{}

	// populate topic registry and usedDevices
	for _, topic := range topics {
		usedDevices[topic.GetLocalDeviceId()] = topic
		if old, ok := this.topicRegisterGet(topic.GetTopic()); !ok {
			err = this.addTopic(topic)
		} else if !EqualTopicDesc(old, topic) {
			err = this.updateTopic(topic)
		}
		if err != nil {
			return err
		}
	}

	addedDevices := map[string]bool{}
	removedDevices := map[string]bool{}

	//find old devices to remove
	for id, oldDesc := range oldDevices {
		if _, ok := usedDevices[id]; !ok {
			if _, ok2 := removedDevices[id]; !ok2 {
				removedDevices[id] = true
				err := this.removeDevice(oldDesc)
				if err != nil {
					return err
				}
			}
		}
	}

	//find new devices to add/update
	for id, desc := range usedDevices {
		if oldDesc, ok := oldDevices[id]; !ok {
			if _, ok2 := addedDevices[id]; !ok2 {
				addedDevices[id] = true
				err := this.addDevice(desc)
				if err != nil {
					return err
				}
			}
		} else if !EqualDeviceDesc(oldDesc, desc) {
			err := this.updateDevice(desc)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (this *Connector) addTopic(topicDesc TopicDescription) (err error) {
	eventTopic := topicDesc.GetEventTopic()
	respTopic := topicDesc.GetResponseTopic()
	if eventTopic != "" {
		err = this.mqtt.Subscribe(eventTopic, 2, this.EventHandler)
	} else if respTopic != "" {
		err = this.mqtt.Subscribe(respTopic, 2, this.ResponseHandler)
	}
	if err != nil {
		return err
	}
	this.topicRegisterSet(topicDesc.GetTopic(), topicDesc)
	return nil
}

func (this *Connector) updateTopic(topic TopicDescription) error {
	err := this.removeTopic(topic.GetTopic())
	if err != nil {
		return err
	}
	return this.addTopic(topic)
}

func (this *Connector) removeTopic(topic string) (err error) {
	desc, exists := this.topicRegisterGet(topic)
	if !exists {
		return nil
	}
	eventTopic := desc.GetEventTopic()
	respTopic := desc.GetResponseTopic()
	if eventTopic != "" {
		err = this.mqtt.Unsubscribe(eventTopic)
	} else if respTopic != "" {
		err = this.mqtt.Unsubscribe(respTopic)
	}
	this.topicRegisterRemove(topic)
	return nil
}

func (this *Connector) addDevice(device DeviceDescription) (err error) {
	err = this.mgwClient.SetDevice(device.GetLocalDeviceId(), device.GetDeviceName(), device.GetDeviceTypeId(), "")
	if err != nil {
		log.Println("ERROR: unable to send device info to mgw", err)
		return err
	}
	err = this.mgwClient.ListenToDeviceCommands(device.GetLocalDeviceId(), this.CommandHandler)
	if err != nil {
		log.Println("ERROR: unable to subscribe to device commands", err)
		return err
	}
	return nil
}

func (this *Connector) updateDevice(device DeviceDescription) (err error) {
	err = this.mgwClient.SetDevice(device.GetLocalDeviceId(), device.GetDeviceName(), device.GetDeviceTypeId(), "")
	if err != nil {
		log.Println("ERROR: unable to send device info to mgw", err)
		return err
	}
	return nil
}

func (this *Connector) removeDevice(device DeviceDescription) error {
	id := device.GetLocalDeviceId()
	if this.config.DeleteDevices {
		log.Println("delete device", device.GetDeviceName(), id)
		err := this.mgwClient.RemoveDevice(id)
		if err != nil {
			return err
		}
	} else {
		log.Println("topic description has ben removed but device deletion is disabled", device.GetDeviceName(), id)
	}
	return this.mgwClient.StopListenToDeviceCommands(id)
}

func (this *Connector) topicRegisterSet(topic string, desc TopicDescription) {
	this.topicRegisterMux.Lock()
	defer this.topicRegisterMux.Unlock()
	this.topicRegister[topic] = desc
}

func (this *Connector) topicRegisterRemove(topic string) {
	this.topicRegisterMux.Lock()
	defer this.topicRegisterMux.Unlock()
	delete(this.topicRegister, topic)
}

func (this *Connector) topicRegisterGet(topic string) (desc TopicDescription, ok bool) {
	this.topicRegisterMux.Lock()
	defer this.topicRegisterMux.Unlock()
	desc, ok = this.topicRegister[topic]
	return
}

func (this *Connector) topicRegisterGetTopics() (topics []string) {
	this.topicRegisterMux.Lock()
	defer this.topicRegisterMux.Unlock()
	for topic, _ := range this.topicRegister {
		topics = append(topics, topic)
	}
	return
}

func (this *Connector) topicRegisterGetAll() (result map[string]TopicDescription) {
	result = map[string]TopicDescription{}
	this.topicRegisterMux.Lock()
	defer this.topicRegisterMux.Unlock()
	for key, value := range this.topicRegister {
		result[key] = value
	}
	return
}

func EqualTopicDesc(old TopicDescription, topic TopicDescription) bool {
	if EqualDeviceDesc(old, topic) &&
		old.GetTopic() == topic.GetTopic() &&
		old.GetEventTopic() == topic.GetEventTopic() &&
		old.GetResponseTopic() == topic.GetResponseTopic() &&
		old.GetCmdTopic() == topic.GetCmdTopic() &&
		old.GetLocalServiceId() == topic.GetLocalServiceId() {
		return true
	}
	return false
}

func EqualDeviceDesc(old DeviceDescription, topic DeviceDescription) bool {
	if old.GetDeviceName() == topic.GetDeviceName() &&
		old.GetLocalDeviceId() == topic.GetLocalDeviceId() &&
		old.GetDeviceTypeId() == topic.GetDeviceTypeId() {
		return true
	}
	return false
}
