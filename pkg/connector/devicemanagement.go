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
)

func (this *Connector) updateTopics() (err error) {
	if this.topicDescProvider == nil {
		return errors.New("missing topicDescProvider")
	}
	topics, err := this.topicDescProvider(this.config)
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
		if _, ok := this.topicRegisterGet(topic.GetTopic()); !ok {
			err = this.addTopic(topic)
		} else {
			err = this.updateTopic(topic)
		}
		if err != nil {
			return err
		}
	}

	addedDevices := map[string]bool{}
	removedDevices := map[string]bool{}

	//find old devices to remove
	for id, desc := range oldDevices {
		if _, ok := usedDevices[id]; !ok {
			if _, ok2 := removedDevices[id]; !ok2 {
				removedDevices[id] = true
				err := this.removeDevice(desc)
				if err != nil {
					return err
				}
			}
		}
	}

	//find new devices to add
	for id, desc := range usedDevices {
		if _, ok := oldDevices[id]; !ok {
			if _, ok2 := addedDevices[id]; !ok2 {
				addedDevices[id] = true
				err := this.addDevice(desc)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (this *Connector) addTopic(topicDesc TopicDescription) error {
	topic := topicDesc.GetTopic()
	log.Println("add", topic)
	err := this.mqtt.Subscribe(topic, 2, this.EventHandler)
	if err != nil {
		return err
	}
	this.topicRegisterSet(topic, topicDesc)
	return nil
}

func (this *Connector) updateTopic(topic TopicDescription) error {
	log.Println("update", topic.GetTopic())
	this.topicRegisterSet(topic.GetTopic(), topic)
	return nil
}

func (this *Connector) removeTopic(topic string) error {
	log.Println("removeTopic", topic)
	err := this.mqtt.Unsubscribe(topic)
	if err != nil {
		return err
	}
	this.topicRegisterRemove(topic)
	return nil
}

func (this *Connector) addDevice(device DeviceDescription) error {
	return this.registerDevice(device.GetLocalDeviceId(), device.GetDeviceName(), device.GetDeviceTypeId())
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

func (this *Connector) registerDevice(id string, name string, deviceTypeid string) (err error) {
	err = this.mgwClient.SetDevice(id, name, deviceTypeid, "")
	if err != nil {
		log.Println("ERROR: unable to send device info to mgw", err)
		return err
	}
	err = this.mgwClient.ListenToDeviceCommands(id, this.CommandHandler)
	if err != nil {
		log.Println("ERROR: unable to subscribe to device commands", err)
		return err
	}
	return nil
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
