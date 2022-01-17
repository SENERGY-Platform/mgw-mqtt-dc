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
	"net/url"
)

const DeviceState = "online"

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

	err = this.validateTopicDescriptions(topics)
	if err != nil {
		return err
	}

	events, commands, responses := this.splitTopicDescriptions(topics)
	oldDevices := map[string]TopicDescription{}
	usedDevices := map[string]TopicDescription{}

	// populate event topic registry and usedDevices
	oldEvents := this.eventTopicRegister.GetAll()
	usedEvents := map[string]bool{}
	for _, topic := range events {
		usedEvents[topic.GetEventTopic()] = true
		usedDevices[topic.GetLocalDeviceId()] = topic
		if old, ok := this.eventTopicRegister.Get(topic.GetEventTopic()); !ok {
			err = this.addEvent(topic)
		} else if !EqualTopicDesc(old, topic) {
			err = this.updateEvent(topic)
		}
		if err != nil {
			return err
		}
	}
	for key, topic := range oldEvents {
		oldDevices[topic.GetLocalDeviceId()] = topic
		if _, used := usedEvents[key]; !used {
			err = this.removeEvent(key)
			if err != nil {
				return err
			}
		}
	}

	// populate response registry and usedDevices
	oldResponses := this.responseTopicRegister.GetAll()
	usedResponses := map[string]bool{}
	for _, topic := range responses {
		usedResponses[topic.GetResponseTopic()] = true
		usedDevices[topic.GetLocalDeviceId()] = topic
		if old, ok := this.responseTopicRegister.Get(topic.GetResponseTopic()); !ok {
			err = this.addResponse(topic)
		} else if !EqualTopicDesc(old, topic) {
			err = this.updateResponse(topic)
		}
		if err != nil {
			return err
		}
	}
	for key, topic := range oldResponses {
		oldDevices[topic.GetLocalDeviceId()] = topic
		if _, notDeleted := usedResponses[key]; !notDeleted {
			err = this.removeResponse(key)
			if err != nil {
				return err
			}
		}
	}

	// populate commands registry and usedDevices
	oldCommands := this.commandTopicRegister.GetAll()
	usedCommands := map[string]bool{}
	for _, topic := range commands {
		usedDevices[topic.GetLocalDeviceId()] = topic
		commandId := getCommandIdFromDesc(topic)
		usedCommands[commandId] = true
		this.commandTopicRegister.Set(commandId, topic)
	}
	for key, topic := range oldCommands {
		oldDevices[topic.GetLocalDeviceId()] = topic
		if _, notDeleted := usedCommands[key]; !notDeleted {
			this.commandTopicRegister.Remove(key)
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
		err = this.mgwClient.SetDevice(desc.GetLocalDeviceId(), desc.GetDeviceName(), desc.GetDeviceTypeId(), DeviceState)
		if err != nil {
			log.Println("ERROR: unable to send device info to mgw", err)
			this.mgwClient.SendClientError("unable to send device info to mgw: " + err.Error())
			return err
		}
		if _, ok := oldDevices[id]; !ok {
			if _, ok2 := addedDevices[id]; !ok2 {
				addedDevices[id] = true
				err := this.addDeviceCommandListener(desc)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func getCommandIdFromDesc(desc TopicDescription) string {
	return getCommandId(desc.GetLocalDeviceId(), desc.GetLocalServiceId())
}

func getCommandId(deviceId string, serviceId string) string {
	return url.PathEscape(deviceId) + "/" + url.PathEscape(serviceId)
}

func (this *Connector) addDeviceCommandListener(device DeviceDescription) (err error) {
	if this.config.Debug {
		log.Println("DEBUG: add device command listener", device)
	}
	err = this.mgwClient.ListenToDeviceCommands(device.GetLocalDeviceId(), this.CommandHandler)
	if err != nil {
		log.Println("ERROR: unable to subscribe to device commands", err)
		this.mgwClient.SendClientError("unable to subscribe to device commands: " + err.Error())
		return err
	}
	return nil
}

func (this *Connector) removeDevice(device DeviceDescription) error {
	if this.config.Debug {
		log.Println("DEBUG: remove device", device)
	}
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
