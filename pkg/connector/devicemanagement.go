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
	"net/url"
	"strings"

	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"

	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/mgw"
)

func (this *Connector) updateTopics() (err error) {
	this.updateTopicsMux.Lock()
	defer this.updateTopicsMux.Unlock()
	if this.topicDescProvider == nil {
		return errors.New("missing topicDescProvider")
	}
	topics, err := this.topicDescProvider(this.config, this.devicerepo)
	if err != nil {
		return err
	}

	topics, rejectedDevices := this.validateTopicDescriptions(topics)
	for deviceId, reasons := range rejectedDevices {
		reason := strings.Join(reasons, "; ")
		this.config.GetLogger().Warn("rejected invalid device topic descriptions", "deviceLocalId", deviceId, "reason", reason)
		this.mgwClient.SendDeviceError(deviceId, "rejected invalid device topic descriptions: "+reason)
	}

	events, commands, responses := this.splitTopicDescriptions(topics)

	err = this.onlineCheck.Preprocess(events)
	if err != nil {
		return err
	}

	oldDevices := map[string]TopicDescription{}
	usedDevices := map[string]TopicDescription{}

	// populate event topic registry and usedDevices
	// delay subscriptions after device management/registration to ensure evaluation of retained messages
	oldEvents := this.eventTopicRegister.GetAll()
	usedEvents := map[string]bool{}
	addEvents := []TopicDescription{}
	updateEvents := []TopicDescription{}

	for _, topic := range events {
		usedEvents[topic.GetEventTopic()] = true
		usedDevices[topic.GetLocalDeviceId()] = topic
		if old, ok := this.eventTopicRegister.Get(topic.GetEventTopic()); !ok {
			addEvents = append(addEvents, topic)
		} else if !EqualTopicDesc(old, topic) {
			updateEvents = append(updateEvents, topic)
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

	deviceHasEvents := map[string]bool{}
	for _, topic := range events {
		deviceHasEvents[topic.GetLocalDeviceId()] = true
	}

	addedDevices := map[string]bool{}
	removedDevices := map[string]bool{}

	//find old devices to remove
	for id, oldDesc := range oldDevices {
		if _, ok := usedDevices[id]; !ok {
			if _, ok2 := removedDevices[id]; !ok2 {
				removedDevices[id] = true
				if !this.deviceIsActive(id) {
					// the device has never been registered at the mgw
					continue
				}
				// a rejected device is misconfigured, not removed: keep it in the mgw, so that the device error stays visible
				if _, rejected := rejectedDevices[id]; rejected {
					err = this.mgwClient.StopListenToDeviceCommands(id)
				} else {
					err = this.removeDevice(oldDesc)
				}
				if err != nil {
					return err
				}
			}
		}
	}

	//find new devices to add/update
	for id, desc := range usedDevices {
		wasActive := this.deviceIsActive(id)
		if !wasActive && !deviceHasEvents[id] {
			// without event topics there is no event that could activate the device
			this.storeActivatedDevice(id)
		}
		if !this.deviceIsActive(id) {
			continue
		}
		state := mgw.Online
		if temp, ok := this.onlineCheck.LoadState(desc); ok {
			state = temp
		}
		err = this.mgwClient.SetDevice(desc.GetLocalDeviceId(), desc.GetDeviceName(), desc.GetDeviceTypeId(), string(state))
		if err != nil {
			this.config.GetLogger().Error("unable to send device info to mgw", "error", err)
			this.mgwClient.SendClientError("unable to send device info to mgw: " + util.MgwErrorMessage(err))
			return err
		}
		if _, ok := oldDevices[id]; !ok || !wasActive {
			if _, ok2 := addedDevices[id]; !ok2 {
				addedDevices[id] = true
				err := this.addDeviceCommandListener(desc)
				if err != nil {
					return err
				}
			}
		}
	}

	// forget devices without topic descriptions; rejected devices stay in the mgw and therefore stay activated
	err = this.activatedDevices.Retain(func(id string) bool {
		_, used := usedDevices[id]
		_, rejected := rejectedDevices[id]
		return used || rejected
	})
	if err != nil {
		this.config.GetLogger().Error("unable to store activated devices", "error", err)
		this.mgwClient.SendClientError("unable to store activated devices: " + util.MgwErrorMessage(err))
	}

	//update subscriptions (only after device registration to ensure evaluation of retained messages)
	for _, topic := range addEvents {
		err = this.addEvent(topic)
		if err != nil {
			return err
		}
	}
	for _, topic := range updateEvents {
		err = this.updateEvent(topic)
		if err != nil {
			return err
		}
	}

	return nil
}

// deviceIsActive reports whether the device is registered at the mgw.
// if config.ActivateDevicesOnEvent is set, a device is registered only after its first event.
func (this *Connector) deviceIsActive(localDeviceId string) bool {
	return !this.config.ActivateDevicesOnEvent || this.activatedDevices.Contains(localDeviceId)
}

func (this *Connector) storeActivatedDevice(localDeviceId string) {
	// Add keeps the id in memory even if the file could not be written
	err := this.activatedDevices.Add(localDeviceId)
	if err != nil {
		this.config.GetLogger().Error("unable to store activated devices", "error", err)
		this.mgwClient.SendClientError("unable to store activated devices: " + util.MgwErrorMessage(err))
	}
}

// activateDevice registers the device of desc at the mgw on its first event.
// returns false if the topic description has been removed in the meantime.
func (this *Connector) activateDevice(desc TopicDescription) (active bool, err error) {
	this.updateTopicsMux.Lock()
	defer this.updateTopicsMux.Unlock()
	id := desc.GetLocalDeviceId()
	if this.deviceIsActive(id) {
		return true, nil
	}
	// the description may have changed since the event has been received
	current, ok := this.eventTopicRegister.Get(desc.GetEventTopic())
	if !ok || current.GetLocalDeviceId() != id {
		return false, nil
	}
	desc = current
	this.config.GetLogger().Info("activate device after first event", "deviceName", desc.GetDeviceName(), "deviceLocalId", id)
	state := mgw.Online
	if temp, ok := this.onlineCheck.LoadState(desc); ok {
		state = temp
	}
	err = this.mgwClient.SetDevice(id, desc.GetDeviceName(), desc.GetDeviceTypeId(), string(state))
	if err != nil {
		return false, err
	}
	err = this.addDeviceCommandListener(desc)
	if err != nil {
		return false, err
	}
	this.storeActivatedDevice(id)
	return true, nil
}

func getCommandIdFromDesc(desc TopicDescription) string {
	return getCommandId(desc.GetLocalDeviceId(), desc.GetLocalServiceId())
}

func getCommandId(deviceId string, serviceId string) string {
	return url.PathEscape(deviceId) + "/" + url.PathEscape(serviceId)
}

func (this *Connector) addDeviceCommandListener(device DeviceDescription) (err error) {
	this.config.GetLogger().Debug("add device command listener", "deviceName", device.GetDeviceName(), "deviceLocalId", device.GetLocalDeviceId())
	err = this.mgwClient.ListenToDeviceCommands(device.GetLocalDeviceId(), this.CommandHandler)
	if err != nil {
		this.config.GetLogger().Error("unable to subscribe to device commands", "error", err)
		this.mgwClient.SendClientError("unable to subscribe to device commands: " + util.MgwErrorMessage(err))
		return err
	}
	return nil
}

func (this *Connector) removeDevice(device DeviceDescription) error {
	this.config.GetLogger().Debug("try to remove device", "deviceName", device.GetDeviceName(), "deviceLocalId", device.GetLocalDeviceId())
	id := device.GetLocalDeviceId()
	if this.config.DeleteDevices {
		this.config.GetLogger().Info("delete device from platform", "deviceName", device.GetDeviceName(), "deviceLocalId", id)
		err := this.mgwClient.RemoveDevice(id)
		if err != nil {
			return err
		}
	} else {
		this.config.GetLogger().Info("topic description has ben removed but device deletion is disabled", "deviceName", device.GetDeviceName(), "deviceLocalId", id)
	}
	return this.mgwClient.StopListenToDeviceCommands(id)
}
