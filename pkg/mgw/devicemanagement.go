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

package mgw

import (
	"encoding/json"
	"errors"
	"log"
)

func (this *Client) SetDeviceInfo(deviceId string, info DeviceInfo) error {
	return this.SendDeviceUpdate(DeviceInfoUpdate{
		Method:   "set",
		DeviceId: deviceId,
		Data:     info,
	})
}

func (this *Client) SetDevice(deviceId string, name string, deviceTypeid string, state string) error {
	return this.SendDeviceUpdate(DeviceInfoUpdate{
		Method:   "set",
		DeviceId: deviceId,
		Data: DeviceInfo{
			Name:       name,
			State:      State(state),
			DeviceType: deviceTypeid,
		},
	})
}

func (this *Client) RemoveDevice(deviceId string) error {
	return this.SendDeviceUpdate(DeviceInfoUpdate{
		Method:   "delete",
		DeviceId: deviceId,
	})
}

func (this *Client) SendDeviceUpdate(info DeviceInfoUpdate) error {
	if !this.mqtt.IsConnected() {
		log.Println("WARNING: mqtt client not connected")
		return errors.New("mqtt client not connected")
	}
	topic := DeviceManagerTopic + "/" + this.connectorId
	msg, err := json.Marshal(info)
	if this.debug {
		log.Println("DEBUG: publish ", topic, string(msg))
	}
	token := this.mqtt.Publish(topic, 2, false, string(msg))
	if token.Wait() && token.Error() != nil {
		log.Println("Error on Client.Publish(): ", token.Error())
		return token.Error()
	}
	return err
}
