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
	"encoding/json"
	"errors"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
	"log"
)

func (this *Connector) validateTopicDescriptions(topics []TopicDescription) error {
	topics = util.ListFilterDuplicates(topics, func(a TopicDescription, b TopicDescription) bool {
		duplicate := EqualTopicDesc(a, b)
		if duplicate {
			log.Println("WARNING: found duplicate topic description:", descToStr(a))
		}
		return duplicate
	})

	eventTopicUsed := map[string]bool{}
	respTopicUsed := map[string]bool{}
	cmdTopicUsed := map[string]bool{}
	cmdIdUsed := map[string]bool{}

	deviceToName := map[string]string{}
	deviceToDeviceType := map[string]string{}
	for _, topic := range topics {
		event := topic.GetEventTopic()
		cmd := topic.GetCmdTopic()
		resp := topic.GetResponseTopic()
		deviceId := topic.GetLocalDeviceId()
		deviceName := topic.GetDeviceName()
		deviceTypeId := topic.GetDeviceTypeId()
		cmdId := getCommandIdFromDesc(topic)

		//check for invalid element
		if cmd == event || (cmd != "" && event != "") {
			j, _ := json.Marshal(map[string]string{"e": event, "c": cmd, "r": resp})
			return errors.New("invalid topic description: expect either event or command topic: " + string(j))
		}
		if resp != "" && cmd == "" {
			j, _ := json.Marshal(map[string]string{"e": event, "c": cmd, "r": resp})
			log.Println("WARNING: response topic will not be used if command topic is not set", string(j))
		}

		//check for name redefinition
		if known, exists := deviceToName[deviceId]; exists && known != deviceName {
			return errors.New("device " + deviceId + " has multiple name assignments: " + known + " and " + deviceName)
		} else {
			deviceToName[deviceId] = deviceName
		}

		//check for device-type redefinition
		if known, exists := deviceToDeviceType[deviceId]; exists && known != deviceTypeId {
			return errors.New("device " + deviceId + " has multiple device-type-id assignments: " + known + " and " + deviceTypeId)
		} else {
			deviceToDeviceType[deviceId] = deviceTypeId
		}

		//check for response topic reuse for commands
		if cmd != "" {
			cmdTopicUsed[cmd] = true
		}
		if resp != "" {
			if exists := cmdTopicUsed[resp]; exists {
				return errors.New("collision between command and response topic: " + resp)
			}
		}

		//check for device-id + service-id reuse in commands (a command topic can be used for mor than one service)
		if cmd != "" {
			if exists := cmdIdUsed[cmdId]; exists {
				return errors.New("reused device-id/service-id: " + cmdId)
			}
			cmdIdUsed[cmdId] = true
		}

		//check for event topic reuse for other events --> error
		if event != "" {
			if exists := eventTopicUsed[event]; exists {
				return errors.New("reused event topic: " + event)
			}
			eventTopicUsed[event] = true
		}

		//WARN if event and response topic collide (it's but warning would be nice)
		if resp != "" {
			respTopicUsed[resp] = true
			if eventTopicUsed[resp] {
				log.Println("WARNING: response topic is also used as event topic", resp)
			}
		}
		if event != "" && respTopicUsed[event] {
			log.Println("WARNING: event topic is also used as response topic", event)
		}
	}
	return nil
}

func descToStr(desc TopicDescription) string {
	event := desc.GetEventTopic()
	cmd := desc.GetCmdTopic()
	resp := desc.GetResponseTopic()
	deviceId := desc.GetLocalDeviceId()
	deviceName := desc.GetDeviceName()
	deviceTypeId := desc.GetDeviceTypeId()
	j, _ := json.Marshal(map[string]string{"e": event, "c": cmd, "r": resp, "d": deviceId, "n": deviceName, "dt": deviceTypeId})
	return string(j)
}
