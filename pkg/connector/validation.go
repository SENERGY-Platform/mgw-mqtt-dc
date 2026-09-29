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
	"slices"

	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
)

// validateTopicDescriptions returns the valid topic descriptions and the rejected devices with their reasons.
// a device is always rejected as a whole, to prevent partially registered devices.
// if a conflict involves multiple devices (e.g. a reused event topic), all of them are rejected,
// because it is not decidable which one is configured correctly.
func (this *Connector) validateTopicDescriptions(topics []TopicDescription) (valid []TopicDescription, rejected map[string][]string) {
	topics = util.ListFilterDuplicates(topics, func(a TopicDescription, b TopicDescription) bool {
		duplicate := EqualTopicDesc(a, b)
		if duplicate {
			this.config.GetLogger().Warn("found duplicate topic description", "topic", descToStr(a))
		}
		return duplicate
	})

	rejected = map[string][]string{}
	reject := func(deviceIds []string, reason string) {
		for _, deviceId := range util.ListFilterDuplicates(deviceIds, func(a string, b string) bool { return a == b }) {
			rejected[deviceId] = append(rejected[deviceId], reason)
		}
	}

	eventTopicUsers := map[string][]string{}
	cmdTopicUsers := map[string][]string{}
	respTopicUsers := map[string][]string{}
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
			reject([]string{deviceId}, "invalid topic description: expect either event or command topic: "+string(j))
			continue
		}
		if resp != "" && cmd == "" {
			this.config.GetLogger().Warn("response topic will not be used if command topic is not set", "event_topic", event, "cmd_topic", cmd, "resp_topic", resp)
		}

		//check for name redefinition
		if known, exists := deviceToName[deviceId]; exists && known != deviceName {
			reject([]string{deviceId}, "device "+deviceId+" has multiple name assignments: "+known+" and "+deviceName)
		} else {
			deviceToName[deviceId] = deviceName
		}

		//check for device-type redefinition
		if known, exists := deviceToDeviceType[deviceId]; exists && known != deviceTypeId {
			reject([]string{deviceId}, "device "+deviceId+" has multiple device-type-id assignments: "+known+" and "+deviceTypeId)
		} else {
			deviceToDeviceType[deviceId] = deviceTypeId
		}

		//check for device-id + service-id reuse in commands (a command topic can be used for mor than one service)
		if cmd != "" {
			if cmdIdUsed[cmdId] {
				reject([]string{deviceId}, "reused device-id/service-id: "+cmdId)
			}
			cmdIdUsed[cmdId] = true
		}

		if event != "" {
			eventTopicUsers[event] = append(eventTopicUsers[event], deviceId)
		}
		if cmd != "" {
			cmdTopicUsers[cmd] = append(cmdTopicUsers[cmd], deviceId)
		}
		if resp != "" && cmd != "" {
			respTopicUsers[resp] = append(respTopicUsers[resp], deviceId)
		}
	}

	//check for event topic reuse for other events
	for event, deviceIds := range eventTopicUsers {
		if len(deviceIds) > 1 {
			reject(deviceIds, "reused event topic: "+event)
		}
	}

	//check for response topic reuse for commands
	for resp, respDeviceIds := range respTopicUsers {
		if cmdDeviceIds, exists := cmdTopicUsers[resp]; exists {
			reject(append(slices.Clone(respDeviceIds), cmdDeviceIds...), "collision between command and response topic: "+resp)
		}
	}

	// the reasons are part of the error message sent to the mgw, which must be stable to be deduplicated
	for deviceId, reasons := range rejected {
		slices.Sort(reasons)
		rejected[deviceId] = slices.Compact(reasons)
	}

	valid = util.ListFilter(topics, func(topic TopicDescription) bool {
		_, isRejected := rejected[topic.GetLocalDeviceId()]
		return !isRejected
	})

	//WARN if event and response topic collide (it's but warning would be nice)
	eventTopicUsed := map[string]bool{}
	respTopicUsed := map[string]bool{}
	for _, topic := range valid {
		if event := topic.GetEventTopic(); event != "" {
			eventTopicUsed[event] = true
			if respTopicUsed[event] {
				this.config.GetLogger().Warn("event topic is also used as response topic", "topic", event)
			}
		}
		if resp := topic.GetResponseTopic(); resp != "" && topic.GetCmdTopic() != "" {
			respTopicUsed[resp] = true
			if eventTopicUsed[resp] {
				this.config.GetLogger().Warn("response topic is also used as event topic", "topic", resp)
			}
		}
	}
	return valid, rejected
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
