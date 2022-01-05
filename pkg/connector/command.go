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
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/mgw"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
	"log"
)

func (this *Connector) CommandHandler(deviceId string, serviceId string, command mgw.Command) {
	go func() {
		cmdId := getCommandId(deviceId, serviceId)
		desc, ok := this.commandTopicRegister.Get(cmdId)
		if !ok {
			log.Println("WARNING: got command for unknown device description", cmdId)
			return
		}

		expectsDeviceResponse := desc.GetResponseTopic() != ""
		if expectsDeviceResponse {
			this.storeCorrelationId(cmdId, command.CommandId)
		}

		err := this.commandMqttClient.Publish(desc.GetCmdTopic(), 2, false, []byte(command.Data))
		if err != nil {
			log.Println("ERROR: unable to send command to mgw", err)
			this.removeCorrelationId(cmdId, command.CommandId)
		}

		if !expectsDeviceResponse {
			err = this.mgwClient.Respond(deviceId, serviceId, mgw.Command{
				CommandId: command.CommandId,
				Data:      "",
			})
			if err != nil {
				log.Println("ERROR: unable to send empty response", err)
			}
		}
	}()
}

func (this *Connector) storeCorrelationId(key string, correlationId string) {
	//TODO remove correlation ids to old to be used
	this.correlationStore.Update(key, func(l []string) []string {
		return append(l, correlationId)
	})
}

func (this *Connector) removeCorrelationId(key string, correlationId string) {
	this.correlationStore.Update(key, func(l []string) []string {
		return util.ListFilter(l, func(value string) bool {
			return value != correlationId
		})
	})
}

func (this *Connector) popCorrelationId(key string) (correlationId string, exists bool) {
	this.correlationStore.Do(func(m *map[string][]string) {
		l, ok := (*m)[key]
		if !ok {
			exists = false
			return
		}
		if len(l) > 0 {
			exists = true
			correlationId = l[0]
			l = l[1:]
			(*m)[key] = l
		} else {
			exists = false
		}
	})
	return
}
