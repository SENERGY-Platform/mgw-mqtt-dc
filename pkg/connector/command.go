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

		this.storeCorrelationId(cmdId, command.CommandId)

		err := this.mqtt.Publish(desc.GetCmdTopic(), 2, false, []byte(command.Data))
		if err != nil {
			log.Println("ERROR: unable to send event to mgw", err)
		}
	}()
}

func (this *Connector) storeCorrelationId(key string, correlationId string) {
	this.correlationStore.Do(func(m *map[string][]string) {
		l := (*m)[key]
		l = append(l, correlationId)
		(*m)[key] = l
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
