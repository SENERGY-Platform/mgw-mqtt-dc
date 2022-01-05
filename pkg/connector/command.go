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
	"time"
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

type CorrelationId struct {
	id   string
	date time.Time
}

func (this *Connector) removeOldCorrelationIds(key string) {
	this.correlationStore.Update(key, func(l []CorrelationId) []CorrelationId {
		return util.ListFilter(l, func(value CorrelationId) bool {
			toOld := time.Since(value.date) > this.MaxCorrelationIdAge
			if toOld {
				log.Println("WARNING: drop correlation id because its older than MaxCorrelationIdAge", value.id, value.date.String())
			}
			return !toOld
		})
	})
}

func (this *Connector) storeCorrelationId(key string, correlationId string) {
	this.removeOldCorrelationIds(key)
	this.correlationStore.Update(key, func(l []CorrelationId) []CorrelationId {
		return append(l, CorrelationId{id: correlationId, date: time.Now()})
	})
}

func (this *Connector) removeCorrelationId(key string, correlationId string) {
	this.removeOldCorrelationIds(key)
	this.correlationStore.Update(key, func(l []CorrelationId) []CorrelationId {
		return util.ListFilter(l, func(value CorrelationId) bool {
			return value.id != correlationId
		})
	})
}

func (this *Connector) popCorrelationId(key string) (correlationId string, exists bool) {
	this.removeOldCorrelationIds(key)
	this.correlationStore.Do(func(m *map[string][]CorrelationId) {
		l, ok := (*m)[key]
		if !ok {
			exists = false
			return
		}
		if len(l) > 0 {
			exists = true
			correlationId = l[0].id
			l = l[1:]
			(*m)[key] = l
		} else {
			exists = false
		}
	})
	return
}
