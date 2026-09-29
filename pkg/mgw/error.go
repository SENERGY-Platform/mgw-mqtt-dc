/*
 * Copyright 2022 InfAI (CC SES)
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
	"log/slog"
	"sync"
	"time"
)

func (this *Client) SendClientError(message string) {
	payload := this.connectorId + ": " + message
	this.publishError("error/client", payload, "Error on Client.SendClientError()")
}

func (this *Client) SendDeviceError(localDeviceId string, message string) {
	payload := this.connectorId + ": " + message
	this.publishError("error/device/"+localDeviceId, payload, "Error on Client.SendDeviceError()")
}

func (this *Client) SendCommandError(correlationId string, message string) {
	this.publishError("error/command/"+correlationId, message, "Error on Client.SendCommandError()")
}

func (this *Client) publishError(topic string, payload string, errorLogMsg string) {
	if !this.mqtt.IsConnected() {
		slog.Warn("mqtt client not connected")
		return
	}
	if !this.errorDeduplication.reserve(topic, payload, time.Now()) {
		slog.Debug("skip error already sent within deduplication period", "topic", topic, "payload", payload)
		return
	}
	slog.Debug("publish error", "topic", topic, "payload", payload)
	token := this.mqtt.Publish(topic, 2, false, payload)
	if token.Wait() && token.Error() != nil {
		this.errorDeduplication.release(topic, payload)
		slog.Error(errorLogMsg, "error", token.Error())
	}
}

// errorDeduplication remembers when an error message has been sent,
// to prevent the same message from being sent again within period.
// a period <= 0 disables the deduplication.
type errorDeduplication struct {
	period time.Duration
	mux    sync.Mutex
	sent   map[string]time.Time
}

func newErrorDeduplication(period time.Duration) *errorDeduplication {
	return &errorDeduplication{period: period, sent: map[string]time.Time{}}
}

// reserve returns true if the message has not been sent within the period and marks it as sent at now.
// marking before publishing prevents concurrent calls from sending the same message twice.
func (this *errorDeduplication) reserve(topic string, payload string, now time.Time) bool {
	if this == nil || this.period <= 0 {
		return true
	}
	this.mux.Lock()
	defer this.mux.Unlock()
	for key, sentAt := range this.sent {
		if now.Sub(sentAt) >= this.period {
			delete(this.sent, key)
		}
	}
	key := topic + "\n" + payload
	if _, found := this.sent[key]; found {
		return false
	}
	this.sent[key] = now
	return true
}

// release removes the mark of a message that could not be sent, so that it is not suppressed
func (this *errorDeduplication) release(topic string, payload string) {
	if this == nil || this.period <= 0 {
		return
	}
	this.mux.Lock()
	defer this.mux.Unlock()
	delete(this.sent, topic+"\n"+payload)
}
