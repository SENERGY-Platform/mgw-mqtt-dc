/*
 * Copyright 2026 InfAI (CC SES)
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
	"context"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/configuration"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/devicerepo"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/integrationtests/mocks"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/mgw"
)

func TestActivatedDevicesSurviveReload(t *testing.T) {
	location := filepath.Join(t.TempDir(), "activated.json")
	devices, err := LoadActivatedDevices(location)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"b", "a", "c"} {
		if err = devices.Add(id); err != nil {
			t.Fatal(err)
		}
	}
	if err = devices.Retain(func(id string) bool { return id != "c" }); err != nil {
		t.Fatal(err)
	}

	content, err := os.ReadFile(location)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != `["a","b"]` {
		t.Errorf("unexpected file content %s", content)
	}

	reloaded, err := LoadActivatedDevices(location)
	if err != nil {
		t.Fatal(err)
	}
	if !reloaded.Contains("a") || !reloaded.Contains("b") || reloaded.Contains("c") {
		t.Errorf("unexpected reloaded ids %v", reloaded.ids)
	}
}

func TestActivatedDevicesStartEmptyWithoutFile(t *testing.T) {
	devices, err := LoadActivatedDevices(filepath.Join(t.TempDir(), "missing.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(devices.ids) != 0 {
		t.Errorf("expected no ids, got %v", devices.ids)
	}
}

func TestActivatedDevicesRejectInvalidFile(t *testing.T) {
	location := filepath.Join(t.TempDir(), "activated.json")
	if err := os.WriteFile(location, []byte("not json"), 0666); err != nil {
		t.Fatal(err)
	}
	_, err := LoadActivatedDevices(location)
	if err == nil {
		t.Error("expected error for invalid file")
	}
}

var eventDesc = mocks.TopicDesc{DeviceName: "d1", DeviceType: "dt", DeviceId: "d1", ServiceId: "s1", EventTopic: "d1/event"}
var cmdDesc = mocks.TopicDesc{DeviceName: "d2", DeviceType: "dt", DeviceId: "d2", ServiceId: "s1", CmdTopic: "d2/cmd"}

func TestDeviceWithEventsIsRegisteredOnlyAfterFirstEvent(t *testing.T) {
	env := newActivationEnv(t, true, filepath.Join(t.TempDir(), "activated.json"), eventDesc)
	env.update()

	if calls := env.mgw.get(); len(calls) != 0 {
		t.Fatalf("expected no mgw calls before the first event, got %v", calls)
	}

	env.mqtt.receive(t, "d1/event", "42")
	env.mgw.waitFor(t, "SendEvent d1 s1 42")

	expected := []string{"SetDevice d1 d1 dt online", "ListenToDeviceCommands d1", "SendEvent d1 s1 42"}
	if calls := env.mgw.get(); !reflect.DeepEqual(calls, expected) {
		t.Errorf("unexpected mgw calls\n%v\n%v", calls, expected)
	}
}

func TestDeviceWithoutEventsIsRegisteredImmediately(t *testing.T) {
	env := newActivationEnv(t, true, "", cmdDesc)
	env.update()

	expected := []string{"SetDevice d2 d2 dt online", "ListenToDeviceCommands d2"}
	if calls := env.mgw.get(); !reflect.DeepEqual(calls, expected) {
		t.Errorf("unexpected mgw calls\n%v\n%v", calls, expected)
	}
}

func TestActivatedDeviceIsRegisteredAfterRestart(t *testing.T) {
	location := filepath.Join(t.TempDir(), "activated.json")
	first := newActivationEnv(t, true, location, eventDesc)
	first.update()
	first.mqtt.receive(t, "d1/event", "42")
	first.mgw.waitFor(t, "SendEvent d1 s1 42")

	restarted := newActivationEnv(t, true, location, eventDesc)
	restarted.update()

	expected := []string{"SetDevice d1 d1 dt online", "ListenToDeviceCommands d1"}
	if calls := restarted.mgw.get(); !reflect.DeepEqual(calls, expected) {
		t.Errorf("unexpected mgw calls\n%v\n%v", calls, expected)
	}
}

func TestRemovedDeviceIsDeletedOnlyIfActivated(t *testing.T) {
	env := newActivationEnv(t, true, "", eventDesc)
	env.update()
	env.setDescriptions()
	env.update()

	if calls := env.mgw.get(); len(calls) != 0 {
		t.Fatalf("expected no mgw calls for a never activated device, got %v", calls)
	}

	env.setDescriptions(eventDesc)
	env.update()
	env.mqtt.receive(t, "d1/event", "42")
	env.mgw.waitFor(t, "SendEvent d1 s1 42")
	env.setDescriptions()
	env.update()

	calls := env.mgw.get()
	if !slices.Contains(calls, "RemoveDevice d1") || !slices.Contains(calls, "StopListenToDeviceCommands d1") {
		t.Errorf("expected removal of the activated device, got %v", calls)
	}
	if env.connector.activatedDevices.Contains("d1") {
		t.Error("removed device is still activated")
	}
}

func TestDevicesAreRegisteredImmediatelyWithoutActivationMode(t *testing.T) {
	env := newActivationEnv(t, false, "", eventDesc)
	env.update()

	expected := []string{"SetDevice d1 d1 dt online", "ListenToDeviceCommands d1"}
	if calls := env.mgw.get(); !reflect.DeepEqual(calls, expected) {
		t.Errorf("unexpected mgw calls\n%v\n%v", calls, expected)
	}
}

type activationEnv struct {
	connector *Connector
	mgw       *recordingMgw
	mqtt      *recordingMqtt
	mux       sync.Mutex
	descs     []mocks.TopicDesc
	t         *testing.T
}

func newActivationEnv(t *testing.T, activateOnEvent bool, location string, descs ...mocks.TopicDesc) *activationEnv {
	env := &activationEnv{descs: descs, t: t, mgw: &recordingMgw{}, mqtt: &recordingMqtt{handlers: map[string]func(string, bool, []byte){}}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var err error
	env.connector, err = NewWithFactories(ctx, configuration.Config{
		DeleteDevices:          true,
		ActivateDevicesOnEvent: activateOnEvent,
		ActivatedDevicesFile:   location,
	}, NewTopicDescriptionProvider(func(config configuration.Config, repo *devicerepo.DeviceRepo) ([]mocks.TopicDesc, error) {
		env.mux.Lock()
		defer env.mux.Unlock()
		return slices.Clone(env.descs), nil
	}), func(ctx context.Context, config configuration.Config, refreshNotifier func()) (MgwClient, error) {
		return env.mgw, nil
	}, func(ctx context.Context, brokerUrl string, clientId string, username string, password string, insecureSkipVerify bool) (MqttClient, error) {
		return env.mqtt, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func (this *activationEnv) setDescriptions(descs ...mocks.TopicDesc) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.descs = descs
}

// update runs a topic update and discards the mgw calls of earlier steps
func (this *activationEnv) update() {
	this.mgw.reset()
	if err := this.connector.updateTopics(); err != nil {
		this.t.Fatal(err)
	}
}

type recordingMgw struct {
	mux   sync.Mutex
	calls []string
}

func (this *recordingMgw) record(call string) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.calls = append(this.calls, call)
}

func (this *recordingMgw) get() []string {
	this.mux.Lock()
	defer this.mux.Unlock()
	return slices.Clone(this.calls)
}

func (this *recordingMgw) reset() {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.calls = nil
}

func (this *recordingMgw) waitFor(t *testing.T, call string) {
	deadline := time.Now().Add(2 * time.Second)
	for !slices.Contains(this.get(), call) {
		if time.Now().After(deadline) {
			t.Fatalf("missing mgw call %q, got %v", call, this.get())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (this *recordingMgw) ListenToDeviceCommands(deviceId string, commandHandler mgw.DeviceCommandHandler) error {
	this.record("ListenToDeviceCommands " + deviceId)
	return nil
}

func (this *recordingMgw) StopListenToDeviceCommands(deviceId string) error {
	this.record("StopListenToDeviceCommands " + deviceId)
	return nil
}

func (this *recordingMgw) SetDevice(deviceId string, name string, deviceTypeid string, state string) error {
	this.record("SetDevice " + deviceId + " " + name + " " + deviceTypeid + " " + state)
	return nil
}

func (this *recordingMgw) RemoveDevice(deviceId string) error {
	this.record("RemoveDevice " + deviceId)
	return nil
}

func (this *recordingMgw) SendEvent(deviceId string, serviceId string, value []byte) error {
	this.record("SendEvent " + deviceId + " " + serviceId + " " + string(value))
	return nil
}

func (this *recordingMgw) Respond(deviceId string, serviceId string, response mgw.Command) error {
	return nil
}

func (this *recordingMgw) SendClientError(message string) {
	this.record("SendClientError " + message)
}

func (this *recordingMgw) SendDeviceError(localDeviceId string, message string) {
	this.record("SendDeviceError " + localDeviceId + " " + message)
}

func (this *recordingMgw) SendCommandError(correlationId string, message string) {
	this.record("SendCommandError " + message)
}

type recordingMqtt struct {
	mux      sync.Mutex
	handlers map[string]func(topic string, retained bool, payload []byte)
}

func (this *recordingMqtt) Subscribe(topic string, qos byte, handler func(topic string, retained bool, payload []byte)) error {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.handlers[topic] = handler
	return nil
}

func (this *recordingMqtt) Unsubscribe(topic string) error {
	this.mux.Lock()
	defer this.mux.Unlock()
	delete(this.handlers, topic)
	return nil
}

func (this *recordingMqtt) Publish(topic string, qos byte, retained bool, payload []byte) error {
	return nil
}

func (this *recordingMqtt) receive(t *testing.T, topic string, payload string) {
	this.mux.Lock()
	handler, ok := this.handlers[topic]
	this.mux.Unlock()
	if !ok {
		t.Fatalf("no subscription for %v", topic)
	}
	handler(topic, false, []byte(payload))
}
