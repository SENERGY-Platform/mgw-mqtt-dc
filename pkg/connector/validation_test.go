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
	"slices"
	"sync"
	"testing"

	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/configuration"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/devicerepo"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/topicdescription/model"
)

type recordingMgwMock struct {
	MgwMock
	mux          sync.Mutex
	devices      []string
	removed      []string
	deviceErrors map[string]string
}

func (this *recordingMgwMock) SetDevice(deviceId string, name string, deviceTypeid string, state string) error {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.devices = append(this.devices, deviceId)
	return nil
}

func (this *recordingMgwMock) RemoveDevice(deviceId string) error {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.removed = append(this.removed, deviceId)
	return nil
}

func (this *recordingMgwMock) SendDeviceError(localDeviceId string, message string) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.deviceErrors[localDeviceId] = message
}

func TestUpdateTopicsRejectsInvalidDevice(t *testing.T) {
	// the topic templates {{.CmdPrefix}}{{.Device}}/{{.Service}} and {{.RespPrefix}}{{.Device}}/{{.Service}}
	// result in equal command and response topics, if the device has no prefix attributes
	invalid := []model.TopicDescription{
		{CmdTopic: "invalid/POWER1", RespTopic: "invalid/POWER1", DeviceTypeId: "dt", DeviceLocalId: "invalid", ServiceLocalId: "POWER1", DeviceName: "invalid"},
		{EventTopic: "tele/invalid/SENSOR", DeviceTypeId: "dt", DeviceLocalId: "invalid", ServiceLocalId: "SENSOR", DeviceName: "invalid"},
	}
	valid := []model.TopicDescription{
		{CmdTopic: "cmnd/valid/POWER1", RespTopic: "stat/valid/POWER1", DeviceTypeId: "dt", DeviceLocalId: "valid", ServiceLocalId: "POWER1", DeviceName: "valid"},
		{EventTopic: "tele/valid/SENSOR", DeviceTypeId: "dt", DeviceLocalId: "valid", ServiceLocalId: "SENSOR", DeviceName: "valid"},
	}
	topics := append(slices.Clone(valid), invalid...)

	mgwMock := &recordingMgwMock{deviceErrors: map[string]string{}}
	conn, err := NewWithFactories(context.Background(), configuration.Config{DeleteDevices: true}, NewTopicDescriptionProvider(func(config configuration.Config, repo *devicerepo.DeviceRepo) ([]model.TopicDescription, error) {
		return topics, nil
	}), func(ctx context.Context, config configuration.Config, refreshNotifier func()) (MgwClient, error) {
		return mgwMock, nil
	}, NewMqttFactory(newMqttMock))
	if err != nil {
		t.Fatal(err)
	}

	err = conn.updateTopics()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(mgwMock.devices, []string{"valid"}) {
		t.Errorf("expected only the valid device to be registered, got %v", mgwMock.devices)
	}
	if _, ok := conn.eventTopicRegister.Get("tele/valid/SENSOR"); !ok {
		t.Error("expected event of the valid device to be registered")
	}
	if _, ok := conn.eventTopicRegister.Get("tele/invalid/SENSOR"); ok {
		t.Error("expected no event of the invalid device to be registered")
	}
	if _, ok := conn.responseTopicRegister.Get("stat/valid/POWER1"); !ok {
		t.Error("expected response of the valid device to be registered")
	}
	if _, ok := conn.commandTopicRegister.Get(getCommandId("valid", "POWER1")); !ok {
		t.Error("expected command of the valid device to be registered")
	}
	if _, ok := mgwMock.deviceErrors["invalid"]; !ok || len(mgwMock.deviceErrors) != 1 {
		t.Errorf("expected exactly one device error for the invalid device, got %v", mgwMock.deviceErrors)
	}

	// a previously valid device that becomes invalid is unregistered but not removed from the mgw
	topics = append(slices.Clone(valid[:1]), model.TopicDescription{EventTopic: "valid/SENSOR", DeviceTypeId: "dt", DeviceLocalId: "valid", ServiceLocalId: "SENSOR", DeviceName: "other name"})
	err = conn.updateTopics()
	if err != nil {
		t.Fatal(err)
	}
	if len(mgwMock.removed) != 0 {
		t.Errorf("expected no removed devices, got %v", mgwMock.removed)
	}
	if len(conn.eventTopicRegister.GetAll()) != 0 || len(conn.responseTopicRegister.GetAll()) != 0 || len(conn.commandTopicRegister.GetAll()) != 0 {
		t.Error("expected no registered topics")
	}
	if _, ok := mgwMock.deviceErrors["valid"]; !ok {
		t.Errorf("expected device error for the now invalid device, got %v", mgwMock.deviceErrors)
	}
}

func TestValidateTopicDescriptionsRejectsAllDevicesOfConflict(t *testing.T) {
	conn := &Connector{}
	valid, rejected := conn.validateTopicDescriptions([]TopicDescription{
		model.TopicDescription{EventTopic: "shared/SENSOR", DeviceTypeId: "dt", DeviceLocalId: "a", ServiceLocalId: "SENSOR", DeviceName: "a"},
		model.TopicDescription{EventTopic: "a/STATE", DeviceTypeId: "dt", DeviceLocalId: "a", ServiceLocalId: "STATE", DeviceName: "a"},
		model.TopicDescription{EventTopic: "shared/SENSOR", DeviceTypeId: "dt", DeviceLocalId: "b", ServiceLocalId: "SENSOR", DeviceName: "b"},
		model.TopicDescription{CmdTopic: "b/POWER", DeviceTypeId: "dt", DeviceLocalId: "b", ServiceLocalId: "POWER", DeviceName: "b"},
		model.TopicDescription{CmdTopic: "c/POWER", RespTopic: "d/POWER", DeviceTypeId: "dt", DeviceLocalId: "c", ServiceLocalId: "POWER", DeviceName: "c"},
		model.TopicDescription{CmdTopic: "d/POWER", DeviceTypeId: "dt", DeviceLocalId: "d", ServiceLocalId: "POWER", DeviceName: "d"},
		model.TopicDescription{EventTopic: "e/SENSOR", DeviceTypeId: "dt", DeviceLocalId: "e", ServiceLocalId: "SENSOR", DeviceName: "e"},
	})
	if len(valid) != 1 || valid[0].GetLocalDeviceId() != "e" {
		t.Errorf("expected only device e to be valid, got %v", valid)
	}
	for _, id := range []string{"a", "b", "c", "d"} {
		if _, ok := rejected[id]; !ok {
			t.Errorf("expected device %v to be rejected, got %v", id, rejected)
		}
	}
}

func TestValidateTopicDescriptionsStableReasons(t *testing.T) {
	conn := &Connector{}
	topics := []TopicDescription{
		model.TopicDescription{EventTopic: "shared/1", DeviceTypeId: "dt", DeviceLocalId: "a", ServiceLocalId: "s1", DeviceName: "a"},
		model.TopicDescription{EventTopic: "shared/2", DeviceTypeId: "dt", DeviceLocalId: "a", ServiceLocalId: "s2", DeviceName: "a"},
		model.TopicDescription{EventTopic: "shared/3", DeviceTypeId: "dt", DeviceLocalId: "a", ServiceLocalId: "s3", DeviceName: "a"},
		model.TopicDescription{CmdTopic: "a/POWER", RespTopic: "a/POWER", DeviceTypeId: "dt", DeviceLocalId: "a", ServiceLocalId: "POWER", DeviceName: "a"},
		model.TopicDescription{EventTopic: "shared/1", DeviceTypeId: "dt", DeviceLocalId: "b", ServiceLocalId: "s1", DeviceName: "b"},
		model.TopicDescription{EventTopic: "shared/2", DeviceTypeId: "dt", DeviceLocalId: "b", ServiceLocalId: "s2", DeviceName: "b"},
		model.TopicDescription{EventTopic: "shared/3", DeviceTypeId: "dt", DeviceLocalId: "b", ServiceLocalId: "s3", DeviceName: "b"},
	}
	_, first := conn.validateTopicDescriptions(topics)
	if len(first["a"]) != 4 || !slices.IsSorted(first["a"]) {
		t.Fatalf("expected 4 sorted reasons for device a, got %v", first["a"])
	}
	for range 100 {
		_, rejected := conn.validateTopicDescriptions(topics)
		for deviceId, reasons := range rejected {
			if !slices.Equal(reasons, first[deviceId]) {
				t.Fatalf("unstable reasons for device %v: %v != %v", deviceId, reasons, first[deviceId])
			}
		}
	}
}
