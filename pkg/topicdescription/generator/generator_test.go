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

package generator

import (
	"encoding/json"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/topicdescription/generator/iotmodel"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/topicdescription/model"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
	"reflect"
	"testing"
)

func TestGenerateTopicDescriptions(t *testing.T) {
	devices := []iotmodel.Device{
		{
			LocalId:      "d1",
			Name:         "device 1",
			DeviceTypeId: "dt1",
		},
		{
			LocalId:      "d2",
			Name:         "device 2",
			DeviceTypeId: "dt1",
		},
		{
			LocalId:      "d3",
			Name:         "device 3",
			DeviceTypeId: "dt2",
		},
		//dt doesnt use searched attributes
		{
			LocalId:      "d4",
			Name:         "device 4",
			DeviceTypeId: "dt3",
		},
		//dt is unknown
		{
			LocalId:      "d5",
			Name:         "device 5",
			DeviceTypeId: "dt4",
		},
	}

	deviceTypes := []iotmodel.DeviceType{
		{
			Id: "dt1",
			Services: []iotmodel.Service{
				{
					LocalId: "s1",
					Attributes: []iotmodel.Attribute{
						{Key: EventAttribute, Value: "{{.Device}}/e1"},
					},
				},
				{
					LocalId: "s2",
					Attributes: []iotmodel.Attribute{
						{Key: CommandAttribute, Value: "{{.Device}}/c1"},
					},
				},
				{
					LocalId: "s3",
					Attributes: []iotmodel.Attribute{
						{Key: CommandAttribute, Value: "{{.Device}}/c2"},
						{Key: ResponseAttribute, Value: "{{.Device}}/r2"},
					},
				},
				{
					LocalId: "s4",
					Attributes: []iotmodel.Attribute{
						{Key: EventAttribute, Value: "{{.Device}}/e3"},
						{Key: CommandAttribute, Value: "{{.Device}}/c3"},
						{Key: ResponseAttribute, Value: "{{.Device}}/r3"},
					},
				},
			},
		},
		{
			Id: "dt2",
			Services: []iotmodel.Service{
				{
					LocalId: "s1",
					Attributes: []iotmodel.Attribute{
						{Key: EventAttribute, Value: "{{.LocalDeviceId}}/{{.LocalServiceId}}"},
					},
				},
			},
		},
		{
			Id: "dt3",
			Services: []iotmodel.Service{
				{
					LocalId: "s1",
				},
			},
		},
	}

	expected := []model.TopicDescription{
		{
			EventTopic:     "d1/e1",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  "d1",
			ServiceLocalId: "s1",
			DeviceName:     "device 1",
		},
		{
			EventTopic:     "d2/e1",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  "d2",
			ServiceLocalId: "s1",
			DeviceName:     "device 2",
		},

		{
			CmdTopic:       "d1/c1",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  "d1",
			ServiceLocalId: "s2",
			DeviceName:     "device 1",
		},
		{
			CmdTopic:       "d2/c1",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  "d2",
			ServiceLocalId: "s2",
			DeviceName:     "device 2",
		},
		{
			CmdTopic:       "d1/c2",
			RespTopic:      "d1/r2",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  "d1",
			ServiceLocalId: "s3",
			DeviceName:     "device 1",
		},
		{
			CmdTopic:       "d2/c2",
			RespTopic:      "d2/r2",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  "d2",
			ServiceLocalId: "s3",
			DeviceName:     "device 2",
		},

		{
			EventTopic:     "d1/e3",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  "d1",
			ServiceLocalId: "s4",
			DeviceName:     "device 1",
		},
		{
			EventTopic:     "d2/e3",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  "d2",
			ServiceLocalId: "s4",
			DeviceName:     "device 2",
		},
		{
			CmdTopic:       "d1/c3",
			RespTopic:      "d1/r3",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  "d1",
			ServiceLocalId: "s4",
			DeviceName:     "device 1",
		},
		{
			CmdTopic:       "d2/c3",
			RespTopic:      "d2/r3",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  "d2",
			ServiceLocalId: "s4",
			DeviceName:     "device 2",
		},

		{
			EventTopic:     "d3/s1",
			DeviceTypeId:   "dt2",
			DeviceLocalId:  "d3",
			ServiceLocalId: "s1",
			DeviceName:     "device 3",
		},
	}
	util.ListSort(expected, func(a model.TopicDescription, b model.TopicDescription) bool {
		return a.GetTopic() < b.GetTopic()
	})

	actual := GenerateTopicDescriptions(devices, deviceTypes)

	if !reflect.DeepEqual(expected, actual) {
		e, _ := json.Marshal(expected)
		a, _ := json.Marshal(actual)
		t.Error("\n", string(e), "\n", string(a))
	}
}