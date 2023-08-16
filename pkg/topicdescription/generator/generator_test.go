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
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/topicdescription/model"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
	"github.com/SENERGY-Platform/models/go/models"
	"reflect"
	"testing"
)

func TestGenerateTopicDescriptionsWithTruncate(t *testing.T) {
	prefix := "foobarbatz:"
	devices := []models.Device{
		{
			LocalId:      prefix + "d1",
			Name:         "device 1",
			DeviceTypeId: "dt1",
		},
		{
			LocalId:      prefix + "d2",
			Name:         "device 2",
			DeviceTypeId: "dt1",
		},
		{
			LocalId:      prefix + "d3",
			Name:         "device 3",
			DeviceTypeId: "dt2",
		},
		//dt doesnt use searched attributes
		{
			LocalId:      prefix + "d4",
			Name:         "device 4",
			DeviceTypeId: "dt3",
		},
		//dt is unknown
		{
			LocalId:      prefix + "d5",
			Name:         "device 5",
			DeviceTypeId: "dt4",
		},
	}

	deviceTypes := []models.DeviceType{
		{
			Id: "dt1",
			Services: []models.Service{
				{
					LocalId: "s1",
					Attributes: []models.Attribute{
						{Key: EventAttribute, Value: "{{.Device}}/e1"},
					},
				},
				{
					LocalId: "s2",
					Attributes: []models.Attribute{
						{Key: CommandAttribute, Value: "{{.Device}}/c1"},
					},
				},
				{
					LocalId: "s3",
					Attributes: []models.Attribute{
						{Key: CommandAttribute, Value: "{{.Device}}/c2"},
						{Key: ResponseAttribute, Value: "{{.Device}}/r2"},
					},
				},
				{
					LocalId: "s4",
					Attributes: []models.Attribute{
						{Key: EventAttribute, Value: "{{.Device}}/e3"},
						{Key: CommandAttribute, Value: "{{.Device}}/c3"},
						{Key: ResponseAttribute, Value: "{{.Device}}/r3"},
					},
				},
			},
		},
		{
			Id: "dt2",
			Services: []models.Service{
				{
					LocalId: "s1",
					Attributes: []models.Attribute{
						{Key: EventAttribute, Value: "{{.LocalDeviceId}}/{{.LocalServiceId}}"},
					},
				},
			},
		},
		{
			Id: "dt3",
			Services: []models.Service{
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
			DeviceLocalId:  prefix + "d1",
			ServiceLocalId: "s1",
			DeviceName:     "device 1",
		},
		{
			EventTopic:     "d2/e1",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  prefix + "d2",
			ServiceLocalId: "s1",
			DeviceName:     "device 2",
		},

		{
			CmdTopic:       "d1/c1",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  prefix + "d1",
			ServiceLocalId: "s2",
			DeviceName:     "device 1",
		},
		{
			CmdTopic:       "d2/c1",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  prefix + "d2",
			ServiceLocalId: "s2",
			DeviceName:     "device 2",
		},
		{
			CmdTopic:       "d1/c2",
			RespTopic:      "d1/r2",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  prefix + "d1",
			ServiceLocalId: "s3",
			DeviceName:     "device 1",
		},
		{
			CmdTopic:       "d2/c2",
			RespTopic:      "d2/r2",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  prefix + "d2",
			ServiceLocalId: "s3",
			DeviceName:     "device 2",
		},

		{
			EventTopic:     "d1/e3",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  prefix + "d1",
			ServiceLocalId: "s4",
			DeviceName:     "device 1",
		},
		{
			EventTopic:     "d2/e3",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  prefix + "d2",
			ServiceLocalId: "s4",
			DeviceName:     "device 2",
		},
		{
			CmdTopic:       "d1/c3",
			RespTopic:      "d1/r3",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  prefix + "d1",
			ServiceLocalId: "s4",
			DeviceName:     "device 1",
		},
		{
			CmdTopic:       "d2/c3",
			RespTopic:      "d2/r3",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  prefix + "d2",
			ServiceLocalId: "s4",
			DeviceName:     "device 2",
		},

		{
			EventTopic:     "d3/s1",
			DeviceTypeId:   "dt2",
			DeviceLocalId:  prefix + "d3",
			ServiceLocalId: "s1",
			DeviceName:     "device 3",
		},
	}
	util.ListSort(expected, func(a model.TopicDescription, b model.TopicDescription) bool {
		return a.GetTopic() < b.GetTopic()
	})

	actual := GenerateTopicDescriptions(devices, deviceTypes, prefix)

	if !reflect.DeepEqual(expected, actual) {
		e, _ := json.Marshal(expected)
		a, _ := json.Marshal(actual)
		t.Error("\n", string(e), "\n", string(a))
	}
}

func TestGenerateTopicDescriptions(t *testing.T) {
	devices := []models.Device{
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

	deviceTypes := []models.DeviceType{
		{
			Id: "dt1",
			Services: []models.Service{
				{
					LocalId: "s1",
					Attributes: []models.Attribute{
						{Key: EventAttribute, Value: "{{.Device}}/e1"},
					},
				},
				{
					LocalId: "s2",
					Attributes: []models.Attribute{
						{Key: CommandAttribute, Value: "{{.Device}}/c1"},
					},
				},
				{
					LocalId: "s3",
					Attributes: []models.Attribute{
						{Key: CommandAttribute, Value: "{{.Device}}/c2"},
						{Key: ResponseAttribute, Value: "{{.Device}}/r2"},
					},
				},
				{
					LocalId: "s4",
					Attributes: []models.Attribute{
						{Key: EventAttribute, Value: "{{.Device}}/e3"},
						{Key: CommandAttribute, Value: "{{.Device}}/c3"},
						{Key: ResponseAttribute, Value: "{{.Device}}/r3"},
					},
				},
			},
		},
		{
			Id: "dt2",
			Services: []models.Service{
				{
					LocalId: "s1",
					Attributes: []models.Attribute{
						{Key: EventAttribute, Value: "{{.LocalDeviceId}}/{{.LocalServiceId}}"},
					},
				},
			},
		},
		{
			Id: "dt3",
			Services: []models.Service{
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

	actual := GenerateTopicDescriptions(devices, deviceTypes, "")

	if !reflect.DeepEqual(expected, actual) {
		e, _ := json.Marshal(expected)
		a, _ := json.Marshal(actual)
		t.Error("\n", string(e), "\n", string(a))
	}
}

func TestGenerateTopicDescriptionsWithAttr(t *testing.T) {
	devices := []models.Device{
		{
			LocalId:      "d1",
			Name:         "device 1",
			DeviceTypeId: "dt1",
			Attributes: []models.Attribute{
				{Key: "foo", Value: "bar"},
				{Key: "something", Value: "else"},
				{Key: "sepl/batz", Value: "ignore"},
			},
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

	deviceTypes := []models.DeviceType{
		{
			Id: "dt1",
			Services: []models.Service{
				{
					LocalId: "s1",
					Attributes: []models.Attribute{
						{Key: EventAttribute, Value: "{{.foo}}{{.Device}}/e1"},
					},
				},
				{
					LocalId: "s2",
					Attributes: []models.Attribute{
						{Key: CommandAttribute, Value: "{{.foo}}{{.Device}}/c1"},
					},
				},
				{
					LocalId: "s3",
					Attributes: []models.Attribute{
						{Key: CommandAttribute, Value: "{{.foo}}{{.Device}}/c2"},
						{Key: ResponseAttribute, Value: "{{.foo}}{{.Device}}/r2"},
					},
				},
				{
					LocalId: "s4",
					Attributes: []models.Attribute{
						{Key: EventAttribute, Value: "{{.foo}}{{.Device}}/e3"},
						{Key: CommandAttribute, Value: "{{.foo}}{{.Device}}/c3"},
						{Key: ResponseAttribute, Value: "{{.foo}}{{.Device}}/r3"},
					},
				},
			},
		},
		{
			Id: "dt2",
			Services: []models.Service{
				{
					LocalId: "s1",
					Attributes: []models.Attribute{
						{Key: EventAttribute, Value: "{{.LocalDeviceId}}/{{.LocalServiceId}}"},
					},
				},
			},
		},
		{
			Id: "dt3",
			Services: []models.Service{
				{
					LocalId: "s1",
				},
			},
		},
	}

	expected := []model.TopicDescription{
		{
			EventTopic:     "bard1/e1",
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
			CmdTopic:       "bard1/c1",
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
			CmdTopic:       "bard1/c2",
			RespTopic:      "bard1/r2",
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
			EventTopic:     "bard1/e3",
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
			CmdTopic:       "bard1/c3",
			RespTopic:      "bard1/r3",
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

	actual := GenerateTopicDescriptions(devices, deviceTypes, "")

	if !reflect.DeepEqual(expected, actual) {
		e, _ := json.Marshal(expected)
		a, _ := json.Marshal(actual)
		t.Error("\n", string(e), "\n", string(a))
	}
}

func TestGeneratorDuplicate(t *testing.T) {
	devices := []models.Device{
		{
			LocalId:      "d1",
			Name:         "device 1",
			DeviceTypeId: "dt1",
		},
	}

	deviceTypes := []models.DeviceType{
		{
			Id: "dt1",
			Services: []models.Service{
				{
					Name:    "setOn",
					LocalId: "power",
					Attributes: []models.Attribute{
						{Key: CommandAttribute, Value: "{{.Device}}/c2"},
						{Key: ResponseAttribute, Value: "{{.Device}}/r2"},
					},
				},
				{
					Name:    "setOff",
					LocalId: "power",
					Attributes: []models.Attribute{
						{Key: CommandAttribute, Value: "{{.Device}}/c2"},
						{Key: ResponseAttribute, Value: "{{.Device}}/r2"},
					},
				},
			},
		},
	}

	expected := []model.TopicDescription{
		{
			CmdTopic:       "d1/c2",
			RespTopic:      "d1/r2",
			DeviceTypeId:   "dt1",
			DeviceLocalId:  "d1",
			ServiceLocalId: "power",
			DeviceName:     "device 1",
		},
	}
	util.ListSort(expected, func(a model.TopicDescription, b model.TopicDescription) bool {
		return a.GetTopic() < b.GetTopic()
	})

	actual := GenerateTopicDescriptions(devices, deviceTypes, "")

	if !reflect.DeepEqual(expected, actual) {
		e, _ := json.Marshal(expected)
		a, _ := json.Marshal(actual)
		t.Error("\n", string(e), "\n", string(a))
	}
}
