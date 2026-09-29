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

package onlinechecker

import (
	"errors"
	"testing"

	"github.com/SENERGY-Platform/models/go/models"
)

// device types from the device-repository v2 may carry the aspects of a content variable
// only as list (aspect_ids), without the deprecated single aspect_id
func TestMarshallerAspectIdList(t *testing.T) {
	m, err := NewMarshaller(DeviceRepoMock{
		GetCharacteristicF: func(id string) (models.Characteristic, error) {
			if id != "devicevalue" && id != "requestvalue" {
				return models.Characteristic{}, errors.New("not found")
			}
			return models.Characteristic{Id: id, Name: id, Type: models.Integer}, nil
		},
		GetConceptF: func(id string) (models.Concept, error) {
			if id != "testconcept" {
				return models.Concept{}, errors.New("not found")
			}
			return models.Concept{
				Id:                   "testconcept",
				Name:                 "testconcept",
				CharacteristicIds:    []string{"devicevalue", "requestvalue"},
				BaseCharacteristicId: "requestvalue",
				Conversions: []models.ConverterExtension{
					{From: "requestvalue", To: "devicevalue", Formula: "x - 10", PlaceholderName: "x"},
					{From: "devicevalue", To: "requestvalue", Formula: "x + 10", PlaceholderName: "x"},
				},
			}, nil
		},
		GetConceptIdOfFunctionF: func(id string) string {
			return map[string]string{"fid": "testconcept"}[id]
		},
		GetAspectNodeF: func(id string) (models.AspectNode, error) {
			return models.AspectNode{}, errors.New("not found")
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	service := func(value models.ContentVariable) models.Service {
		value.Id = "value"
		value.Name = "value"
		value.Type = models.Integer
		value.CharacteristicId = "devicevalue"
		value.FunctionId = "fid"
		return models.Service{
			Id:          "sid",
			LocalId:     "lsid",
			Interaction: models.EVENT,
			Outputs: []models.Content{{
				Id: "output",
				ContentVariable: models.ContentVariable{
					Id:                  "outputcontentid",
					Name:                "outputcontent",
					Type:                models.Structure,
					SubContentVariables: []models.ContentVariable{value, {Id: "time", Name: "time", Type: models.Integer}},
				},
				Serialization:     models.JSON,
				ProtocolSegmentId: "output",
			}},
		}
	}
	message := map[string]interface{}{"outputcontent": map[string]interface{}{"time": 3333, "value": 42}}

	cases := map[string]models.ContentVariable{
		"list only":             {AspectIds: []string{"aid", "other_aid"}},
		"list and single alias": {AspectIds: []string{"aid", "other_aid"}, AspectId: "aid"},
		"no aspect":             {},
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			result, err := m.Unmarshal(service(value), "fid", "requestvalue", message)
			if err != nil {
				t.Fatal(err)
			}
			if result != float64(52) {
				t.Errorf("unexpected result: %#v", result)
			}
		})
	}
}
