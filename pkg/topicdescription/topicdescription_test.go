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

package topicdescription

import (
	"encoding/json"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/topicdescription/model"
	"reflect"
	"sort"
	"testing"
)

func TestLoadDir(t *testing.T) {
	result, err := LoadDir("testdata/topicdesc")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(result)
	expected := generateExpected([]string{
		"root_1_a",

		"root_2_a",

		"root_3_aa",
		"root_3_ab",
		"root_3_ac",
		"root_3_ad",

		"root_4_aa",
		"root_4_ab",
		"root_4_ac",
		"root_4_ad",

		"root_5_a",

		"root_6_a",

		"sub_1_a",

		"sub_2_a",

		"sub_3_aa",
		"sub_3_ab",
		"sub_3_ac",
		"sub_3_ad",

		"sub_4_aa",
		"sub_4_ab",
		"sub_4_ac",
		"sub_4_ad",

		"sub_5_a",

		"sub_6_a",

		"sub2_1_a",

		"sub2_2_a",

		"sub2_3_aa",
		"sub2_3_ab",
		"sub2_3_ac",
		"sub2_3_ad",

		"sub2_4_aa",
		"sub2_4_ab",
		"sub2_4_ac",
		"sub2_4_ad",

		"sub2_5_a",

		"sub2_6_a",
	}, true)

	expected = append(expected, generateExpected([]string{
		"root_1_b",

		"root_2_b",

		"root_3_ba",
		"root_3_bb",
		"root_3_bc",
		"root_3_bd",
		"root_3_ca",
		"root_3_cb",
		"root_3_cc",
		"root_3_cd",

		"root_4_ba",
		"root_4_bb",
		"root_4_bc",
		"root_4_bd",
		"root_4_ca",
		"root_4_cb",
		"root_4_cc",
		"root_4_cd",

		"root_5_b",

		"root_6_b",

		"sub_1_b",

		"sub_2_b",

		"sub_3_ba",
		"sub_3_bb",
		"sub_3_bc",
		"sub_3_bd",
		"sub_3_ca",
		"sub_3_cb",
		"sub_3_cc",
		"sub_3_cd",

		"sub_4_ba",
		"sub_4_bb",
		"sub_4_bc",
		"sub_4_bd",
		"sub_4_ca",
		"sub_4_cb",
		"sub_4_cc",
		"sub_4_cd",

		"sub_5_b",

		"sub_6_b",

		"sub2_1_b",

		"sub2_2_b",

		"sub2_3_ba",
		"sub2_3_bb",
		"sub2_3_bc",
		"sub2_3_bd",
		"sub2_3_ca",
		"sub2_3_cb",
		"sub2_3_cc",
		"sub2_3_cd",

		"sub2_4_ba",
		"sub2_4_bb",
		"sub2_4_bc",
		"sub2_4_bd",
		"sub2_4_ca",
		"sub2_4_cb",
		"sub2_4_cc",
		"sub2_4_cd",

		"sub2_5_b",

		"sub2_6_b",
	}, false)...)

	sort.Slice(expected, func(i, j int) bool {
		return expected[i].GetTopic() < expected[j].GetTopic()
	})

	sort.Slice(result, func(i, j int) bool {
		return result[i].GetTopic() < result[j].GetTopic()
	})

	if !reflect.DeepEqual(result, expected) {
		rj, _ := json.Marshal(result)
		ej, _ := json.Marshal(expected)
		t.Error(string(rj), "\n", string(ej))
	}
}

func generateExpected(topics []string, withResp bool) (result []model.TopicDescription) {
	for _, topic := range topics {
		temp := model.TopicDescription{
			DeviceTypeId:   "dtid",
			DeviceLocalId:  "dlid",
			ServiceLocalId: "slid",
			DeviceName:     "name",
		}
		if withResp {
			temp.CmdTopic = topic
			temp.RespTopic = "resp"
		} else {
			temp.EventTopic = topic
		}
		result = append(result, temp)
	}
	return result
}
