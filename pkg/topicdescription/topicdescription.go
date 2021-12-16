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
	"encoding/csv"
	"encoding/json"
	"errors"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/configuration"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type TopicDescription struct {
	Topic          string `json:"topic" yaml:"topic"`
	RespTopic      string `json:"resp_topic" yaml:"resp_topic"`
	DeviceTypeId   string `json:"device_type_id" yaml:"device_type_id"`
	DeviceLocalId  string `json:"device_local_id" yaml:"device_local_id"`
	ServiceLocalId string `json:"service_local_id" yaml:"service_local_id"`
	DeviceName     string `json:"device_name" yaml:"device_name"`
}

func (this TopicDescription) GetTopic() string {
	return this.Topic
}

func (this TopicDescription) GetDeviceName() string {
	return this.DeviceName
}

func (this TopicDescription) GetResponseTopic() string {
	return this.RespTopic
}

func (this TopicDescription) GetDeviceTypeId() string {
	return this.DeviceTypeId
}

func (this TopicDescription) GetLocalDeviceId() string {
	return this.DeviceLocalId
}

func (this TopicDescription) GetLocalServiceId() string {
	return this.ServiceLocalId
}

func Load(config configuration.Config) (topicDescriptions []TopicDescription, err error) {
	return LoadDir(config.DeviceDescriptionsDir)
}

func LoadDir(dir string) (topicDescriptions []TopicDescription, err error) {
	topicDescriptions = []TopicDescription{}
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return topicDescriptions, err
	}
	for _, file := range files {
		p := path.Join(dir, file.Name())
		if file.IsDir() {
			temp, err := LoadDir(p)
			if err != nil {
				return topicDescriptions, err
			}
			topicDescriptions = append(topicDescriptions, temp...)
		} else {
			ext := filepath.Ext(file.Name())
			switch ext {
			case ".md":
				//ignore and do not warn
			case ".json":
				temp, err := LoadJson(p)
				if err != nil {
					log.Println("WARNING: unable to load", p, err)
					continue
				}
				topicDescriptions = append(topicDescriptions, temp...)
			case ".csv":
				temp, err := LoadCsv(p)
				if err != nil {
					log.Println("WARNING: unable to load", p, err)
					continue
				}
				topicDescriptions = append(topicDescriptions, temp...)
			case ".yml":
				fallthrough
			case ".yaml":
				temp, err := LoadYaml(p)
				if err != nil {
					log.Println("WARNING: unable to load", p, err)
					continue
				}
				topicDescriptions = append(topicDescriptions, temp...)
			default:
				log.Println("WARNING: unknown file type in topic-descriptions directory", ext, file.Name())
			}
		}
	}
	return topicDescriptions, nil
}

func LoadJson(location string) (topicDescriptions []TopicDescription, err error) {
	file, err := os.Open(location)
	if err != nil {
		log.Println("error on config load:\n", location, "\n", err)
		return topicDescriptions, err
	}
	err = json.NewDecoder(file).Decode(&topicDescriptions)
	if err != nil {
		log.Println("error on config load:\n", location, "\n", err)
		return topicDescriptions, err
	}
	return topicDescriptions, nil
}

func LoadYaml(location string) (topicDescriptions []TopicDescription, err error) {
	file, err := os.Open(location)
	if err != nil {
		log.Println("error on config load:\n", location, "\n", err)
		return topicDescriptions, err
	}
	err = yaml.NewDecoder(file).Decode(&topicDescriptions)
	if err != nil {
		log.Println("error on config load:\n", location, "\n", err)
		return topicDescriptions, err
	}
	return topicDescriptions, nil
}

func LoadCsv(location string) (topicDescriptions []TopicDescription, err error) {
	file, err := os.Open(location)
	if err != nil {
		log.Println("error on config load:\n", location, "\n", err)
		return topicDescriptions, err
	}
	reader := csv.NewReader(file)
	reader.Comment = '#'
	reader.FieldsPerRecord = -1
	reader.TrimLeadingSpace = true
	lines, err := reader.ReadAll()
	if err != nil {
		log.Println("error on config load:\n", location, "\n", err)
		return topicDescriptions, err
	}
	for _, line := range lines {
		temp := TopicDescription{
			Topic:          "",
			RespTopic:      "",
			DeviceTypeId:   "",
			DeviceLocalId:  "",
			ServiceLocalId: "",
			DeviceName:     "",
		}
		rows := len(line)
		if rows != 5 && rows != 6 {
			err = errors.New("invalid cow count (expect 5 or 6 rows)")
			log.Println("error on config load:\n", location, "\n", err)
			return topicDescriptions, err
		}
		temp.Topic = strings.TrimSpace(line[0])
		temp.DeviceLocalId = strings.TrimSpace(line[1])
		temp.DeviceName = strings.TrimSpace(line[2])
		temp.DeviceTypeId = strings.TrimSpace(line[3])
		temp.ServiceLocalId = strings.TrimSpace(line[4])
		if rows == 6 {
			temp.RespTopic = strings.TrimSpace(line[5])
		}
		topicDescriptions = append(topicDescriptions, temp)
	}
	return topicDescriptions, nil
}
