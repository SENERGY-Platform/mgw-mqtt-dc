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
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/topicdescription/model"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func Load(config configuration.Config) (topicDescriptions []model.TopicDescription, err error) {
	return LoadDir(config.DeviceDescriptionsDir)
}

func LoadDir(dir string) (topicDescriptions []model.TopicDescription, err error) {
	topicDescriptions = []model.TopicDescription{}
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

func LoadJson(location string) (topicDescriptions []model.TopicDescription, err error) {
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

func LoadYaml(location string) (topicDescriptions []model.TopicDescription, err error) {
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

func LoadCsv(location string) (topicDescriptions []model.TopicDescription, err error) {
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
		temp := model.TopicDescription{
			CmdTopic:       "",
			EventTopic:     "",
			RespTopic:      "",
			DeviceTypeId:   "",
			DeviceLocalId:  "",
			ServiceLocalId: "",
			DeviceName:     "",
		}
		rows := len(line)
		if rows != 6 && rows != 7 {
			err = errors.New("invalid cow count (expect 6 or 7 rows)")
			log.Println("error on config load:\n", location, "\n", err)
			return topicDescriptions, err
		}
		temp.CmdTopic = strings.TrimSpace(line[0])
		temp.EventTopic = strings.TrimSpace(line[1])
		temp.DeviceLocalId = strings.TrimSpace(line[2])
		temp.DeviceName = strings.TrimSpace(line[3])
		temp.DeviceTypeId = strings.TrimSpace(line[4])
		temp.ServiceLocalId = strings.TrimSpace(line[5])
		if rows == 7 {
			temp.RespTopic = strings.TrimSpace(line[6])
		}
		topicDescriptions = append(topicDescriptions, temp)
	}
	return topicDescriptions, nil
}
