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

package topicdescription

import (
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/configuration"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/devicerepo"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/topicdescription/generator"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/topicdescription/model"
	"log"
)

func LoadWithGenerator(config configuration.Config, repo *devicerepo.DeviceRepo) (topicDescriptions []model.TopicDescription, err error) {
	defer func() {
		topicDescriptions, err = LoadDir(config.DeviceDescriptionsDir)
	}()
	devices, deviceTypes, err := generator.GetDeviceInfos(repo, config.GeneratorPermissionSearchUrl, config.GeneratorFilterDevicesByAttribute)
	if err != nil {
		log.Println("WARNING: unable to generate topic descriptions:", err)
		return nil, err
	}
	err = generator.Store(generator.GenerateTopicDescriptions(devices, deviceTypes, config.GeneratorTruncateDevicePrefix), config.GeneratorDeviceDescriptionsDir)
	if err != nil {
		log.Println("WARNING: unable to store generated topic descriptions:", err)
		return nil, err
	}
	return
}
