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
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/topicdescription/generator"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/topicdescription/model"
	"log"
	"path"
)

func LoadWithGenerator(config configuration.Config) (topicDescriptions []model.TopicDescription, err error) {
	defer func() {
		topicDescriptions, err = LoadDir(config.DeviceDescriptionsDir)
	}()
	devices, deviceTypes, err := generator.GetDeviceInfos(generator.NewAuth(generator.Credentials{
		AuthEndpoint:     config.GeneratorAuthEndpoint,
		AuthClientId:     config.GeneratorAuthClientId,
		AuthClientSecret: config.GeneratorAuthClientSecret,
		Username:         config.GeneratorAuthUsername,
		Password:         config.GeneratorAuthPassword,
	}).Refresh().JwtToken(), config.GeneratorPermissionSearchUrl, config.GeneratorDeviceRepositoryUrl, config.GeneratorFilterDevicesByAttribute)
	if err != nil {
		log.Println("WARNING: unable to generate topic descriptions:", err)
		return nil, err
	}
	err = generator.Store(generator.GenerateTopicDescriptions(devices, deviceTypes, config.GeneratorTruncateDevicePrefix), path.Join(config.DeviceDescriptionsDir, config.GeneratorDeviceDescriptionsDir))
	if err != nil {
		log.Println("WARNING: unable to store generated topic descriptions:", err)
		return nil, err
	}
	return
}
