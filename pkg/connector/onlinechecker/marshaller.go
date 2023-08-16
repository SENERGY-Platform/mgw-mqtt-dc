/*
 * Copyright 2023 InfAI (CC SES)
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
	"fmt"
	"github.com/SENERGY-Platform/converter/lib/converter"
	marshallerconfig "github.com/SENERGY-Platform/marshaller/lib/config"
	"github.com/SENERGY-Platform/marshaller/lib/marshaller/v2"
	"github.com/SENERGY-Platform/models/go/models"
	"log"
	"runtime/debug"
)

type Marshaller struct {
	marshaller *v2.Marshaller
	deviceRepo DeviceRepoForMarshaller
}

type DeviceRepoForMarshaller interface {
	GetCharacteristic(id string) (characteristic models.Characteristic, err error)
	GetConcept(id string) (concept models.Concept, err error)
	GetConceptIdOfFunction(id string) string
	GetAspectNode(id string) (models.AspectNode, error)
}

func NewMarshaller(deviceRepo DeviceRepoForMarshaller) (result *Marshaller, err error) {
	c, err := converter.New()
	if err != nil {
		return result, err
	}
	result = &Marshaller{
		marshaller: v2.New(marshallerconfig.Config{Debug: false}, c, deviceRepo),
		deviceRepo: deviceRepo,
	}
	return result, nil
}

func (this *Marshaller) Unmarshal(service models.Service, functionId string, targetCharacteristic string, message map[string]interface{}) (value interface{}, err error) {
	path, err := this.getPath(functionId, service)
	if err != nil {
		return value, err
	}

	//no protocol is needed because we provide a serialized message
	value, err = this.marshaller.Unmarshal(models.Protocol{}, service, targetCharacteristic, path, nil, message)
	return value, err
}

func (this *Marshaller) getPath(functionId string, service models.Service) (string, error) {
	if functionId == "" {
		return "", fmt.Errorf("%v", "missing function id in conditional event description")
	}
	paths := this.marshaller.GetOutputPaths(service, functionId, nil)
	if len(paths) > 1 {
		var err error
		paths, err = this.marshaller.SortPathsByAspectDistance(this.deviceRepo, service, nil, paths)
		if err != nil {
			log.Println("ERROR:", err)
			debug.PrintStack()
			return "", fmt.Errorf("%v", err.Error())
		}
		log.Println("WARNING: found multiple paths for function and aspect. only one will be used for Unmarshall")
	}
	if len(paths) == 0 {
		return "", fmt.Errorf("%v", "no output path found for criteria")
	}
	return paths[0], nil
}
