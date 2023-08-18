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
	"errors"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/configuration"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/mgw"
	"github.com/SENERGY-Platform/models/go/models"
	"log"
	"runtime/debug"
	"sync"
)

func New[T TopicDesc](config configuration.Config, devicerepo DeviceRepo) (result *Checker[T], err error) {
	m, err := NewMarshaller(devicerepo)
	if err != nil {
		return result, err
	}
	return &Checker[T]{
		marshaller:                           m,
		repo:                                 devicerepo,
		config:                               config,
		mux:                                  sync.Mutex{},
		knownStates:                          map[string]mgw.State{},
		knownServices:                        map[string]models.Service{},
		serviceImplementsOnlineFunctionIndex: map[string]bool{},
		deviceUsesOnlineFunctionIndex:        map[string]bool{},
	}, nil
}

type DeviceRepo interface {
	DeviceRepoForMarshaller
	GetService(deviceTypeId string, localServiceId string) (models.Service, error)
}

type TopicDesc interface {
	GetLocalServiceId() string
	GetDeviceTypeId() string
	GetLocalDeviceId() string
}

type Checker[T TopicDesc] struct {
	marshaller                           *Marshaller
	config                               configuration.Config
	mux                                  sync.Mutex
	knownStates                          map[string]mgw.State
	knownServices                        map[string]models.Service
	serviceImplementsOnlineFunctionIndex map[string]bool
	deviceUsesOnlineFunctionIndex        map[string]bool
	repo                                 DeviceRepo
}

func (this *Checker[T]) Preprocess(topics []T) error {
	if !this.config.GeneratorUse {
		return nil
	}
	this.mux.Lock()
	defer this.mux.Unlock()
	clear(this.knownServices)
	clear(this.serviceImplementsOnlineFunctionIndex)
	clear(this.deviceUsesOnlineFunctionIndex)
	for _, topic := range topics {
		service, err := this.repo.GetService(topic.GetDeviceTypeId(), topic.GetLocalServiceId())
		if err != nil {
			return err
		}
		this.knownServices[topic.GetDeviceTypeId()+"."+topic.GetLocalServiceId()] = service
		for _, output := range service.Outputs {
			if variableContainsFunction(output.ContentVariable, this.config.OnlineCheckFunctionId) {
				this.serviceImplementsOnlineFunctionIndex[topic.GetDeviceTypeId()+"."+topic.GetLocalServiceId()] = true
				this.deviceUsesOnlineFunctionIndex[topic.GetLocalDeviceId()] = true
				if _, ok := this.knownStates[topic.GetLocalDeviceId()]; !ok {
					this.knownStates[topic.GetLocalDeviceId()] = mgw.Offline
				}
			}
		}
	}
	return nil
}

func variableContainsFunction(variable models.ContentVariable, functionId string) bool {
	if variable.FunctionId == functionId {
		return true
	}
	for _, sub := range variable.SubContentVariables {
		if variableContainsFunction(sub, functionId) {
			return true
		}
	}
	return false
}

func (this *Checker[T]) LoadState(desc T) (state mgw.State, found bool) {
	if !this.config.GeneratorUse {
		return mgw.Online, true
	}
	this.mux.Lock()
	defer this.mux.Unlock()
	state, found = this.knownStates[desc.GetLocalDeviceId()]
	return state, found
}

func (this *Checker[T]) CheckAndStoreState(desc T, retained bool, payload []byte) (state mgw.State, ignore bool) {
	if !this.config.GeneratorUse {
		return "", true
	}
	if !this.deviceUsesOnlineFunction(desc) {
		return "", true
	}
	if !this.serviceImplementsOnlineFunction(desc) {
		if retained {
			return "", true
		}
		this.store(desc, mgw.Online)
		return mgw.Online, false
	}
	service, err := this.getService(desc)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		return "", true
	}

	msg, err := this.serialize(service, payload)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		return "", true
	}

	result, err := this.marshaller.Unmarshal(service, this.config.OnlineCheckFunctionId, this.config.OnlineCheckBooleanCharacteristicId, msg)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		return "", true
	}

	if b, ok := result.(bool); ok {
		state = mgw.Offline
		if b {
			state = mgw.Online
		}
		this.store(desc, state)
		return state, false
	} else {
		return "", true
	}
}

func (this *Checker[T]) getService(desc T) (models.Service, error) {
	this.mux.Lock()
	defer this.mux.Unlock()
	service, ok := this.knownServices[desc.GetDeviceTypeId()+"."+desc.GetLocalServiceId()]
	if !ok {
		return service, errors.New("service not found in knownServices")
	}
	return service, nil
}

func (this *Checker[T]) store(desc T, state mgw.State) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.knownStates[desc.GetLocalDeviceId()] = state
}

func (this *Checker[T]) serviceImplementsOnlineFunction(desc T) bool {
	this.mux.Lock()
	defer this.mux.Unlock()
	return this.serviceImplementsOnlineFunctionIndex[desc.GetDeviceTypeId()+"."+desc.GetLocalServiceId()]
}

func (this *Checker[T]) deviceUsesOnlineFunction(desc T) bool {
	this.mux.Lock()
	defer this.mux.Unlock()
	return this.deviceUsesOnlineFunctionIndex[desc.GetLocalDeviceId()]
}
