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

package devicerepo

import (
	"encoding/json"
	"errors"
	"github.com/SENERGY-Platform/device-repository/lib/client"
	"github.com/SENERGY-Platform/device-repository/lib/model"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/devicerepo/auth"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/service-commons/pkg/cache"
	"github.com/SENERGY-Platform/service-commons/pkg/cache/fallback"
	"io"
	"log"
	"net/http"
	"net/url"
	"runtime/debug"
	"strings"
	"time"
)

func New(config RepoConfig, auth *auth.Auth) (result *DeviceRepo, err error) {
	cacheDuration, err := time.ParseDuration(config.CacheDuration)
	if err != nil {
		return result, err
	}
	result = &DeviceRepo{
		auth:            auth,
		config:          config,
		cacheExpiration: cacheDuration,
	}
	result.client = client.NewClient(config.DeviceRepositoryUrl, func() (token string, err error) {
		return result.GetToken()
	})
	cacheConf := cache.Config{}
	if config.FallbackFile != "" && config.FallbackFile != "-" {
		cacheConf.FallbackProvider = fallback.NewProvider(config.FallbackFile)
	}
	result.cache, err = cache.New(cacheConf)
	if err != nil {
		return result, err
	}
	return result, nil
}

type RepoConfig struct {
	DeviceRepositoryUrl string
	CacheDuration       string
	FallbackFile        string
}

type DeviceRepo struct {
	auth            *auth.Auth
	cache           *cache.Cache
	config          RepoConfig
	cacheExpiration time.Duration
	client          client.Interface
}

func (this *DeviceRepo) GetJson(token string, endpoint string, result interface{}) (err error) {
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return err
	}
	if token != "" {
		req.Header.Set("Authorization", token)
	}
	c := &http.Client{
		Timeout: 10 * time.Second,
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		//internal service errors may be retried
		temp, _ := io.ReadAll(resp.Body)
		return errors.New(strings.TrimSpace(string(temp)))
	}
	if resp.StatusCode >= 300 {
		temp, _ := io.ReadAll(resp.Body)
		return errors.New(strings.TrimSpace(string(temp)))
	}
	err = json.NewDecoder(resp.Body).Decode(result)
	if err != nil {
		log.Println("ERROR:", err.Error())
		debug.PrintStack()
		return errors.New(err.Error())
	}
	return nil
}

func (this *DeviceRepo) GetToken() (string, error) {
	if this.auth == nil {
		this.auth = &auth.Auth{}
	}
	return this.auth.EnsureAccess()
}

func (this *DeviceRepo) GetUserId(token string) (userId string, err error) {
	if this.auth == nil {
		this.auth = &auth.Auth{}
	}
	return this.auth.GetUserId(token)
}

func (this *DeviceRepo) GetCharacteristic(id string) (result models.Characteristic, err error) {
	return cache.Use(this.cache, "characteristics."+id, func() (models.Characteristic, error) {
		return this.getCharacteristic(id)
	}, func(characteristic models.Characteristic) error {
		if characteristic.Id == "" {
			return errors.New("invalid characteristic returned from cache")
		}
		return nil
	}, this.cacheExpiration)
}

func (this *DeviceRepo) getCharacteristic(id string) (result models.Characteristic, err error) {
	token, err := this.GetToken()
	if err != nil {
		return result, err
	}

	err = this.GetJson(token, this.config.DeviceRepositoryUrl+"/characteristics/"+url.PathEscape(id), &result)
	return
}

func (this *DeviceRepo) GetConcept(id string) (result models.Concept, err error) {
	return cache.Use(this.cache, "concept."+id, func() (models.Concept, error) {
		return this.getConcept(id)
	}, func(concept models.Concept) error {
		if concept.Id == "" {
			return errors.New("invalid concept returned from cache")
		}
		return nil
	}, this.cacheExpiration)
}

func (this *DeviceRepo) getConcept(id string) (result models.Concept, err error) {
	token, err := this.GetToken()
	if err != nil {
		return result, err
	}
	err = this.GetJson(token, this.config.DeviceRepositoryUrl+"/concepts/"+url.PathEscape(id), &result)
	return
}

func (this *DeviceRepo) GetConceptIdOfFunction(id string) string {
	function, err := this.GetFunction(id)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		return ""
	}
	return function.ConceptId
}

func (this *DeviceRepo) GetFunction(id string) (result models.Function, err error) {
	return cache.Use(this.cache, "functions."+id, func() (models.Function, error) {
		return this.getFunction(id)
	}, func(function models.Function) error {
		if function.Id == "" {
			return errors.New("invalid function returned from cache")
		}
		return nil
	}, this.cacheExpiration)
}

func (this *DeviceRepo) getFunction(id string) (result models.Function, err error) {
	token, err := this.GetToken()
	if err != nil {
		return result, err
	}
	err = this.GetJson(token, this.config.DeviceRepositoryUrl+"/functions/"+url.PathEscape(id), &result)
	return
}

func (this *DeviceRepo) GetAspectNode(id string) (result models.AspectNode, err error) {
	return cache.Use(this.cache, "aspect-nodes."+id, func() (models.AspectNode, error) {
		return this.getAspectNode(id)
	}, func(node models.AspectNode) error {
		if node.Id == "" {
			return errors.New("invalid node returned from cache")
		}
		return nil
	}, this.cacheExpiration)
}

func (this *DeviceRepo) getAspectNode(id string) (result models.AspectNode, err error) {
	token, err := this.GetToken()
	if err != nil {
		return result, err
	}
	err = this.GetJson(token, this.config.DeviceRepositoryUrl+"/aspect-nodes/"+url.QueryEscape(id), &result)
	return
}

func (this *DeviceRepo) GetDeviceType(id string) (result models.DeviceType, err error) {
	return cache.Use(this.cache, "device-types."+id, func() (models.DeviceType, error) {
		return this.getDeviceType(id)
	}, func(deviceType models.DeviceType) error {
		if deviceType.Id == "" {
			return errors.New("invalid device type returned from cache")
		}
		return nil
	}, this.cacheExpiration)
}

func (this *DeviceRepo) getDeviceType(id string) (result models.DeviceType, err error) {
	token, err := this.GetToken()
	if err != nil {
		return result, err
	}
	err = this.GetJson(token, this.config.DeviceRepositoryUrl+"/device-types/"+url.QueryEscape(id), &result)
	return
}

func (this *DeviceRepo) GetService(deviceTypeId string, localServiceId string) (result models.Service, err error) {
	dt, err := this.GetDeviceType(deviceTypeId)
	if err != nil {
		return result, err
	}
	for _, s := range dt.Services {
		if s.LocalId == localServiceId {
			return s, nil
		}
	}
	return result, errors.New("service not found")
}

func (this *DeviceRepo) ListDeviceTypes(token string, listOptions model.DeviceTypeListOptions) (result []models.DeviceType, err error, errCode int) {
	result, _, err, errCode = this.client.ListDeviceTypesV3(token, listOptions)
	return
}

func (this *DeviceRepo) ListDevices(token string, options model.DeviceListOptions) (result []models.Device, err error, errCode int) {
	return this.client.ListDevices(token, options)
}
