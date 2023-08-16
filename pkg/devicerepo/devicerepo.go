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
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/devicerepo/auth"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/devicerepo/cache"
	"github.com/SENERGY-Platform/models/go/models"
	"io"
	"log"
	"net/http"
	"net/url"
	"runtime/debug"
	"strings"
	"time"
)

func New(config RepoConfig, auth *auth.Auth) (result *DeviceRepo, err error) {
	result = &DeviceRepo{
		auth:   auth,
		config: config,
	}
	cacheDuration, err := time.ParseDuration(config.CacheDuration)
	if err != nil {
		return result, err
	}
	if config.FallbackFile == "" {
		result.cache = cache.NewCache(cacheDuration)
	} else {
		fallback, err := cache.NewFallback(config.FallbackFile)
		if err != nil {
			return result, err
		}
		result.cache = cache.NewCacheWithFallback(cacheDuration, fallback)
	}
	return result, nil
}

type RepoConfig struct {
	DeviceRepositoryUrl string
	CacheDuration       string
	FallbackFile        string
}

type DeviceRepo struct {
	auth   *auth.Auth
	cache  *cache.Cache
	config RepoConfig
}

func (this *DeviceRepo) GetJson(token string, endpoint string, result interface{}) (err error) {
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", token)
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	resp, err := client.Do(req)
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

func (this *DeviceRepo) GetCharacteristic(id string) (result models.Characteristic, err error) {
	err = this.cache.Use("characteristics."+id, func() (interface{}, error) {
		return this.getCharacteristic(id)
	}, &result)
	return
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
	err = this.cache.Use("concept."+id, func() (interface{}, error) {
		return this.getConcept(id)
	}, &result)
	return
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
	err = this.cache.Use("functions."+id, func() (interface{}, error) {
		return this.getFunction(id)
	}, &result)
	return
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
	err = this.cache.Use("aspect-nodes."+id, func() (interface{}, error) {
		return this.getAspectNode(id)
	}, &result)
	return
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
	err = this.cache.Use("device-types."+id, func() (interface{}, error) {
		return this.getDeviceType(id)
	}, &result)
	return
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
