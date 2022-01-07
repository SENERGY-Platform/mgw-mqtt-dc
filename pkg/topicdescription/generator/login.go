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

package generator

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

func NewAuth(cred Credentials) *Auth {
	return &Auth{Credentials: cred}
}

type Auth struct {
	Credentials
	OpenidToken
	mux sync.Mutex
}

type OpenidToken struct {
	AccessToken      string    `json:"access_token"`
	ExpiresIn        float64   `json:"expires_in"`
	RefreshExpiresIn float64   `json:"refresh_expires_in"`
	RefreshToken     string    `json:"refresh_token"`
	TokenType        string    `json:"token_type"`
	RequestTime      time.Time `json:"-"`
}

type Credentials struct {
	AuthEndpoint     string
	AuthClientId     string
	AuthClientSecret string
	Username         string
	Password         string
}

var TimeNow = func() time.Time {
	return time.Now()
}

func (this *Auth) Refresh() *Auth {
	this.mux.Lock()
	defer this.mux.Unlock()
	age := TimeNow().Sub(this.RequestTime).Seconds()
	if this.AccessToken != "" && this.ExpiresIn > age+5 {
		return this
	}
	if this.RefreshToken != "" && this.RefreshExpiresIn > age+5 {
		log.Println("refresh token", this.RefreshExpiresIn, age)
		openid, err := RefreshOpenidToken(this.AuthEndpoint, this.AuthClientId, this.AuthClientSecret, this.RefreshToken)
		if err != nil {
			log.Println("WARNING: unable to use refresh token", err)
		} else {
			this.OpenidToken = openid
			return this
		}
	}
	log.Println("get new access token")
	openid, err := GetOpenidPasswordToken(this.AuthEndpoint, this.AuthClientId, this.AuthClientSecret, this.Username, this.Password)
	this.OpenidToken = openid
	if err != nil {
		log.Println("ERROR: unable to get new access token", err)
	}
	return this
}

func (this *Auth) JwtToken() string {
	return "Bearer " + this.AccessToken
}

func GetOpenidPasswordToken(authEndpoint string, authClientId string, authClientSecret string, username string, password string) (token OpenidToken, err error) {
	requesttime := time.Now()
	client := http.Client{
		Timeout: 5 * time.Second,
	}

	values := url.Values{
		"client_id":  {authClientId},
		"username":   {username},
		"password":   {password},
		"grant_type": {"password"},
	}

	if authClientSecret != "" {
		values["client_secret"] = []string{authClientSecret}
	}

	resp, err := client.PostForm(authEndpoint+"/auth/realms/master/protocol/openid-connect/token", values)

	if err != nil {
		return token, err
	}
	if resp.StatusCode >= 300 {
		b, _ := ioutil.ReadAll(resp.Body)
		err = errors.New(resp.Status + ": " + string(b))
		return
	}
	err = json.NewDecoder(resp.Body).Decode(&token)
	token.RequestTime = requesttime
	return
}

func RefreshOpenidToken(authEndpoint string, authClientId string, authClientSecret string, refreshToken string) (openid OpenidToken, err error) {
	requesttime := time.Now()
	client := http.Client{
		Timeout: 5 * time.Second,
	}

	values := url.Values{
		"client_id":     {authClientId},
		"refresh_token": {refreshToken},
		"grant_type":    {"refresh_token"},
	}

	if authClientSecret != "" {
		values["client_secret"] = []string{authClientSecret}
	}

	resp, err := client.PostForm(authEndpoint+"/auth/realms/master/protocol/openid-connect/token", values)

	if err != nil {
		return openid, err
	}
	if resp.StatusCode >= 300 {
		b, _ := ioutil.ReadAll(resp.Body)
		err = errors.New(resp.Status + ": " + string(b))
		return
	}
	err = json.NewDecoder(resp.Body).Decode(&openid)
	openid.RequestTime = requesttime
	return
}
