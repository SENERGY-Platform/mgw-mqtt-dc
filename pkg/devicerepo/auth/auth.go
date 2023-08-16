/*
 * Copyright (c) 2023 InfAI (CC SES)
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

package auth

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"
)

type Auth struct {
	Credentials      Credentials
	CurrentTokenInfo TokenInfo
}

type TokenInfo struct {
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

func (this *Auth) EnsureAccess() (token string, err error) {
	duration := time.Now().Sub(this.CurrentTokenInfo.RequestTime).Seconds()

	if this.CurrentTokenInfo.AccessToken != "" && this.CurrentTokenInfo.ExpiresIn-5 > duration {
		token = "Bearer " + this.CurrentTokenInfo.AccessToken
		return
	}

	if this.CurrentTokenInfo.RefreshToken != "" && this.CurrentTokenInfo.RefreshExpiresIn-5 > duration {
		log.Println("refresh token", this.CurrentTokenInfo.RefreshExpiresIn, duration)
		err = refreshOpenidToken(&this.CurrentTokenInfo, this.Credentials)
		if err != nil {
			log.Println("WARNING: unable to use refreshtoken", err)
		} else {
			token = "Bearer " + this.CurrentTokenInfo.AccessToken
			return
		}
	}

	log.Println("get new access token")
	err = getOpenidToken(&this.CurrentTokenInfo, this.Credentials)
	if err != nil {
		log.Println("ERROR: unable to get new access token", err)
		this = &Auth{}
	}
	token = "Bearer " + this.CurrentTokenInfo.AccessToken
	return
}

func getOpenidToken(token *TokenInfo, cred Credentials) (err error) {
	requesttime := time.Now()
	var values url.Values
	if cred.AuthClientSecret == "" {
		values = url.Values{
			"client_id":  {cred.AuthClientId},
			"username":   {cred.Username},
			"password":   {cred.Password},
			"grant_type": {"password"},
		}
	} else {
		values = url.Values{
			"client_id":     {cred.AuthClientId},
			"client_secret": {cred.AuthClientSecret},
			"grant_type":    {"client_credentials"},
		}

	}
	resp, err := http.PostForm(cred.AuthEndpoint+"/auth/realms/master/protocol/openid-connect/token", values)

	if err != nil {
		log.Println("ERROR: getOpenidToken::PostForm()", err)
		return err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		err = errors.New(string(body))
		resp.Body.Close()
		return
	}
	err = json.NewDecoder(resp.Body).Decode(token)
	token.RequestTime = requesttime
	return
}

func refreshOpenidToken(token *TokenInfo, cred Credentials) (err error) {
	requesttime := time.Now()
	resp, err := http.PostForm(cred.AuthEndpoint+"/auth/realms/master/protocol/openid-connect/token", url.Values{
		"client_id":     {cred.AuthClientId},
		"refresh_token": {token.RefreshToken},
		"grant_type":    {"refresh_token"},
	})

	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		err = errors.New(string(body))
		resp.Body.Close()
		return
	}
	err = json.NewDecoder(resp.Body).Decode(token)
	token.RequestTime = requesttime
	return
}
