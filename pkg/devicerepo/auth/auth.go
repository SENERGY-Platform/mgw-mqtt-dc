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
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
	"github.com/SENERGY-Platform/service-commons/pkg/jwt"
)

type Auth struct {
	Credentials      Credentials
	CurrentTokenInfo TokenInfo
	mgwIdpClient     *MgwIdpClient
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
	AuthEndpoint      string
	AuthClientId      string
	AuthClientSecret  string
	Username          string
	Password          string
	MgwCertManagerUrl string
}

func (this Credentials) AuthEnabled() bool {
	return this.AuthEndpoint != "" && this.AuthEndpoint != "-"
}

func (this *Auth) EnsureAccess() (token string, err error) {
	if !this.Credentials.AuthEnabled() {
		return "", nil
	}
	duration := time.Now().Sub(this.CurrentTokenInfo.RequestTime).Seconds()

	if this.CurrentTokenInfo.AccessToken != "" && this.CurrentTokenInfo.ExpiresIn-5 > duration {
		token = "Bearer " + this.CurrentTokenInfo.AccessToken
		return
	}

	if this.CurrentTokenInfo.RefreshToken != "" && this.CurrentTokenInfo.RefreshExpiresIn-5 > duration {
		slog.Debug("refresh token", "refresh_expires_in", this.CurrentTokenInfo.RefreshExpiresIn, "duration", duration)
		err = refreshOpenidToken(&this.CurrentTokenInfo, this.Credentials)
		if err != nil {
			slog.Warn("unable to use refreshtoken", "error", err)
		} else {
			token = "Bearer " + this.CurrentTokenInfo.AccessToken
			return
		}
	}

	slog.Debug("get new access token")
	err = getOpenidToken(&this.CurrentTokenInfo, this.Credentials)
	if err != nil {
		slog.Error("unable to get new access token", "error", err)
		this = &Auth{}
	}
	token = "Bearer " + this.CurrentTokenInfo.AccessToken
	return
}

func (this *Auth) GetUserId(token string) (userId string, err error) {
	if token == "" {
		if this.mgwIdpClient == nil {
			this.mgwIdpClient, err = NewMgwIdpClient(this.Credentials.MgwCertManagerUrl)
			if err != nil {
				return "", err
			}
		}
		return this.mgwIdpClient.GetUserId()
	} else {
		parsedToken, err := jwt.Parse(token)
		if err != nil {
			return "", err
		}
		return parsedToken.GetUserId(), nil
	}
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
		slog.Error("getOpenidToken::PostForm()", "error", err)
		return &util.ExternalError{Service: ServiceName, Msg: "request failed", Err: err}
	}
	return handleTokenResponse(resp, token, requesttime)
}

func refreshOpenidToken(token *TokenInfo, cred Credentials) (err error) {
	requesttime := time.Now()
	resp, err := http.PostForm(cred.AuthEndpoint+"/auth/realms/master/protocol/openid-connect/token", url.Values{
		"client_id":     {cred.AuthClientId},
		"refresh_token": {token.RefreshToken},
		"grant_type":    {"refresh_token"},
	})

	if err != nil {
		return &util.ExternalError{Service: ServiceName, Msg: "request failed", Err: err}
	}
	return handleTokenResponse(resp, token, requesttime)
}

const ServiceName = "auth"

func handleTokenResponse(resp *http.Response, token *TokenInfo, requesttime time.Time) (err error) {
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return &util.ExternalError{Service: ServiceName, Msg: "unexpected response status", StatusCode: resp.StatusCode, Err: errors.New(string(body))}
	}
	err = json.NewDecoder(resp.Body).Decode(token)
	if err != nil {
		return &util.ExternalError{Service: ServiceName, Msg: "unable to decode response", Err: err}
	}
	token.RequestTime = requesttime
	return nil
}
