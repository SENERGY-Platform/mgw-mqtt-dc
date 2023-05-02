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
	"context"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/integrationtests/docker"
	"sync"
	"testing"
	"time"
)

func TestLogin(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	keycloakUrl, err := docker.Keycloak(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	auth := NewAuth(Credentials{
		AuthEndpoint:     keycloakUrl,
		AuthClientId:     "client-connector-lib",
		AuthClientSecret: "",
		Username:         "testuser",
		Password:         "testpw",
	})

	token := auth.Refresh().JwtToken()
	if token == "" {
		t.Error(auth.OpenidToken)
	}
}

func TestLoginRefresh(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	keycloakUrl, err := docker.Keycloak(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	timeNowBackup := TimeNow
	defer func() {
		TimeNow = timeNowBackup
	}()

	auth := NewAuth(Credentials{
		AuthEndpoint:     keycloakUrl,
		AuthClientId:     "client-connector-lib",
		AuthClientSecret: "",
		Username:         "testuser",
		Password:         "testpw",
	})

	//first call
	token := auth.Refresh().JwtToken()
	if token == "" {
		t.Error(auth.OpenidToken)
	}

	//second call with current token
	token = auth.Refresh().JwtToken()
	if token == "" {
		t.Error(auth.OpenidToken)
	}

	//with refresh token
	TimeNow = func() time.Time {
		return time.Now().Add(time.Duration(auth.ExpiresIn) * time.Second)
	}
	token = auth.Refresh().JwtToken()
	if token == "" {
		t.Error(auth.OpenidToken)
	}

	//refresh token is dead
	TimeNow = func() time.Time {
		return time.Now().Add(time.Duration(auth.RefreshExpiresIn) * time.Second)
	}

	token = auth.Refresh().JwtToken()
	if token == "" {
		t.Error(auth.OpenidToken)
	}
}
