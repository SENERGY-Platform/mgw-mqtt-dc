/*
 * Copyright 2025 InfAI (CC SES)
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
	"context"
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/mgw-cloud-proxy/cert-manager/lib/client"
	"net/http"
	"net/url"
	"sync"
	"time"
)

func NewMgwIdpClient(mgwCertManagerUrl string) (*MgwIdpClient, error) {
	if mgwCertManagerUrl == "" {
		return nil, errors.New("missing mgwCertManagerUrl")
	}
	_, err := url.Parse(mgwCertManagerUrl)
	if err != nil {
		return nil, fmt.Errorf("invalid mgwCertManagerUrl: %w", err)
	}
	return &MgwIdpClient{client: client.New(http.DefaultClient, mgwCertManagerUrl)}, nil
}

type MgwIdpClient struct {
	client                *client.Client
	lastUserId            string
	lastUserIdRequestTime time.Time
	mux                   sync.Mutex
}

func (this *MgwIdpClient) GetUserId() (string, error) {
	this.mux.Lock()
	defer this.mux.Unlock()
	if this.lastUserId != "" && time.Since(this.lastUserIdRequestTime) < 10*time.Minute {
		return this.lastUserId, nil
	}
	network, err := this.client.NetworkInfo(context.Background(), false, "")
	if err != nil {
		return "", err
	}
	this.lastUserId = network.UserID
	this.lastUserIdRequestTime = time.Now()
	return this.lastUserId, nil
}
