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

package docker

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"github.com/ory/dockertest/v3"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

func Keycloak(ctx context.Context, wg *sync.WaitGroup) (hostPort string, ipAddress string, err error) {
	log.Println("start keycloak")
	pool, err := dockertest.NewPool("")
	if err != nil {
		return "", "", err
	}

	//keycloak, err := pool.Run("fgseitsrancher.wifa.intern.uni-leipzig.de:5000/keycloak", "dev", []string{
	keycloak, err := pool.Run("jboss/keycloak", "11.0.3", []string{
		"KEYCLOAK_USER=testuser",
		"KEYCLOAK_PASSWORD=testpw",
		"PROXY_ADDRESS_FORWARDING=true",
		"DB_USER=keycloak",
	})
	if err != nil {
		return "", "", err
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		log.Println("DEBUG: remove container " + keycloak.Container.Name)
		log.Println(keycloak.Close())
	}()

	//go Dockerlog(pool, ctx, keycloak, "KEYCLOAK")

	hostPort = keycloak.GetPort("8080/tcp")
	err = pool.Retry(func() error {
		//get admin access token
		form := url.Values{}
		form.Add("username", "testuser")
		form.Add("password", "testpw")
		form.Add("grant_type", "password")
		form.Add("client_id", "admin-cli")
		resp, err := http.Post(
			"http://"+keycloak.Container.NetworkSettings.IPAddress+":8080/auth/realms/master/protocol/openid-connect/token",
			"application/x-www-form-urlencoded",
			strings.NewReader(form.Encode()))
		if err != nil {
			log.Println("unable to request admin token", err)
			return err
		}
		tokenMsg := map[string]interface{}{}
		err = json.NewDecoder(resp.Body).Decode(&tokenMsg)
		if err != nil {
			log.Println("unable to decode admin token", err)
			return err
		}
		return nil
	})
	return hostPort, keycloak.Container.NetworkSettings.IPAddress, err
}

//go:embed keycloak.json
var keycloakImport []byte

func ConfigKeycloak(url string) (err error) {
	log.Println("send config to keycloak")
	token, err := GetKeycloakAdminToken(url)
	if err != nil {
		return err
	}
	client := http.Client{
		Timeout: 5 * time.Second,
	}

	req, err := http.NewRequest("POST", url+"/auth/admin/realms/master/partialImport", bytes.NewReader(keycloakImport))
	if err != nil {
		return err
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	req.WithContext(ctx)
	req.Header.Set("Authorization", token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := ioutil.ReadAll(resp.Body)
		err = errors.New(resp.Status + ": " + string(b))
		return
	}
	return nil
}

func GetKeycloakAdminToken(authEndpoint string) (token string, err error) {
	client := http.Client{
		Timeout: 5 * time.Second,
	}
	resp, err := client.PostForm(authEndpoint+"/auth/realms/master/protocol/openid-connect/token", url.Values{
		"username":   {"testuser"},
		"password":   {"testpw"},
		"client_id":  {"admin-cli"},
		"grant_type": {"password"},
	})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := ioutil.ReadAll(resp.Body)
		err = errors.New(resp.Status + ": " + string(b))
		return
	}
	result := map[string]interface{}{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	token, _ = result["access_token"].(string)
	token = "Bearer " + token
	return
}
