/*
 * Copyright 2026 InfAI (CC SES)
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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
)

func TestTokenErrorMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid_grant","time":"` + time.Now().Format(time.RFC3339Nano) + `"}`))
	}))
	defer server.Close()

	a := &Auth{Credentials: Credentials{AuthEndpoint: server.URL, AuthClientId: "client", Username: "user", Password: "pw"}}
	_, err := a.EnsureAccess()
	if err == nil {
		t.Fatal("expected error")
	}
	if actual := util.MgwErrorMessage(err); actual != "auth: unexpected response status (status code 401)" {
		t.Errorf("got %q (full error: %v)", actual, err)
	}
}
