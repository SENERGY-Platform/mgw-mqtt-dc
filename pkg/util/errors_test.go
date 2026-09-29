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
package util

import (
	"errors"
	"fmt"
	"net"
	"testing"
)

func TestMgwErrorMessage(t *testing.T) {
	external := &ExternalError{Service: "device-repository", Msg: "unexpected response status", StatusCode: 500, Err: errors.New(`{"time":"2026-09-29T10:00:00Z","error":"db timeout"}`)}
	cases := []struct {
		err      error
		expected string
	}{
		{err: external, expected: "device-repository: unexpected response status (status code 500)"},
		{err: fmt.Errorf("unable to load: %w", external), expected: "device-repository: unexpected response status (status code 500)"},
		{err: &ExternalError{Service: "auth", Msg: "request failed", Err: errors.New("dial tcp 10.0.0.1:8080: connect: connection refused")}, expected: "auth: request failed"},
		{err: &net.OpError{Op: "read", Net: "tcp", Source: &net.TCPAddr{Port: 54321}, Addr: &net.TCPAddr{Port: 1883}, Err: errors.New("connection reset by peer")}, expected: "network error on read"},
		{err: errors.New("service not found"), expected: "service not found"},
	}
	for _, c := range cases {
		if actual := MgwErrorMessage(c.err); actual != c.expected {
			t.Errorf("MgwErrorMessage(%v) = %q, expected %q", c.err, actual, c.expected)
		}
	}
	if actual := external.Error(); actual != `device-repository: unexpected response status (status code 500): {"time":"2026-09-29T10:00:00Z","error":"db timeout"}` {
		t.Errorf("expected details in Error(), got %q", actual)
	}
}
