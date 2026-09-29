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
	"net"
	"strconv"
)

// ExternalError is an error caused by a request to an external service.
// Error() contains all details (response body, addresses, ...) and is meant for logs.
// MgwMessage() contains only a fixed text and the status code, so that the message is stable
// and repeated errors can be deduplicated before they are sent to the mgw.
type ExternalError struct {
	Service    string // name of the external service, e.g. "device-repository"
	Msg        string // fixed text without variable content, e.g. "unexpected response status"
	StatusCode int    // http status code, 0 if no response has been received
	Err        error
}

func (this *ExternalError) Error() string {
	result := this.MgwMessage()
	if this.Err != nil {
		result = result + ": " + this.Err.Error()
	}
	return result
}

func (this *ExternalError) Unwrap() error {
	return this.Err
}

func (this *ExternalError) MgwMessage() string {
	result := this.Service + ": " + this.Msg
	if this.StatusCode != 0 {
		result = result + " (status code " + strconv.Itoa(this.StatusCode) + ")"
	}
	return result
}

// MgwErrorMessage returns the text of err that may be sent to the mgw.
// details of external errors and network errors (response bodies, addresses, ports) are omitted,
// because they would make the message unstable.
func MgwErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	var externalErr *ExternalError
	if errors.As(err, &externalErr) {
		return externalErr.MgwMessage()
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return "network error on " + opErr.Op
	}
	return err.Error()
}
