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

package onlinechecker

import (
	"errors"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/platform-connector-lib/marshalling"
	"github.com/SENERGY-Platform/platform-connector-lib/msgvalidation"
)

func (this *Checker[T]) serialize(service models.Service, payload []byte) (result map[string]interface{}, err error) {
	msg := map[string]string{this.config.ProtocolDataFieldName: string(payload)}
	protocol := this.config.ProtocolDescription

	result = map[string]interface{}{}
	fallback, fallbackKnown := marshalling.Get("json")
	for _, output := range service.Outputs {
		marshaller, ok := marshalling.Get(string(output.Serialization))
		if !ok {
			return result, errors.New("unknown format " + string(output.Serialization))
		}
		for _, segment := range protocol.ProtocolSegments {
			if segment.Id == output.ProtocolSegmentId {
				segmentMsg, ok := msg[segment.Name]
				if ok {
					out, err := marshaller.Unmarshal(segmentMsg, output.ContentVariable)
					if err != nil && fallbackKnown {
						out, err = fallback.Unmarshal(segmentMsg, output.ContentVariable)
					}
					if err != nil {
						return result, err
					}
					result[output.ContentVariable.Name] = out
				}
			}
		}
	}

	result, err = msgvalidation.Clean(result, service)
	if err != nil {
		return result, err
	}
	return result, err
}
