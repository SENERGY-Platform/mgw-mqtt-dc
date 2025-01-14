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

package connector

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

const TransformerJsonUnwrapInput = "json-unwrap-input"
const TransformerJsonUnwrapOutput = "json-unwrap-output"

func (this *Connector) handleTransformations(desc TopicDescription, kind string, payload []byte) ([]byte, error) {
	switch kind {
	case TransformerJsonUnwrapInput:
		return this.handleJsonUnwrapTransformations(desc.GetTransformations(TransformerJsonUnwrapInput), payload)
	case TransformerJsonUnwrapOutput:
		return this.handleJsonUnwrapTransformations(desc.GetTransformations(TransformerJsonUnwrapOutput), payload)
	}
	return payload, nil
}

func (this *Connector) handleJsonUnwrapTransformations(paths []string, payload []byte) ([]byte, error) {
	if len(paths) == 0 {
		return payload, nil
	}
	var value interface{}
	err := json.Unmarshal(payload, &value)
	if err != nil {
		return nil, fmt.Errorf("payload is not valid json: %w", err)
	}
	value, err = recursiveJsonUnwrap(value, paths, []string{})
	if err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func recursiveJsonUnwrap(value interface{}, paths []string, currentPath []string) (interface{}, error) {
	var err error
	switch v := value.(type) {
	case []interface{}:
		for i, e := range v {
			nextPath := []string{}
			nextPath = append(nextPath, currentPath...)
			nextPath = append(nextPath, "*")
			v[i], err = recursiveJsonUnwrap(e, paths, nextPath)
			if err != nil {
				return nil, err
			}
		}
		for i, e := range v {
			nextPath := []string{}
			nextPath = append(nextPath, currentPath...)
			nextPath = append(nextPath, strconv.Itoa(i))
			v[i], err = recursiveJsonUnwrap(e, paths, nextPath)
			if err != nil {
				return nil, err
			}
		}
		return v, nil
	case map[string]interface{}:
		for k, e := range v {
			nextPath := []string{}
			nextPath = append(nextPath, currentPath...)
			nextPath = append(nextPath, k)
			v[k], err = recursiveJsonUnwrap(e, paths, nextPath)
			if err != nil {
				return nil, err
			}
		}
		return v, nil
	case string:
		if slices.Contains(paths, strings.Join(currentPath, ".")) {
			var newVal interface{}
			err = json.Unmarshal([]byte(v), &newVal)
			return newVal, err
		}
	default:
		return v, nil
	}
	return value, nil
}
