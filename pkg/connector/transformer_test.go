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

package connector

import (
	"testing"
)

func TestJsonUnwrapStableError(t *testing.T) {
	conn := &Connector{}
	payload := []byte(`{"a": "{invalid", "b": "[1,", "c": "x", "d": "{\"ok\": true}"}`)
	paths := []string{"a", "b", "c", "d"}
	_, err := conn.handleJsonUnwrapTransformations(paths, payload)
	if err == nil {
		t.Fatal("expected error")
	}
	first := err.Error()
	for range 100 {
		_, err = conn.handleJsonUnwrapTransformations(paths, payload)
		if err == nil || err.Error() != first {
			t.Fatalf("unstable error: %v != %v", err, first)
		}
	}
}
