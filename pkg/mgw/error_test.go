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

package mgw

import (
	"testing"
	"time"
)

func TestErrorDeduplication(t *testing.T) {
	start := time.Now()
	d := newErrorDeduplication(time.Hour)

	if !d.reserve("error/device/d1", "msg", start) {
		t.Error("expected first message to be sent")
	}
	if d.reserve("error/device/d1", "msg", start.Add(59*time.Minute)) {
		t.Error("expected same message within period to be suppressed")
	}
	if !d.reserve("error/device/d2", "msg", start.Add(time.Minute)) {
		t.Error("expected same message on other topic to be sent")
	}
	if !d.reserve("error/device/d1", "other msg", start.Add(time.Minute)) {
		t.Error("expected other message on same topic to be sent")
	}
	if !d.reserve("error/device/d1", "msg", start.Add(time.Hour)) {
		t.Error("expected same message after period to be sent")
	}
	if d.reserve("error/device/d1", "msg", start.Add(time.Hour+time.Minute)) {
		t.Error("expected period to restart with the last sent message")
	}

	// a message that could not be sent is not suppressed
	if !d.reserve("error/client", "msg", start) {
		t.Error("expected first message to be sent")
	}
	d.release("error/client", "msg")
	if !d.reserve("error/client", "msg", start.Add(time.Second)) {
		t.Error("expected released message to be sent")
	}

	// expired entries are removed
	d.reserve("error/device/d3", "msg", start.Add(3*time.Hour))
	if len(d.sent) != 1 {
		t.Errorf("expected only the latest entry to remain, got %v", d.sent)
	}
}

func TestErrorDeduplicationDisabled(t *testing.T) {
	now := time.Now()
	for _, d := range []*errorDeduplication{nil, newErrorDeduplication(0)} {
		if !d.reserve("error/client", "msg", now) || !d.reserve("error/client", "msg", now) {
			t.Error("expected disabled deduplication to send every message")
		}
	}
}
