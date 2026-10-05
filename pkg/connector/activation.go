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
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

// ActivatedDevices holds the local ids of devices that have sent at least one event.
// if a file location is set, the ids are stored in this file, so that they survive a restart.
type ActivatedDevices struct {
	mux      sync.Mutex
	ids      map[string]bool
	location string
}

func LoadActivatedDevices(location string) (result *ActivatedDevices, err error) {
	if location == "-" {
		location = ""
	}
	result = &ActivatedDevices{ids: map[string]bool{}, location: location}
	if location == "" {
		return result, nil
	}
	content, err := os.ReadFile(location)
	if errors.Is(err, os.ErrNotExist) {
		return result, nil
	}
	if err != nil {
		return result, err
	}
	ids := []string{}
	err = json.Unmarshal(content, &ids)
	if err != nil {
		return result, err
	}
	for _, id := range ids {
		result.ids[id] = true
	}
	return result, nil
}

func (this *ActivatedDevices) Contains(id string) bool {
	this.mux.Lock()
	defer this.mux.Unlock()
	return this.ids[id]
}

func (this *ActivatedDevices) Add(id string) error {
	this.mux.Lock()
	defer this.mux.Unlock()
	if this.ids[id] {
		return nil
	}
	this.ids[id] = true
	return this.store()
}

// Retain removes all ids for which keep returns false
func (this *ActivatedDevices) Retain(keep func(id string) bool) error {
	this.mux.Lock()
	defer this.mux.Unlock()
	changed := false
	for id := range this.ids {
		if !keep(id) {
			delete(this.ids, id)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return this.store()
}

// store writes into a temporary file and renames it, so that a crash does not leave a truncated file
func (this *ActivatedDevices) store() error {
	if this.location == "" {
		return nil
	}
	ids := make([]string, 0, len(this.ids))
	for id := range this.ids {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	content, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(this.location), filepath.Base(this.location)+".tmp*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	_, err = temp.Write(content)
	if err != nil {
		temp.Close()
		return err
	}
	err = temp.Sync()
	if err != nil {
		temp.Close()
		return err
	}
	err = temp.Close()
	if err != nil {
		return err
	}
	return os.Rename(temp.Name(), this.location)
}
