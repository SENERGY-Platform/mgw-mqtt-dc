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

package util

import "sync"

type SyncMap[T any] struct {
	m   map[string]T
	mux sync.Mutex
}

func NewSyncMap[T any]() *SyncMap[T] {
	return &SyncMap[T]{
		m: map[string]T{},
	}
}

func (this *SyncMap[T]) Do(f func(m *map[string]T)) {
	this.mux.Lock()
	defer this.mux.Unlock()
	f(&this.m)
}

func (this *SyncMap[T]) Update(key string, update func(value T) T) {
	this.Do(func(m *map[string]T) {
		v := (*m)[key]
		(*m)[key] = update(v)
	})
}

func (this *SyncMap[T]) Set(key string, value T) {
	this.Do(func(m *map[string]T) {
		(*m)[key] = value
	})
}

func (this *SyncMap[T]) Remove(key string) {
	this.Do(func(m *map[string]T) {
		delete(*m, key)
	})
}

func (this *SyncMap[T]) Get(key string) (value T, ok bool) {
	this.Do(func(m *map[string]T) {
		value, ok = (*m)[key]
	})
	return
}

func (this *SyncMap[T]) GetKeys() (keys []string) {
	this.Do(func(m *map[string]T) {
		for topic, _ := range *m {
			keys = append(keys, topic)
		}
	})
	return
}

func (this *SyncMap[T]) GetAll() (result map[string]T) {
	result = map[string]T{}
	this.Do(func(m *map[string]T) {
		for key, value := range *m {
			result[key] = value
		}
	})
	return
}
