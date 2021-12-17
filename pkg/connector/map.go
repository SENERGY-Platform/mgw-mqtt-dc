/*
 * Copyright 2021 InfAI (CC SES)
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

import "sync"

type Map[T any] struct {
	register map[string]T
	mux      sync.Mutex
}

func NewMap[T any]() *Map[T] {
	return &Map[T]{
		register: map[string]T{},
	}
}

func (this *Map[T]) Set(key string, desc T) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.register[key] = desc
}

func (this *Map[T]) Remove(key string) {
	this.mux.Lock()
	defer this.mux.Unlock()
	delete(this.register, key)
}

func (this *Map[T]) Get(key string) (desc T, ok bool) {
	this.mux.Lock()
	defer this.mux.Unlock()
	desc, ok = this.register[key]
	return
}

func (this *Map[T]) GetKeys() (keys []string) {
	this.mux.Lock()
	defer this.mux.Unlock()
	for topic, _ := range this.register {
		keys = append(keys, topic)
	}
	return
}

func (this *Map[T]) GetAll() (result map[string]T) {
	result = map[string]T{}
	this.mux.Lock()
	defer this.mux.Unlock()
	for key, value := range this.register {
		result[key] = value
	}
	return
}
