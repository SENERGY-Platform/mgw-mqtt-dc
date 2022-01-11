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

package generator

import (
	"encoding/json"
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/topicdescription/model"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

func Store(descriptions []model.TopicDescription, dir string) (err error) {
	localDeviceIdToDescriptions := map[string][]model.TopicDescription{}
	for _, desc := range descriptions {
		localDeviceIdToDescriptions[desc.DeviceLocalId] = append(localDeviceIdToDescriptions[desc.DeviceLocalId], desc)
	}

	//create or update files
	generatedFiles := map[string]bool{}
	for localId, desc := range localDeviceIdToDescriptions {
		fileName := getGeneratedFileName(localId)
		generatedFiles[fileName] = true
		fileLocation := filepath.Join(dir, fileName)
		log.Println("GENERATOR: update/create", fileLocation)
		err = StoreFile(desc, fileLocation)
		if err != nil {
			return err
		}
	}

	//remove unused files
	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, f := range files {
		name := f.Name()
		if !generatedFiles[name] && strings.HasPrefix(name, FileNamePrefix) {
			fileLocation := filepath.Join(dir, name)
			log.Println("GENERATOR: remove", fileLocation)
			err = os.Remove(fileLocation)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

const FileNamePrefix = "generated_"

func getGeneratedFileName(localId string) string {
	sanitized := url.PathEscape(localId)
	return FileNamePrefix + sanitized + ".json"
}

func StoreFile(descriptions []model.TopicDescription, fileLocation string) (err error) {
	file, err := os.OpenFile(fileLocation, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	err = json.NewEncoder(file).Encode(descriptions)
	if err != nil {
		return err
	}
	return file.Sync()
}
