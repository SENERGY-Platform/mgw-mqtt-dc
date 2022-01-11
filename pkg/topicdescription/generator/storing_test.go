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
	"github.com/SENERGY-Platform/mgw-mqtt-dc/pkg/util"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestStore(t *testing.T) {
	dir := t.TempDir()

	t.Run("add manual created files and dirs", func(t *testing.T) {
		files := []string{"foo.json", "bar.yaml", "batz.txt", "readme.md"}
		for _, name := range files {
			f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
			if err != nil {
				t.Error(err)
				return
			}
			err = f.Sync()
			if err != nil {
				t.Error(err)
				return
			}
			err = f.Close()
			if err != nil {
				t.Error(err)
				return
			}
		}
	})
	t.Run("initial generate", func(t *testing.T) {
		err := Store([]model.TopicDescription{
			{
				CmdTopic:       "with space/s1",
				DeviceLocalId:  "foobar:with space",
				ServiceLocalId: "s1",
				DeviceName:     "with space",
			},
			{
				CmdTopic:       "with-dash/s1",
				DeviceLocalId:  "foobar:with-dash",
				ServiceLocalId: "s1",
				DeviceName:     "with-dash",
			},
			{
				CmdTopic:       "with/slash/s1",
				DeviceLocalId:  "foobar:with/slash",
				ServiceLocalId: "s1",
				DeviceName:     "with/slash",
			},
			{
				CmdTopic:       "d1/s1",
				DeviceLocalId:  "foobar:d1",
				ServiceLocalId: "s1",
				DeviceName:     "unchanged",
			},
			{
				EventTopic:     "d1/s2",
				DeviceLocalId:  "foobar:d1",
				ServiceLocalId: "s2",
				DeviceName:     "unchanged",
			},
			{
				CmdTopic:       "d2/s1",
				DeviceLocalId:  "foobar:d2",
				ServiceLocalId: "s1",
				DeviceName:     "update",
			},
			{
				EventTopic:     "d2/s2",
				DeviceLocalId:  "foobar:d2",
				ServiceLocalId: "s2",
				DeviceName:     "update",
			},
			{
				CmdTopic:       "d3/s1",
				DeviceLocalId:  "foobar:d3",
				ServiceLocalId: "s1",
				DeviceName:     "delete",
			},
			{
				EventTopic:     "d3/s2",
				DeviceLocalId:  "foobar:d3",
				ServiceLocalId: "s2",
				DeviceName:     "delete",
			},
		}, dir)
		if err != nil {
			t.Error(err)
			return
		}
	})
	t.Run("check after initial generate", func(t *testing.T) {
		expected := []string{
			"bar.yaml",
			"batz.txt",
			"foo.json",
			"generated_foobar:d1.json",
			"generated_foobar:d2.json",
			"generated_foobar:d3.json",
			"generated_foobar:with%20space.json",
			"generated_foobar:with%2Fslash.json",
			"generated_foobar:with-dash.json",
			"readme.md",
		}
		files := []string{}
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if !d.IsDir() {
				filename, err := filepath.Rel(dir, path)
				if err != nil {
					t.Error(err)
					return err
				}
				files = append(files, filename)
			}
			return nil
		})
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(expected, files) {
			t.Error("\n", expected, "\n", files)
		}
	})
	t.Run("check initial d2", func(t *testing.T) {
		f, err := os.Open(filepath.Join(dir, "generated_foobar:d2.json"))
		if err != nil {
			t.Error(err)
			return
		}
		defer f.Close()
		d2 := []model.TopicDescription{}
		err = json.NewDecoder(f).Decode(&d2)
		if err != nil {
			t.Error(err)
			return
		}
		expected := []model.TopicDescription{
			{
				CmdTopic:       "d2/s1",
				DeviceLocalId:  "foobar:d2",
				ServiceLocalId: "s1",
				DeviceName:     "update",
			},
			{
				EventTopic:     "d2/s2",
				DeviceLocalId:  "foobar:d2",
				ServiceLocalId: "s2",
				DeviceName:     "update",
			},
		}
		util.ListSort(expected, func(a model.TopicDescription, b model.TopicDescription) bool {
			return a.GetTopic() < b.GetTopic()
		})
		util.ListSort(d2, func(a model.TopicDescription, b model.TopicDescription) bool {
			return a.GetTopic() < b.GetTopic()
		})
		if !reflect.DeepEqual(expected, d2) {
			t.Error("\n", expected, "\n", d2)
		}
	})
	t.Run("generate with add, remove, update, and unchanged", func(t *testing.T) {
		err := Store([]model.TopicDescription{
			{
				CmdTopic:       "with space/s1",
				DeviceLocalId:  "foobar:with space",
				ServiceLocalId: "s1",
				DeviceName:     "with space",
			},
			{
				CmdTopic:       "with-dash/s1",
				DeviceLocalId:  "foobar:with-dash",
				ServiceLocalId: "s1",
				DeviceName:     "with-dash",
			},
			{
				CmdTopic:       "with/slash/s1",
				DeviceLocalId:  "foobar:with/slash",
				ServiceLocalId: "s1",
				DeviceName:     "with/slash",
			},
			{
				CmdTopic:       "d1/s1",
				DeviceLocalId:  "foobar:d1",
				ServiceLocalId: "s1",
				DeviceName:     "unchanged",
			},
			{
				EventTopic:     "d1/s2",
				DeviceLocalId:  "foobar:d1",
				ServiceLocalId: "s2",
				DeviceName:     "unchanged",
			},
			{
				EventTopic:     "d2/s2",
				DeviceLocalId:  "foobar:d2",
				ServiceLocalId: "s2",
				DeviceName:     "updated",
			},
			{
				EventTopic:     "d2/s3",
				DeviceLocalId:  "foobar:d2",
				ServiceLocalId: "s3",
				DeviceName:     "updated",
			},
			{
				CmdTopic:       "d4/s1",
				DeviceLocalId:  "foobar:d4",
				ServiceLocalId: "s1",
				DeviceName:     "add",
			},
			{
				EventTopic:     "d4/s2",
				DeviceLocalId:  "foobar:d4",
				ServiceLocalId: "s2",
				DeviceName:     "add",
			},
		}, dir)
		if err != nil {
			t.Error(err)
			return
		}
	})
	t.Run("check after second generate", func(t *testing.T) {
		expected := []string{
			"bar.yaml",
			"batz.txt",
			"foo.json",
			"generated_foobar:d1.json",
			"generated_foobar:d2.json",
			"generated_foobar:d4.json",
			"generated_foobar:with%20space.json",
			"generated_foobar:with%2Fslash.json",
			"generated_foobar:with-dash.json",
			"readme.md",
		}
		files := []string{}
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if !d.IsDir() {
				filename, err := filepath.Rel(dir, path)
				if err != nil {
					t.Error(err)
					return err
				}
				files = append(files, filename)
			}
			return nil
		})
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(expected, files) {
			t.Error("\n", expected, "\n", files)
		}
	})

	t.Run("check updated d2", func(t *testing.T) {
		f, err := os.Open(filepath.Join(dir, "generated_foobar:d2.json"))
		if err != nil {
			t.Error(err)
			return
		}
		defer f.Close()
		d2 := []model.TopicDescription{}
		err = json.NewDecoder(f).Decode(&d2)
		if err != nil {
			t.Error(err)
			return
		}
		expected := []model.TopicDescription{
			{
				EventTopic:     "d2/s2",
				DeviceLocalId:  "foobar:d2",
				ServiceLocalId: "s2",
				DeviceName:     "updated",
			},
			{
				EventTopic:     "d2/s3",
				DeviceLocalId:  "foobar:d2",
				ServiceLocalId: "s3",
				DeviceName:     "updated",
			},
		}
		util.ListSort(expected, func(a model.TopicDescription, b model.TopicDescription) bool {
			return a.GetTopic() < b.GetTopic()
		})
		util.ListSort(d2, func(a model.TopicDescription, b model.TopicDescription) bool {
			return a.GetTopic() < b.GetTopic()
		})
		if !reflect.DeepEqual(expected, d2) {
			t.Error("\n", expected, "\n", d2)
		}
	})
}
