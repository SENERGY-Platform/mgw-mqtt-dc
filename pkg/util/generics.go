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

import "sort"

func ListMap[From any, To any](from []From, converter func(From) To) (to []To) {
	if from != nil {
		to = make([]To, len(from))
	}
	for i, e := range from {
		to[i] = converter(e)
	}
	return
}

func ListFilter[T any](in []T, filter func(T) bool) (out []T) {
	for _, e := range in {
		if filter(e) {
			out = append(out, e)
		}
	}
	return
}

func ListContains[T any](list []T, check func(a T) bool) bool {
	for _, e := range list {
		if check(e) {
			return true
		}
	}
	return false
}

func ListFilterDuplicates[T any](s []T, equals func(a T, b T) bool) (out []T) {
	for _, a := range s {
		if !ListContains(out, func(b T) bool {
			return equals(a, b)
		}) {
			out = append(out, a)
		}
	}
	return
}

func ListSort[T any](list []T, less func(a T, b T) bool) {
	sort.Slice(list, func(i, j int) bool {
		return less(list[i], list[j])
	})
}

func FMap1[I1 any, ResultType any, NewResultType any](f func(in I1) (ResultType, error), c func(ResultType) NewResultType) func(in I1) (NewResultType, error) {
	return func(in I1) (result NewResultType, err error) {
		temp, err := f(in)
		if err != nil {
			return result, err
		}
		result = c(temp)
		return result, err
	}
}

func FMap2[I1 any, I2 any, ResultType any, NewResultType any](f func(in1 I1, in2 I2) (ResultType, error), c func(ResultType) NewResultType) func(in1 I1, in2 I2) (NewResultType, error) {
	return func(in1 I1, in2 I2) (result NewResultType, err error) {
		temp, err := f(in1, in2)
		if err != nil {
			return result, err
		}
		result = c(temp)
		return result, err
	}
}

func FMap3[I1 any, I2 any, I3 any, ResultType any, NewResultType any](f func(in1 I1, in2 I2, in3 I3) (ResultType, error), c func(ResultType) NewResultType) func(in1 I1, in2 I2, in3 I3) (NewResultType, error) {
	return func(in1 I1, in2 I2, in3 I3) (result NewResultType, err error) {
		temp, err := f(in1, in2, in3)
		if err != nil {
			return result, err
		}
		result = c(temp)
		return result, err
	}
}

func FMap4[I1 any, I2 any, I3 any, I4 any, ResultType any, NewResultType any](f func(in1 I1, in2 I2, in3 I3, in4 I4) (ResultType, error), c func(ResultType) NewResultType) func(in1 I1, in2 I2, in3 I3, in4 I4) (NewResultType, error) {
	return func(in1 I1, in2 I2, in3 I3, in4 I4) (result NewResultType, err error) {
		temp, err := f(in1, in2, in3, in4)
		if err != nil {
			return result, err
		}
		result = c(temp)
		return result, err
	}
}

func FMap5[I1 any, I2 any, I3 any, I4 any, I5 any, ResultType any, NewResultType any](f func(in1 I1, in2 I2, in3 I3, in4 I4, in5 I5) (ResultType, error), c func(ResultType) NewResultType) func(in1 I1, in2 I2, in3 I3, in4 I4, in5 I5) (NewResultType, error) {
	return func(in1 I1, in2 I2, in3 I3, in4 I4, in5 I5) (result NewResultType, err error) {
		temp, err := f(in1, in2, in3, in4, in5)
		if err != nil {
			return result, err
		}
		result = c(temp)
		return result, err
	}
}
