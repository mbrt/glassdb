// Copyright 2023 The glassdb Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package glassdb

import "log"

// Logger interface.
// TODO: Change this into structured logging instead.
type Logger interface {
	Logf(format string, v ...any)
}

type Tracer interface {
	Tracef(foramt string, v ...any)
}

type ConsoleLogger struct{}

func (ConsoleLogger) Logf(format string, v ...any) {
	log.Printf(format, v...)
}

func (ConsoleLogger) Tracef(format string, v ...any) {
	log.Printf(format, v...)
}

type NoLogger struct{}

func (NoLogger) Logf(string, ...any)   {}
func (NoLogger) Tracef(string, ...any) {}

type algoLogger struct {
	logger Logger
	tracer Tracer
}

func (m algoLogger) Logf(format string, v ...any) {
	m.logger.Logf(format, v...)
}

func (m algoLogger) Tracef(format string, v ...any) {
	m.tracer.Tracef(format, v...)
}
