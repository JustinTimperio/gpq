/*
	MIT License

	Copyright (c) 2018 Yusuke Kato

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
*/

package ftime

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type Ftime interface {
	IsDaemonRunning() bool
	GetFormat() string
	SetFormat(format string) Ftime
	GetLocation() *time.Location
	SetLocation(location *time.Location) Ftime
	Now() time.Time
	Stop()
	UnixNow() int64
	UnixUNow() uint32
	UnixNanoNow() int64
	UnixUNanoNow() uint32
	FormattedNow() []byte
	Since(t time.Time) time.Duration
	StartTimerD(ctx context.Context, dur time.Duration) Ftime
}

// Fastime is fastime's base struct, it's stores atomic time object
type fastime struct {
	uut           uint32
	uunt          uint32
	dur           int64
	ut            int64
	unt           int64
	correctionDur time.Duration
	mu            sync.Mutex
	wg            sync.WaitGroup
	running       atomic.Bool
	t             atomic.Pointer[time.Time]
	ft            atomic.Pointer[[]byte]
	format        atomic.Pointer[string]
	formatValid   atomic.Bool
	location      atomic.Pointer[time.Location]
}

const (
	bufSize   = 64
	bufMargin = 10
)

var (
	once     sync.Once
	instance Ftime
)

func init() {
	once.Do(func() {
		instance = New().StartTimerD(context.Background(), time.Millisecond*5)
	})
}

func IsDaemonRunning() (running bool) {
	return instance.IsDaemonRunning()
}

func GetLocation() (loc *time.Location) {
	return instance.GetLocation()
}

func GetFormat() (form string) {
	return instance.GetFormat()
}

// SetLocation replaces time location
func SetLocation(location *time.Location) (ft Ftime) {
	return instance.SetLocation(location)
}

// SetFormat replaces time format
func SetFormat(format string) (ft Ftime) {
	return instance.SetFormat(format)
}

// Now returns current time
func Now() (now time.Time) {
	return instance.Now()
}

// Since returns the time elapsed since t.
// It is shorthand for fastime.Now().Sub(t).
func Since(t time.Time) (dur time.Duration) {
	return instance.Since(t)
}

// Stop stops stopping time refresh daemon
func Stop() {
	instance.Stop()
}

// UnixNow returns current unix time
func UnixNow() (now int64) {
	return instance.UnixNow()
}

// UnixUNow returns current unix time
func UnixUNow() (now uint32) {
	return instance.UnixUNow()
}

// UnixNanoNow returns current unix nano time
func UnixNanoNow() (now int64) {
	return instance.UnixNanoNow()
}

// UnixUNanoNow returns current unix nano time
func UnixUNanoNow() (now uint32) {
	return instance.UnixUNanoNow()
}

// FormattedNow returns formatted byte time
func FormattedNow() (now []byte) {
	return instance.FormattedNow()
}

// StartTimerD provides time refresh daemon
func StartTimerD(ctx context.Context, dur time.Duration) (ft Ftime) {
	return instance.StartTimerD(ctx, dur)
}
