package retryable

import (
	"errors"
	"time"
)

/*
Call pattern:
	default:
			var someResult

			return Retry(func()error{
				someResult, err := whatever()
				return err
			})

	// can have one option or many
		var someResult
		fn := func()error{
			someResult, err := whatever()
			return err
		}
		then, call
		err := RetryWithOpts(fn, WithMaxTimeout(time.Second * 5))
		// or
		err := RetryWithOpts(fn, WithErrorIs(<some error type>))
		// or you can chain them
		err := RetryWithOpts(fn, WithErrorIs(<some error type>), WithMaxTimeout(time.Second * 5)))
		// You can change instruction anyway you like. Last wins for repeatitve instructions
		// eg
		err := RetryWithOpts(fn, WithMaxTimeout(time.Second * 1), WithMaxTimeout(time.Second * 2))
		will mean 2 seconds timeout.That means you can provide only one error filter func (TODO: change)

		// IMPORTANT in all cases we really try not return any error other than errors generated
		// out of executing the fn itself, hence everything is heavily defaulted.

*/

type OptionType string

const (
	Invalid         OptionType = ""
	ErrorFilterFunc OptionType = "ef"
	RetryCountFunc  OptionType = "cf"
	TimeoutFunc     OptionType = "to"
	JitterFunc      OptionType = "jt"

	DefaultRetryCount           = 5
	DefaultTimeoutSec           = 6
	DefaultJitterStepDurationMs = 100
	DefaultJitterMultiplier     = 0.5
)

type Option struct {
	Type OptionType

	// Called when an error is returned from trying
	// if true is returned then error is considered retryable
	ErrorFilterFunc func(error) bool
	// Called before everytime we retry. if returned false
	// we will not retry and we will return original error
	RetryCountFunc func() bool
	// Timeout returns a channel that will have be closed
	// if total time out has elabsed
	TimeoutFunc func() <-chan time.Time
	// Jitter function is called after we retry (and failed)
	// to provide a backoff
	JitterFunc func()
}

type RetryableCall func() error

// Retry retries will all default options
func Retry(toCall RetryableCall) error {
	return retryWithOption(toCall, &Option{})
}

// RetryWithOpts retries with multiple options. All options will
// be rolled into one option (last wins). Winning option will be
// default if it has missing instructions
func RetryWithOpts(toCall RetryableCall, opts ...*Option) error {
	reduced := reduceOpts(opts)
	return retryWithOption(toCall, reduced)

}

// RetryWithOption retries with a single option that contains
// all the needed instruction. If some of the instruction is not provided
// it will be defaulted
func retryWithOption(toCall RetryableCall, option *Option) error {
	// first we assume that everything goes as planed
	// and defer anywork (specially reducing opts to meaningful set)
	// until *we have to retry*

	// we keep err because that is the actual error returned
	err := toCall()
	if err == nil {
		// no need to retry
		return nil
	}

	// default option provided
	defaultOption(option)

	// check that if that error is retryable
	if !option.ErrorFilterFunc(err) {
		return err
	}

	doneTimeout := option.TimeoutFunc()
	// now loop trying the retry
	for {
		select {
		case <-doneTimeout:
			return err
		default:
			// should we retry?
			if !option.RetryCountFunc() {
				return err
			}

			// try again
			err = toCall()
			if err == nil {
				return nil
			}

			// filter again
			if !option.ErrorFilterFunc(err) {
				return err
			}
			// jitter before we go again
			option.JitterFunc()
		}
	}
}

func reduceOpts(opts []*Option) *Option {
	reduced := &Option{}

	// last always wins
	// if there are no defaults provided. will we provide
	// package internal deafults
	for _, opt := range opts {
		switch opt.Type {
		case ErrorFilterFunc:
			reduced.ErrorFilterFunc = opt.ErrorFilterFunc
		case RetryCountFunc:
			reduced.RetryCountFunc = opt.RetryCountFunc
		case TimeoutFunc:
			reduced.TimeoutFunc = opt.TimeoutFunc
		case JitterFunc:
			reduced.JitterFunc = opt.JitterFunc
		default:
			// ignored opt
		}
	}

	return reduced
}

func defaultOption(reduced *Option) {
	// default the reduced opt
	if reduced.ErrorFilterFunc == nil {
		// default is any error
		reduced.ErrorFilterFunc = WithRetryIfErrorAny().ErrorFilterFunc
	}
	if reduced.RetryCountFunc == nil {
		reduced.RetryCountFunc = WithMaxRetryCount(DefaultRetryCount).RetryCountFunc
	}

	if reduced.TimeoutFunc == nil {
		reduced.TimeoutFunc = WithMaxTimeout(time.Second * DefaultTimeoutSec).TimeoutFunc
	}

	if reduced.JitterFunc == nil {
		reduced.JitterFunc = WithJitter(DefaultJitterStepDurationMs, DefaultJitterMultiplier).JitterFunc
	}

}

func WithRetryableErrorFilter(fn func(error) bool) *Option {
	return &Option{
		Type:            ErrorFilterFunc,
		ErrorFilterFunc: fn,
	}
}

// retry if any error
func WithRetryIfErrorAny() *Option {
	return WithRetryableErrorFilter(func(e error) bool {
		return e != nil
	})
}

// retry if error is As as in errors.As
func WithRetryIfErrorAs(cmp error) *Option {
	return WithRetryableErrorFilter(func(e error) bool {
		return errors.Is(e, cmp)
	})
}

// retry if error is As as in errors.Is
func WithRetryIfErrorIs(cmp error) *Option {
	return WithRetryableErrorFilter(func(e error) bool {
		return errors.As(e, &cmp)
	})
}

// sets max retry count
func WithMaxRetryCount(count int) *Option {
	current := 0
	if count < 0 || count > 10 {
		count = 10 // arbitrary really.
	}
	return &Option{
		Type: RetryCountFunc,
		RetryCountFunc: func() bool {
			if current > count {
				return false
			}
			current = current + 1
			return true
		},
	}
}

func WithMaxTimeout(duration time.Duration) *Option {
	return &Option{
		Type: TimeoutFunc,
		TimeoutFunc: func() <-chan time.Time {
			t := time.NewTimer(duration)
			return t.C
		},
	}
}

func WithJitter(stepDuration time.Duration, multiplier float32) *Option {
	current := float32(1)

	ms := float32(stepDuration.Microseconds())
	// min is 100s
	if ms == 0 {
		ms = float32(time.Millisecond * 100)
	}
	// max is 1s 1000
	if ms > float32(time.Millisecond*1000) {
		ms = float32(time.Millisecond * 1000)
	}

	return &Option{
		Type: JitterFunc,
		JitterFunc: func() {
			currentMultiplier := current * multiplier
			toSleep := ms + (float32(ms) * currentMultiplier)
			current = current + 1
			time.Sleep(time.Millisecond * time.Duration(toSleep))
		},
	}
}
