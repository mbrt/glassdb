// Package errors provides utilities for annotating and combining errors.
package errors

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Aliases to the standard errors package.
var (
	New = errors.New
	Is  = errors.Is
	As  = errors.As
)

// bufferPool is a pool of bytes.Buffers.
var bufferPool = sync.Pool{
	New: func() any {
		return &bytes.Buffer{}
	},
}

// WithCause annotates a symptom error with a cause.
//
// Both errors can be discovered by the Is and As methods.
func WithCause(symptom, cause error) error {
	return annotated{
		cause:   cause,
		symptom: symptom,
	}
}

// WithDetails annotates an error with additional detail strings for diagnostics.
func WithDetails(err error, details ...string) error {
	if err == nil {
		return nil
	}
	return detailed{err, details}
}

// WriteDetails writes the detail annotations of err and its chain to w.
func WriteDetails(w io.Writer, err error) {
	var dErr detailed
	for errors.As(err, &dErr) {
		// Append all details of this error.
		iw := indentWriter{w}
		for _, d := range dErr.details {
			//nolint:errcheck
			io.WriteString(iw, "\n- ")
			//nolint:errcheck
			io.WriteString(indentWriter{iw}, d)
		}
		// Continue down the chain.
		err = dErr.error
	}
}

// Details returns the detail annotations of err and its chain as a string.
func Details(err error) string {
	buffer := bufferPool.Get().(*bytes.Buffer)
	WriteDetails(buffer, err)
	res := buffer.String()
	buffer.Reset()
	bufferPool.Put(buffer)
	return res
}

// Combine merges multiple errors into a single error, flattening any nested multi-errors.
func Combine(errs ...error) error {
	if len(errs) == 0 {
		return nil
	}

	var (
		res    multi
		others []error
	)
	for _, err := range errs {
		// Combine only the non-nil errors.
		if err == nil {
			continue
		}
		// Flatten by taking out the multi-error from the list
		// if any is present.
		merr, ok := err.(multi) //nolint:errorlint
		if ok {
			res = merr
			continue
		}
		others = append(others, err)
	}

	// If we found a multierror, return that and append the rest.
	if res != nil {
		return append(res, others...)
	}

	// Check the others.
	if len(others) == 0 {
		return nil
	}
	if len(others) == 1 {
		return others[0]
	}

	return multi(others)
}

// Errors returns the individual errors from a combined error, or a single-element slice otherwise.
func Errors(err error) []error {
	if err == nil {
		return nil
	}
	merr, ok := err.(multi) //nolint:errorlint
	if !ok {
		return []error{err}
	}
	return merr
}

type multi []error

func (m multi) Error() string {
	return fmt.Sprintf("multiple errors (%d); sample: %v", len(m), m[0])
}

func (m multi) Is(target error) bool {
	for _, err := range m {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

func (m multi) As(target any) bool {
	for _, err := range m {
		if errors.As(err, target) {
			return true
		}
	}
	return false
}

func (m multi) Format(f fmt.State, c rune) {
	if (c == 'v' || c == 'w') && f.Flag('+') {
		m.writeMultiline(f)
	} else {
		//nolint:errcheck
		io.WriteString(f, m.Error())
	}
}

func (m multi) writeMultiline(w io.Writer) {
	fmt.Fprintf(w, "multiple errors (%d):", len(m))
	wi := indentWriter{w}
	for _, err := range m {
		//nolint:errcheck
		io.WriteString(w, "\n- ")
		//nolint:errcheck
		fmt.Fprintf(wi, "%+v", err)
	}
}

type detailed struct {
	error
	details []string
}

func (d detailed) Format(f fmt.State, c rune) {
	if (c == 'v' || c == 'w') && f.Flag('+') {
		d.writeMultiline(f)
	} else {
		//nolint:errcheck
		io.WriteString(f, d.Error())
	}
}

func (d detailed) writeMultiline(w io.Writer) {
	fmt.Fprintf(w, "%+v", d.error)
	if len(d.details) == 0 {
		return
	}

	//nolint:errcheck
	io.WriteString(w, "\nNote:")
	iw := indentWriter{w}
	for _, d := range d.details {
		//nolint:errcheck
		io.WriteString(w, "\n- ")
		//nolint:errcheck
		io.WriteString(iw, d)
	}
}

type annotated struct {
	cause   error
	symptom error
}

func (e annotated) Error() string {
	return fmt.Sprintf("%v: %v", e.cause, e.symptom)
}

func (e annotated) Unwrap() error {
	return e.cause
}

func (e annotated) Is(target error) bool {
	return errors.Is(e.symptom, target) || errors.Is(e.cause, target)
}

func (e annotated) As(target any) bool {
	if errors.As(e.symptom, target) {
		return true
	}
	return errors.As(e.cause, target)
}

type indentWriter struct {
	io.Writer
}

func (w indentWriter) Write(b []byte) (int, error) {
	num := 0
	write := func(b []byte) error {
		n, err := w.Writer.Write(b)
		num += n
		return err
	}

	i, j := 0, 0
	for i < len(b) {
		if b[i] == '\n' {
			// Write the indentation.
			if err := write([]byte("\n  ")); err != nil {
				return num, err
			}
			i++
		}
		// Look for the next newline.
		if idx := bytes.IndexByte(b[i:], '\n'); idx >= 0 {
			j = i + idx
		} else {
			j = len(b)
		}
		// Write everything up to the newline, or the end of buffer.
		if err := write(b[i:j]); err != nil {
			return num, err
		}
		i = j
	}

	return num, nil
}
