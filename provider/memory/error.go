package memory

import "fmt"

// multiErrs is a collection of errors that occurred during a broker operation.
type multiErrs struct {
	errors []error
}

// mewMultiErrs creates a new error collection.
func mewMultiErrs(errors ...error) *multiErrs {
	return &multiErrs{
		errors: errors,
	}
}

// add adds an error to the collection.
func (e *multiErrs) add(err error) {
	e.errors = append(e.errors, err)
}

// isEmpty returns true if the collection is empty.
func (e *multiErrs) isEmpty() bool {
	return len(e.errors) == 0
}

// Error returns a string representation of the error collection.
func (e *multiErrs) Error() string {
	var customErr error

	for i, err := range e.errors {
		if customErr == nil {
			customErr = fmt.Errorf("#%d %s", len(e.errors), err)
			continue
		}

		customErr = fmt.Errorf("#%d %s: %s", len(e.errors)-i, err, customErr)
	}
	return fmt.Errorf("%s: memory broker: %d errors occurred", customErr, len(e.errors)).Error()
}
