package gopl

// Result represents the outcome of an operation.
// It contains a status, a warning, and an error.
type Result[S, W, E any] struct {
	success *S
	warning *W
	error   *E
}

// Success creates a successful Result with the given status.
func Success[S, W, E any](status S) Result[S, W, E] {
	return Result[S, W, E]{success: &status}
}

// Warning creates a Result with the given status and warning.
func Warning[S, W, E any](status S, warning W) Result[S, W, E] {
	return Result[S, W, E]{success: &status, warning: &warning}
}

// Error creates a Result with the given status and error.
func Error[S, W, E any](status S, error E) Result[S, W, E] {
	return Result[S, W, E]{error: &error}
}

// Manage handles the Result by calling the appropriate handler functions.
func Manage[S, W, E, SR, WR, ER any](
	r Result[S, W, E],
	successHandler func(S) SR,
	warningHandler func(W) WR,
	errorHandler func(E) ER,
) {
	if r.success != nil {
		successHandler(*r.success)
	}
	if r.warning != nil {
		warningHandler(*r.warning)
	}
	if r.error != nil {
		errorHandler(*r.error)
	}
}
