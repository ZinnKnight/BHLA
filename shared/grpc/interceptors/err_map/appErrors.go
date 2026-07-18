package err_map

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SpecialErrType int

const (
	Unknown SpecialErrType = iota
	Invalid
	Unauthenticated
	PermissionDenied
	FailedPrecondition
	NotFound
	AlreadyExists
	RateLimited
	InternalError
	Unavailable
)

const fallbackMessage = "Произошла ошибка, попробуйте ещё раз"

type Error struct {
	SpecialErrType SpecialErrType
	PublicMessage  string
	cause          error
}

func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %s", e.PublicMessage, e.cause)
	}
	return e.PublicMessage
}

func (e *Error) Unwrap() error { return e.cause }

func NewError(t SpecialErrType, publicMessage string, cause error) *Error {
	return &Error{SpecialErrType: t, PublicMessage: publicMessage, cause: cause}
}

func codeFor(t SpecialErrType) codes.Code {
	switch t {
	case Invalid:
		return codes.InvalidArgument
	case Unauthenticated:
		return codes.Unauthenticated
	case PermissionDenied:
		return codes.PermissionDenied
	case FailedPrecondition:
		return codes.FailedPrecondition
	case NotFound:
		return codes.NotFound
	case AlreadyExists:
		return codes.AlreadyExists
	case RateLimited:
		return codes.ResourceExhausted
	case Unavailable:
		return codes.Unavailable
	case InternalError, Unknown:
		fallthrough
	default:
		return codes.Internal
	}
}

type errMapping struct {
	t       SpecialErrType
	message string
}

var register = map[error]errMapping{}

func RegisterError(origin error, t SpecialErrType, publicMessage string) {
	if origin == nil {
		return
	}
	register[origin] = errMapping{t: t, message: publicMessage}
}

func toGRPCStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}

	var appErr *Error
	if errors.As(err, &appErr) {
		return status.Error(codeFor(appErr.SpecialErrType), appErr.PublicMessage)
	}

	for origin, m := range register {
		if errors.Is(err, origin) {
			return status.Error(codeFor(m.t), m.message)
		}
	}

	return status.Error(codes.Internal, fallbackMessage)
}

func isHidden(err error) bool {
	if err == nil {
		return false
	}
	if st, ok := status.FromError(err); ok {
		c := st.Code()
		return c == codes.Internal || c == codes.Unavailable || c == codes.Unknown
	}
	var appErr *Error
	if errors.As(err, &appErr) {
		return appErr.SpecialErrType == InternalError ||
			appErr.SpecialErrType == Unavailable ||
			appErr.SpecialErrType == Unknown
	}
	for origin := range register {
		if errors.Is(err, origin) {
			return false
		}
	}
	return true
}
