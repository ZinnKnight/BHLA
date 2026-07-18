package validation

import (
	"context"

	"google.golang.org/grpc"

	"BHLA/shared/grpc/interceptors/err_map"
)

type allErrors interface {
	ValidateAll() error
}

type legacy interface {
	Validate() error
}

func validate(req any) error {
	if v, ok := req.(allErrors); ok {
		return v.ValidateAll()
	}
	if v, ok := req.(legacy); ok {
		return v.Validate()
	}
	return nil
}

func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := validate(req); err != nil {
			return nil, err_map.NewError(err_map.Invalid, "некорректные данные запроса", err)
		}
		return handler(ctx, req)
	}
}
