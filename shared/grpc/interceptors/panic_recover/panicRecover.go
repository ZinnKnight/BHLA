package panic_recover

import (
	"context"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"BHLA/shared/logging"
)

func UnaryServerInterceptor(logger logging.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.LogError("panic recovered in grpc handler",
					logging.Field{Key: "method", Value: info.FullMethod},
					logging.Field{Key: "stack", Value: string(debug.Stack())},
					logging.Field{Key: "panic", Value: r},
				)
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}

func StreamServerInterceptor(logger logging.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.LogError("panic recovered in grpc stream",
					logging.Field{Key: "method", Value: info.FullMethod},
					logging.Field{Key: "stack", Value: string(debug.Stack())},
					logging.Field{Key: "panic", Value: r},
				)
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(srv, ss)
	}
}
