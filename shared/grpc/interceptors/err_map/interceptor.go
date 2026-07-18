package err_map

import (
	"context"

	"google.golang.org/grpc"

	"BHLA/shared/logging"
)

func UnaryServerInterceptor(logger logging.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if err == nil {
			return resp, nil
		}
		if isHidden(err) && logger != nil {
			logger.LogError("rpc handler error",
				logging.Field{Key: "method", Value: info.FullMethod},
				logging.Err(err),
			)
		}
		return resp, toGRPCStatus(err)
	}
}

func StreamServerInterceptor(logger logging.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, ss)
		if err == nil {
			return nil
		}
		if isHidden(err) && logger != nil {
			logger.LogError("rpc stream error",
				logging.Field{Key: "method", Value: info.FullMethod},
				logging.Err(err),
			)
		}
		return toGRPCStatus(err)
	}
}
