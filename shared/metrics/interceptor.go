package metrics

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"BHLA/shared/metrics/metrics_port"
)

func UnaryServerInterceptor(rec metrics_port.PrometheusRecord) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		rec.IncRequest(info.FullMethod, status.Code(err).String())
		return resp, err
	}
}

func StreamServerInterceptor(rec metrics_port.PrometheusRecord) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, ss)
		rec.IncRequest(info.FullMethod, status.Code(err).String())
		return err
	}
}
