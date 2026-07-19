package metrics

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func UnaryServerInterceptor(rec Recorder) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		start := time.Now()
		defer func() {
			rec.Observe(info.FullMethod, status.Code(err).String(), time.Since(start))
		}()
		return handler(ctx, req)
	}
}

func StreamServerInterceptor(rec Recorder) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		start := time.Now()
		defer func() {
			rec.Observe(info.FullMethod, status.Code(err).String(), time.Since(start))
		}()
		return handler(srv, ss)
	}
}
