package grpc_adapter

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"

	authpb "BHLA/proto/auth_service"
	"BHLA/shared/logging"

	"BHLA/services/auth_service/internal/ports"
)

type Handler struct {
	authpb.UnimplementedAuthServiceServer
	uc     ports.AuthInbound
	logger logging.Logger
}

func NewHandler(uc ports.AuthInbound, logger logging.Logger) *Handler {
	return &Handler{uc: uc, logger: logger}
}

func (h *Handler) UserLogin(ctx context.Context, req *authpb.LoginRequest) (*authpb.LoginResponse, error) {
	res, err := h.uc.Login(ctx, req.GetUserName(), req.GetUserPassword())
	if err != nil {
		return nil, err
	}
	return &authpb.LoginResponse{UserId: res.UserID, SessionId: res.SessionID}, nil
}

func (h *Handler) ValidateSession(ctx context.Context, req *authpb.ValidateSessionRequest) (*authpb.ValidateSessionResponse, error) {
	sess, err := h.uc.ValidateSession(ctx, req.GetSessionId())
	if err != nil {
		return nil, err
	}
	return &authpb.ValidateSessionResponse{
		SessionId:    sess.SessionID,
		UserId:       sess.UserID,
		SessionState: stateToProto(sess.State),
		Ttl:          timestamppb.New(sess.ValidUntil),
	}, nil
}
