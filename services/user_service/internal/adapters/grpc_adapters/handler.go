package grpc_adapters

import (
	"context"

	userpb "BHLA/proto/user_service"
	"BHLA/shared/auth_context"
	"BHLA/shared/auth_roles"
	"BHLA/shared/grpc/interceptors/err_map"
	"BHLA/shared/logging"

	"BHLA/services/user_service/internal/domain"
	"BHLA/services/user_service/internal/ports"
)

type Handler struct {
	userpb.UnimplementedUserServiceServer
	uc     ports.UserInbound
	prereq PlanChangePrerequisite
	logger logging.Logger
}

func NewHandler(uc ports.UserInbound, prereq PlanChangePrerequisite, logger logging.Logger) *Handler {
	return &Handler{uc: uc, prereq: prereq, logger: logger}
}

func (h *Handler) UserRegistration(ctx context.Context, req *userpb.RegisterRequest) (*userpb.RegisterResponse, error) {
	user, err := h.uc.Register(ctx, req.GetUserName(), req.GetUserPassword())
	if err != nil {
		h.logger.LogError("register failed", logging.Err(err))
		return nil, err
	}
	return &userpb.RegisterResponse{
		UserId:   user.UserID,
		UserName: user.UserName,
		UserRole: planToProto(user.Role),
	}, nil
}

func (h *Handler) GetUserData(ctx context.Context, _ *userpb.GetUserDataRequest) (*userpb.GetUserDataResponse, error) {
	id, ok := auth_context.From(ctx)
	if !ok {
		return nil, errmap.NewError(errmap.Unauthenticated, "требуется авторизация", nil)
	}
	user, err := h.uc.GetUser(ctx, id.UserID)
	if err != nil {
		return nil, err
	}
	return &userpb.GetUserDataResponse{
		UserData: &userpb.UserData{
			UserId:   user.UserID,
			UserName: user.UserName,
			UserRole: planToProto(user.Role),
		},
	}, nil
}

func (h *Handler) PlanChange(ctx context.Context, req *userpb.PlanChangeRequest) (*userpb.PlanChangeResponse, error) {
	id, ok := auth_context.From(ctx)
	if !ok {
		return nil, errmap.NewError(errmap.Unauthenticated, "требуется авторизация", nil)
	}
	if req.GetUserId() != id.UserID {
		return nil, errmap.NewError(errmap.PermissionDenied, "можно менять только собственный тариф", nil)
	}

	newPlan, ok := protoToPlan(req.GetUserRole())
	if !ok || !domain.CanSelfPlanChange(newPlan) {
		return nil, errmap.NewError(errmap.PermissionDenied, "доступны только тарифы Free и Pro", nil)
	}
	if newPlan == auth_roles.Pro && !h.prereq.UpgradeAgree(ctx, id.UserID) {
		return nil, errmap.NewError(errmap.FailedPrecondition, "для перехода на Pro требуется выполнение условия", nil)
	}

	user, err := h.uc.PlanChange(ctx, id.UserID, newPlan)
	if err != nil {
		return nil, err
	}
	return &userpb.PlanChangeResponse{UserId: user.UserID, UserRole: planToProto(user.Role)}, nil
}
