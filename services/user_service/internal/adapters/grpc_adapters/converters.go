package grpc_adapters

import (
	userpb "BHLA/proto/user_service"
	"BHLA/shared/authroles"
)

func planToProto(p authroles.Plan) userpb.UserRoles {
	switch p {
	case authroles.Free:
		return userpb.UserRoles_FREE_PLAN_USER
	case authroles.Pro:
		return userpb.UserRoles_PRO_PLAN_USER
	case authroles.Admin:
		return userpb.UserRoles_ADMIN
	default:
		return userpb.UserRoles_UNAUTHORISED_USER
	}
}

func protoToPlan(r userpb.UserRoles) (authroles.Plan, bool) {
	switch r {
	case userpb.UserRoles_FREE_PLAN_USER:
		return authroles.Free, true
	case userpb.UserRoles_PRO_PLAN_USER:
		return authroles.Pro, true
	case userpb.UserRoles_ADMIN:
		return authroles.Admin, true
	default:
		return "", false
	}
}
