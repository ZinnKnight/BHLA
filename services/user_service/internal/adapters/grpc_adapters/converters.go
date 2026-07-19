package grpc_adapters

import (
	userpb "BHLA/proto/user_service"
	"BHLA/shared/auth_roles"
)

func planToProto(p auth_roles.Plan) userpb.UserRoles {
	switch p {
	case auth_roles.Free:
		return userpb.UserRoles_FREE_PLAN_USER
	case auth_roles.Pro:
		return userpb.UserRoles_PRO_PLAN_USER
	case auth_roles.Admin:
		return userpb.UserRoles_ADMIN
	default:
		return userpb.UserRoles_UNAUTHORISED_USER
	}
}

func protoToPlan(r userpb.UserRoles) (auth_roles.Plan, bool) {
	switch r {
	case userpb.UserRoles_FREE_PLAN_USER:
		return auth_roles.Free, true
	case userpb.UserRoles_PRO_PLAN_USER:
		return auth_roles.Pro, true
	case userpb.UserRoles_ADMIN:
		return auth_roles.Admin, true
	default:
		return "", false
	}
}
