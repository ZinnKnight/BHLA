package grpc_adapter

import (
	authpb "BHLA/proto/auth_service"
	"BHLA/shared/sessionvalidation"
)

func stateToProto(s sessionvalidation.State) authpb.SessionState {
	switch s {
	case sessionvalidation.StateActive:
		return authpb.SessionState_SESSION_STATE_ACTIVE
	case sessionvalidation.StateInactive:
		return authpb.SessionState_SESSION_STATE_INACTIVE
	case sessionvalidation.StateSuspended:
		return authpb.SessionState_SESSION_STATE_SUSPENDED
	default:
		return authpb.SessionState_SESSION_STATE_UNDEFINED
	}
}
