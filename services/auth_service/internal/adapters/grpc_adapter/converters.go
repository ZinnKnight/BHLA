package grpc_adapter

import (
	authpb "BHLA/proto/auth_service"
	"BHLA/shared/session_validation"
)

func stateToProto(s session_validation.State) authpb.SessionState {
	switch s {
	case session_validation.StateActive:
		return authpb.SessionState_SESSION_STATE_ACTIVE
	case session_validation.StateInactive:
		return authpb.SessionState_SESSION_STATE_INACTIVE
	case session_validation.StateSuspended:
		return authpb.SessionState_SESSION_STATE_SUSPENDED
	default:
		return authpb.SessionState_SESSION_STATE_UNDEFINED
	}
}
