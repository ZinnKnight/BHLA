package grpc_adapters

import "context"

type PlanChangePrerequisite interface {
	UpgradeAgree(ctx context.Context, userID string) bool
}

type StubPrerequisite struct{}

func NewStubPrerequisite() StubPrerequisite { return StubPrerequisite{} }

func (StubPrerequisite) UpgradeAgree(ctx context.Context, userID string) bool { return true }
