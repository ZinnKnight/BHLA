package usecase

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"BHLA/shared/auth_roles"
	"BHLA/shared/events"
	"BHLA/shared/logging"
	"BHLA/shared/password_hash"
	"BHLA/shared/tx_manager"

	"BHLA/services/user_service/internal/domain"
	"BHLA/services/user_service/internal/ports"
)

var _ ports.UserInbound = (*UserUseCase)(nil)

type UserUseCase struct {
	repo   ports.UserRepo
	events events.Emitter
	txm    *tx_manager.TxManager
	logger logging.Logger
}

func New(repo ports.UserRepo, emitter events.Emitter, txm *tx_manager.TxManager, logger logging.Logger) *UserUseCase {
	return &UserUseCase{repo: repo, events: emitter, txm: txm, logger: logger}
}

func (uc *UserUseCase) Register(ctx context.Context, userName, userPassword string) (*domain.User, error) {
	user := &domain.User{
		UserID:       uuid.NewString(),
		UserName:     userName,
		UserPassword: userPassword,
		Role:         auth_roles.Free,
	}
	if err := user.ValidateUser(); err != nil {
		return nil, fmt.Errorf("usecase register: %w", err)
	}

	hashed, err := password_hash.Hash(user.UserPassword)
	if err != nil {
		return nil, fmt.Errorf("usecase register: hash password: %w", err)
	}
	user.UserPassword = hashed

	payload, _ := json.Marshal(map[string]string{
		"user_id": user.UserID, "name": user.UserName, "role": user.Role.String(),
	})
	evt := events.Event{
		AggregationType: "user",
		AggregateID:     user.UserID,
		EventType:       "UserRegistered",
		PayLoad:         payload,
		IdempotencyKey:  uuid.NewString(),
	}

	err = uc.txm.Do(ctx, func(ctx context.Context) error {
		if err := uc.repo.SaveUser(ctx, user); err != nil {
			return err
		}
		return uc.events.Emit(ctx, evt)
	})
	if err != nil {
		uc.logger.LogError("register: save user", logging.Field{Key: "user_id", Value: user.UserID}, logging.Err(err))
		return nil, fmt.Errorf("usecase register: %w", err)
	}
	return user, nil
}

func (uc *UserUseCase) GetUser(ctx context.Context, userID string) (*domain.User, error) {
	user, err := uc.repo.GetByID(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("usecase get user: %w", err)
	}
	return user, nil
}

func (uc *UserUseCase) PlanChange(ctx context.Context, userID string, newPlan auth_roles.Plan) (*domain.User, error) {
	var updated *domain.User
	err := uc.txm.Do(ctx, func(ctx context.Context) error {
		user, err := uc.repo.GetByID(ctx, userID)
		if err != nil {
			return err
		}
		if err := uc.repo.UpdatePlan(ctx, userID, newPlan); err != nil {
			return err
		}
		user.Role = newPlan
		updated = user

		payload, _ := json.Marshal(map[string]string{"user_id": userID, "plan": newPlan.String()})
		return uc.events.Emit(ctx, events.Event{
			AggregationType: "user",
			AggregateID:     userID,
			EventType:       "UserPlanChanged",
			PayLoad:         payload,
			IdempotencyKey:  uuid.NewString(),
		})
	})
	if err != nil {
		return nil, fmt.Errorf("usecase plan change: %w", err)
	}
	return updated, nil
}
