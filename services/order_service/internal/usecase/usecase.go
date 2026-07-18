package usecase

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	sagapb "BHLA/proto/saga_events"
	"BHLA/shared/events"
	"BHLA/shared/logging"
	"BHLA/shared/policy"
	"BHLA/shared/quota"
	"BHLA/shared/sagatopics"
	"BHLA/shared/txmanager"

	"BHLA/services/order-service/internal/domain"
	"BHLA/services/order-service/internal/ports"
)

const (
	defaultPageSize = 20
	maxPageSize     = 100
)

var _ ports.OrderInbound = (*UseCase)(nil)

type quotaChecker interface {
	Check(ctx context.Context, plan string, action policy.Action, subject string) (quota.Decision, error)
}

type UseCase struct {
	repo   ports.OrderRepo
	events events.Emitter
	txm    *txmanager.TxManager
	quota  quotaChecker
	logger logging.Logger
}

func New(repo ports.OrderRepo, emitter events.Emitter, txm *txmanager.TxManager, q quotaChecker, logger logging.Logger) *UseCase {
	return &UseCase{repo: repo, events: emitter, txm: txm, quota: q, logger: logger}
}

func (uc *UseCase) enforceOrderQuota(ctx context.Context, cmd domain.CreateOrderCmd) error {
	dec, err := uc.quota.Check(ctx, cmd.UserPlan, policy.ActionCreateOrder, cmd.UserID)
	if err != nil {
		uc.logger.LogError("order quota check (fail-open)", logging.Field{Key: "user_id", Value: cmd.UserID}, logging.Err(err))
		return nil
	}
	if !dec.Allowed {
		return domain.ErrOrderQuotaExceeded
	}
	return nil
}

func (uc *UseCase) CreateOrder(ctx context.Context, cmd domain.CreateOrderCmd) (*domain.Order, error) {
	if err := uc.enforceOrderQuota(ctx, cmd); err != nil {
		return nil, err
	}

	order, err := domain.NewOrder(cmd.UserID, cmd.MarketID, cmd.Price, cmd.Quantity)
	if err != nil {
		return nil, fmt.Errorf("usecase create order: %w", err)
	}

	payload, err := proto.Marshal(&sagapb.OrderCreated{
		OrderId:  order.OrderID,
		UserId:   order.UserID,
		MarketId: order.MarketID,
		Price:    order.Price.String(),
		Amount:   order.Amount.String(),
		Status:   string(order.OrderStatus),
	})
	if err != nil {
		return nil, fmt.Errorf("usecase create order: marshal event: %w", err)
	}

	evt := events.Event{
		AggregationType: "order",
		AggregateID:     order.OrderID,
		EventType:       sagatopics.EventOrderCreated,
		PayLoad:         payload,
		IdempotencyKey:  uuid.NewString(),
	}

	err = uc.txm.Do(ctx, func(ctx context.Context) error {
		if err := uc.repo.SaveOrder(ctx, order); err != nil {
			return err
		}
		return uc.events.Emit(ctx, evt)
	})
	if err != nil {
		uc.logger.LogError("create order failed", logging.Field{Key: "user_id", Value: order.UserID}, logging.Err(err))
		return nil, fmt.Errorf("usecase create order: %w", err)
	}

	uc.logger.LogInfo("order created", logging.Field{Key: "order_id", Value: order.OrderID})
	return order, nil
}

func (uc *UseCase) GetOrderStatusByID(ctx context.Context, orderID, userID string) (*domain.Order, error) {
	order, err := uc.repo.FindByID(ctx, orderID, userID)
	if err != nil {
		return nil, fmt.Errorf("usecase get order: %w", err)
	}
	return order, nil
}

func (uc *UseCase) GetOrderStatusAll(ctx context.Context, userID, pageToken string, pageSize int) ([]*domain.Order, string, error) {
	if pageSize <= 0 || pageSize > maxPageSize {
		pageSize = defaultPageSize
	}

	orders, err := uc.repo.FindAll(ctx, userID, pageToken, pageSize+1)
	if err != nil {
		return nil, "", fmt.Errorf("usecase get all orders: %w", err)
	}

	var next string
	if len(orders) > pageSize {
		next = orders[pageSize-1].OrderID
		orders = orders[:pageSize]
	}
	return orders, next, nil
}
