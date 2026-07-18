package saga

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	sagapb "BHLA/proto/saga_events"
	"BHLA/shared/events"
	"BHLA/shared/idempotency"
	"BHLA/shared/kafka"
	"BHLA/shared/logging"
	"BHLA/shared/sagatopics"
	"BHLA/shared/txmanager"

	"BHLA/services/order-service/internal/domain"
)

type orderStatusRepo interface {
	UpdateStatus(ctx context.Context, orderID, status string) error
}

type Orchestrator struct {
	repo   orderStatusRepo
	tx     *txmanager.TxManager
	events events.Emitter
	idem   *idempotency.Guard
	logger logging.Logger
}

func NewOrchestrator(repo orderStatusRepo, tx *txmanager.TxManager, emitter events.Emitter,
	idem *idempotency.Guard, logger logging.Logger) *Orchestrator {
	return &Orchestrator{repo: repo, tx: tx, events: emitter, idem: idem, logger: logger}
}

func (o *Orchestrator) Handle(ctx context.Context, msg kafka.Message) error {
	switch msg.Header["event_type"] {
	case sagatopics.EventOrderCreated:
		return o.handleOrderCreated(ctx, msg)
	case sagatopics.EventStockReserved:
		return o.handleReply(ctx, msg, true)
	case sagatopics.EventStockRejected:
		return o.handleReply(ctx, msg, false)
	default:
		return nil
	}
}

func (o *Orchestrator) handleOrderCreated(ctx context.Context, msg kafka.Message) error {
	key := msg.Header["idempotency_key"]
	if key == "" {
		return fmt.Errorf("orchestrator: empty idempotency_key for OrderCreated")
	}

	var created sagapb.OrderCreated
	if err := proto.Unmarshal(msg.Value, &created); err != nil {
		o.logger.LogError("orchestrator: bad OrderCreated payload", logging.Err(err))
		return nil // ядовитое -> skip (не ретраим)
	}
	if err := created.ValidateAll(); err != nil {
		o.logger.LogError("orchestrator: invalid OrderCreated", logging.Err(err))
		return nil
	}

	return o.tx.Do(ctx, func(ctx context.Context) error {
		first, err := o.idem.Acquire(ctx, key)
		if err != nil {
			return err
		}
		if !first {
			o.logger.LogInfo("orchestrator: duplicate OrderCreated skipped", logging.Field{Key: "order_id", Value: created.OrderId})
			return nil
		}

		payload, err := proto.Marshal(&sagapb.ReserveStock{
			OrderId:  created.OrderId,
			UserId:   created.UserId,
			MarketId: created.MarketId,
			Amount:   created.Amount,
		})
		if err != nil {
			return fmt.Errorf("orchestrator: marshal ReserveStock: %w", err)
		}
		return o.events.Emit(ctx, events.Event{
			AggregationType: "order",
			AggregateID:     created.OrderId,
			EventType:       sagatopics.CommandReserveStock,
			PayLoad:         payload,
			IdempotencyKey:  uuid.NewString(),
		})
	})
}

func (o *Orchestrator) handleReply(ctx context.Context, msg kafka.Message, reserved bool) error {
	key := msg.Header["idempotency_key"]
	if key == "" {
		return fmt.Errorf("orchestrator: empty idempotency_key for reply")
	}

	orderID, ok := o.orderIDFromReply(msg, reserved)
	if !ok {
		return nil
	}

	newStatus := string(domain.StatusReserved)
	if !reserved {
		newStatus = string(domain.StatusRejected)
	}

	return o.tx.Do(ctx, func(ctx context.Context) error {
		first, err := o.idem.Acquire(ctx, key)
		if err != nil {
			return err
		}
		if !first {
			o.logger.LogInfo("orchestrator: duplicate reply skipped", logging.Field{Key: "order_id", Value: orderID})
			return nil
		}
		if err := o.repo.UpdateStatus(ctx, orderID, newStatus); err != nil {
			return err
		}
		payload, err := proto.Marshal(&sagapb.OrderStatusChanged{OrderId: orderID, Status: newStatus})
		if err != nil {
			return fmt.Errorf("orchestrator: marshal OrderStatusChanged: %w", err)
		}
		o.logger.LogInfo("orchestrator: order status updated",
			logging.Field{Key: "order_id", Value: orderID}, logging.Field{Key: "status", Value: newStatus})
		return o.events.Emit(ctx, events.Event{
			AggregationType: "order",
			AggregateID:     orderID,
			EventType:       sagatopics.EventOrderStatusChanged,
			PayLoad:         payload,
			IdempotencyKey:  uuid.NewString(),
		})
	})
}

func (o *Orchestrator) orderIDFromReply(msg kafka.Message, reserved bool) (string, bool) {
	if reserved {
		var r sagapb.StockReserved
		if err := proto.Unmarshal(msg.Value, &r); err != nil {
			o.logger.LogError("orchestrator: bad StockReserved payload", logging.Err(err))
			return "", false
		}
		if err := r.ValidateAll(); err != nil {
			o.logger.LogError("orchestrator: invalid StockReserved", logging.Err(err))
			return "", false
		}
		return r.OrderId, true
	}
	var r sagapb.StockRejected
	if err := proto.Unmarshal(msg.Value, &r); err != nil {
		o.logger.LogError("orchestrator: bad StockRejected payload", logging.Err(err))
		return "", false
	}
	if err := r.ValidateAll(); err != nil {
		o.logger.LogError("orchestrator: invalid StockRejected", logging.Err(err))
		return "", false
	}
	return r.OrderId, true
}
