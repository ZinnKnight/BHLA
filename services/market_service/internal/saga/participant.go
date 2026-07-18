package saga

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	sagapb "BHLA/proto/saga_events"
	"BHLA/shared/events"
	"BHLA/shared/idempotency"
	"BHLA/shared/kafka"
	"BHLA/shared/logging"
	"BHLA/shared/saga_topics"
	"BHLA/shared/tx_manager"

	"BHLA/services/market_service/internal/domain"
)

type reserveRepo interface {
	FindByID(ctx context.Context, marketID string) (*domain.Market, error)
	SaveReservation(ctx context.Context, orderID, marketID, status string) error
}

type Participant struct {
	repo   reserveRepo
	tx     *tx_manager.TxManager
	events events.Emitter
	idem   *idempotency.Guard
	logger logging.Logger
}

func NewParticipant(repo reserveRepo, tx *tx_manager.TxManager, emitter events.Emitter,
	idem *idempotency.Guard, logger logging.Logger) *Participant {
	return &Participant{repo: repo, tx: tx, events: emitter, idem: idem, logger: logger}
}

func (p *Participant) HandleReserveStock(ctx context.Context, msg kafka.Message) error {
	if msg.Header["event_type"] != saga_topics.CommandReserveStock {
		return nil
	}
	key := msg.Header["idempotency_key"]
	if key == "" {
		return fmt.Errorf("participant: empty idempotency_key")
	}

	var cmd sagapb.ReserveStock
	if err := proto.Unmarshal(msg.Value, &cmd); err != nil {
		p.logger.LogError("participant: bad ReserveStock payload", logging.Err(err))
		return nil // ядовитое -> skip
	}
	if err := cmd.ValidateAll(); err != nil {
		p.logger.LogError("participant: invalid ReserveStock", logging.Err(err))
		return nil
	}

	return p.tx.Do(ctx, func(ctx context.Context) error {
		first, err := p.idem.Acquire(ctx, key)
		if err != nil {
			return err
		}
		if !first {
			p.logger.LogInfo("participant: duplicate ReserveStock skipped", logging.Field{Key: "order_id", Value: cmd.OrderId})
			return nil
		}

		reserved, reason, err := p.reserve(ctx, cmd.MarketId)
		if err != nil {
			return err // инфра-ошибка -> откат+ретрай (не бизнес-отказ)
		}

		status := domain.ReservationReserved
		if !reserved {
			status = domain.ReservationRejected
		}
		if err := p.repo.SaveReservation(ctx, cmd.OrderId, cmd.MarketId, status); err != nil {
			return err
		}
		return p.emitReply(ctx, cmd.OrderId, cmd.MarketId, reserved, reason)
	})
}

func (p *Participant) reserve(ctx context.Context, marketID string) (bool, string, error) {
	market, err := p.repo.FindByID(ctx, marketID)
	if err != nil {
		if errors.Is(err, domain.ErrMarketNotFound) {
			return false, "market not found", nil
		}
		return false, "", err
	}
	if !market.Accessibility {
		return false, "market not accessible", nil
	}
	return true, "", nil
}

func (p *Participant) emitReply(ctx context.Context, orderID, marketID string, reserved bool, reason string) error {
	var (
		eventType string
		payload   []byte
		err       error
	)
	if reserved {
		eventType = saga_topics.EventStockReserved
		payload, err = proto.Marshal(&sagapb.StockReserved{OrderId: orderID, MarketId: marketID})
	} else {
		eventType = saga_topics.EventStockRejected
		payload, err = proto.Marshal(&sagapb.StockRejected{OrderId: orderID, MarketId: marketID, Reason: reason})
	}
	if err != nil {
		return fmt.Errorf("participant: marshal reply: %w", err)
	}
	return p.events.Emit(ctx, events.Event{
		AggregationType: "order",
		AggregateID:     orderID,
		EventType:       eventType,
		PayLoad:         payload,
		IdempotencyKey:  uuid.NewString(),
	})
}
