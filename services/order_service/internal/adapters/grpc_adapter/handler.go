package grpc_adapter

import (
	"context"

	orderpb "BHLA/proto/order_service"
	"BHLA/shared/auth_context"
	"BHLA/shared/grpc/interceptors/err_map"
	"BHLA/shared/logging"

	"BHLA/services/order_service/internal/domain"
	"BHLA/services/order_service/internal/ports"
	"BHLA/services/order_service/internal/streaming"
)

var errUnauthenticated = err_map.NewError(err_map.Unauthenticated, "требуется авторизация", nil)

type Handler struct {
	orderpb.UnimplementedOrderServiceServer
	uc     ports.OrderInbound
	hub    *streaming.Hub
	logger logging.Logger
}

func NewHandler(uc ports.OrderInbound, hub *streaming.Hub, logger logging.Logger) *Handler {
	return &Handler{uc: uc, hub: hub, logger: logger}
}

func (h *Handler) CreateOrder(ctx context.Context, req *orderpb.CreateOrderRequest) (*orderpb.CreateOrderResponse, error) {
	id, ok := auth_context.From(ctx)
	if !ok {
		return nil, errUnauthenticated
	}

	amount, err := decimalPBToDec(req.GetAmount())
	if err != nil {
		return nil, err_map.NewError(err_map.Invalid, "некорректное количество", err)
	}

	cmd := domain.CreateOrderCmd{
		UserID:   id.UserID,
		MarketID: req.GetMarketId(),
		Price:    moneyToDec(req.GetPrice()),
		Quantity: amount,
		UserPlan: id.Role,
	}
	order, err := h.uc.CreateOrder(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return &orderpb.CreateOrderResponse{Order: orderToProto(order)}, nil
}

func (h *Handler) GetOrderStatusByID(ctx context.Context, req *orderpb.OrderStatusByIDRequest) (*orderpb.OrderStatusByIDResponse, error) {
	id, ok := auth_context.From(ctx)
	if !ok {
		return nil, errUnauthenticated
	}
	order, err := h.uc.GetOrderStatusByID(ctx, req.GetOrderId(), id.UserID)
	if err != nil {
		return nil, err
	}
	return &orderpb.OrderStatusByIDResponse{Order: orderToProto(order)}, nil
}

func (h *Handler) GetOrderStatusAll(ctx context.Context, req *orderpb.OrderStatusAllRequest) (*orderpb.OrderStatusAllResponse, error) {
	id, ok := auth_context.From(ctx)
	if !ok {
		return nil, errUnauthenticated
	}
	orders, next, err := h.uc.GetOrderStatusAll(ctx, id.UserID, req.GetPageToken(), int(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	out := make([]*orderpb.Order, 0, len(orders))
	for _, o := range orders {
		out = append(out, orderToProto(o))
	}
	return &orderpb.OrderStatusAllResponse{Orders: out, NextPageToken: next}, nil
}

func (h *Handler) StreamOrderUpdates(req *orderpb.StreamOrderRequest, stream orderpb.OrderService_StreamOrderUpdatesServer) error {
	ctx := stream.Context()
	id, ok := auth_context.From(ctx)
	if !ok {
		return errUnauthenticated
	}
	orderID := req.GetOrderId()

	subID, updates := h.hub.Subscribe(orderID)
	defer h.hub.Unsubscribe(orderID, subID)

	var last domain.OrderStatus
	first := true

	send := func() (terminal bool, err error) {
		order, err := h.uc.GetOrderStatusByID(ctx, orderID, id.UserID)
		if err != nil {
			return false, err
		}
		if first || order.OrderStatus != last {
			if err := stream.Send(&orderpb.OrderStatusByIDResponse{Order: orderToProto(order)}); err != nil {
				return false, err
			}
			last = order.OrderStatus
			first = false
		}
		return order.OrderStatus.IsTerminal(), nil
	}

	if terminal, err := send(); err != nil || terminal {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-updates:
			if terminal, err := send(); err != nil || terminal {
				return err
			}
		}
	}
}
