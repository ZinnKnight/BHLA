package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type OrderStatus string

const (
	StatusUnregistered OrderStatus = "UNREGISTERED_STATUS"
	StatusCreated      OrderStatus = "ORDER_CREATED"
	StatusReserved     OrderStatus = "ORDER_RESERVED"
	StatusRejected     OrderStatus = "ORDER_REJECTED"
	StatusInDelivery   OrderStatus = "ORDER_IN_PROCESS_OF_DELIVERY"
	StatusDelivered    OrderStatus = "ORDER_HAS_BEEN_DELIVERED"
)

func (s OrderStatus) IsTerminal() bool {
	return s == StatusDelivered || s == StatusRejected
}

type Order struct {
	UserID      string
	OrderID     string
	MarketID    string
	Price       decimal.Decimal
	Amount      decimal.Decimal
	OrderStatus OrderStatus
	CreatedAt   time.Time
}

type CreateOrderCmd struct {
	UserID   string
	MarketID string
	Price    decimal.Decimal
	Quantity decimal.Decimal
	UserPlan string
}

func NewOrder(userID, marketID string, price, amount decimal.Decimal) (*Order, error) {
	if userID == "" {
		return nil, ErrInvalidUserID
	}
	if marketID == "" {
		return nil, ErrInvalidMarketID
	}
	if amount.LessThanOrEqual(decimal.Zero) {
		return nil, ErrInvalidAmount
	}
	if price.LessThanOrEqual(decimal.Zero) {
		return nil, ErrInvalidPrice
	}
	return &Order{
		OrderID:     uuid.NewString(),
		UserID:      userID,
		MarketID:    marketID,
		Price:       price,
		Amount:      amount,
		OrderStatus: StatusCreated,
		CreatedAt:   time.Now(),
	}, nil
}

var (
	ErrOrderNotFound      = errors.New("order not found")
	ErrInvalidPrice       = errors.New("invalid price")
	ErrInvalidAmount      = errors.New("invalid amount")
	ErrInvalidMarketID    = errors.New("invalid market id")
	ErrInvalidUserID      = errors.New("invalid user id")
	ErrOrderAlreadyExists = errors.New("order already exists")
	ErrOrderQuotaExceeded = errors.New("order quota exceeded")
)
