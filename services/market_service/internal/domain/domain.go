package domain

import (
	"errors"
	"time"
)

type Market struct {
	MarketName    string
	GoodsID       string
	MarketID      string
	Accessibility bool
	TTL           *time.Time
}

const (
	ReservationReserved = "RESERVED"
	ReservationRejected = "REJECTED"
)

var ErrMarketNotFound = errors.New("market not found")
