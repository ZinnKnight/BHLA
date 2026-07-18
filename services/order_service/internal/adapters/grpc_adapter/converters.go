package grpc_adapter

import (
	"github.com/shopspring/decimal"
	decimalpb "google.golang.org/genproto/googleapis/type/decimal"
	moneypb "google.golang.org/genproto/googleapis/type/money"

	orderpb "BHLA/proto/order_service"
	"BHLA/services/order_service/internal/domain"
)

const currency = "USD"

const nanosPerUnit = 1_000_000_000

func statusToProto(s domain.OrderStatus) orderpb.OrderStatus {
	if v, ok := orderpb.OrderStatus_value[string(s)]; ok {
		return orderpb.OrderStatus(v)
	}
	return orderpb.OrderStatus_UNREGISTERED_STATUS
}

func orderToProto(o *domain.Order) *orderpb.Order {
	return &orderpb.Order{
		UserId:      o.UserID,
		OrderId:     o.OrderID,
		MarketId:    o.MarketID,
		Price:       decToMoney(o.Price),
		Amount:      decToDecimalPB(o.Amount),
		OrderStatus: statusToProto(o.OrderStatus),
		CreatedAt:   o.CreatedAt.Unix(),
	}
}

func moneyToDec(m *moneypb.Money) decimal.Decimal {
	if m == nil {
		return decimal.Zero
	}
	return decimal.NewFromInt(m.GetUnits()).Add(decimal.New(int64(m.GetNanos()), -9))
}

func decToMoney(d decimal.Decimal) *moneypb.Money {
	units := d.IntPart()
	frac := d.Sub(decimal.NewFromInt(units))
	nanos := int32(frac.Mul(decimal.NewFromInt(nanosPerUnit)).IntPart())
	return &moneypb.Money{CurrencyCode: currency, Units: units, Nanos: nanos}
}

func decimalPBToDec(d *decimalpb.Decimal) (decimal.Decimal, error) {
	if d == nil || d.GetValue() == "" {
		return decimal.Zero, nil
	}
	return decimal.NewFromString(d.GetValue())
}

func decToDecimalPB(d decimal.Decimal) *decimalpb.Decimal {
	return &decimalpb.Decimal{Value: d.String()}
}
