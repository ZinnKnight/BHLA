package grpc_adapter

import (
	marketpb "BHLA/proto/market_service"
	"BHLA/services/market-service/internal/domain"
)

func marketToProto(m *domain.Market) *marketpb.Market {
	var ttl int64
	if m.TTL != nil {
		ttl = m.TTL.Unix()
	}
	return &marketpb.Market{
		MarketName:          m.MarketName,
		GoodsId:             m.GoodsID,
		MarketId:            m.MarketID,
		MarketAccessibility: m.Accessibility,
		MarketTtl:           ttl,
	}
}
