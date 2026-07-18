package grpc_adapter

import (
	"context"

	marketpb "BHLA/proto/market_service"
	"BHLA/shared/logging"

	"BHLA/services/market_service/internal/ports"
)

type Handler struct {
	marketpb.UnimplementedSpotInstrumentServiceServer
	uc     ports.MarketInbound
	logger logging.Logger
}

func NewHandler(uc ports.MarketInbound, logger logging.Logger) *Handler {
	return &Handler{uc: uc, logger: logger}
}

func (h *Handler) ViewMarketsByID(ctx context.Context, req *marketpb.ViewMarketRequest) (*marketpb.ViewMarketResponse, error) {
	market, err := h.uc.ViewMarketByID(ctx, req.GetMarketId())
	if err != nil {
		h.logger.LogError("view market by id failed", logging.Field{Key: "market_id", Value: req.GetMarketId()}, logging.Err(err))
		return nil, err
	}
	return &marketpb.ViewMarketResponse{Market: marketToProto(market)}, nil
}

func (h *Handler) ViewAllMarkets(ctx context.Context, req *marketpb.ViewAllMarketsRequest) (*marketpb.ViewAllMarketsResponse, error) {
	markets, next, err := h.uc.ViewAllMarkets(ctx, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		h.logger.LogError("view all markets failed", logging.Err(err))
		return nil, err
	}
	out := make([]*marketpb.Market, 0, len(markets))
	for _, m := range markets {
		out = append(out, marketToProto(m))
	}
	return &marketpb.ViewAllMarketsResponse{Markets: out, NextPageToken: next}, nil
}
