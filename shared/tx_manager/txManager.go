package tx_manager

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type txKey struct{}

const rollbackTimeout = 5 * time.Second

type TxManager struct {
	pool *pgxpool.Pool
}

func NewTxManager(pool *pgxpool.Pool) *TxManager {
	return &TxManager{pool: pool}
}

func (txm *TxManager) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	tx, err := txm.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("txmanager: begin tx: %w", err)
	}

	txCtx := context.WithValue(ctx, txKey{}, tx)

	defer func() {
		rbCtx, cancel := context.WithTimeout(context.Background(), rollbackTimeout)
		defer cancel()
		_ = tx.Rollback(rbCtx)
	}()

	if err := fn(txCtx); err != nil {
		return err
	}

	if err := tx.Commit(txCtx); err != nil {
		return fmt.Errorf("txmanager: commit tx: %w", err)
	}
	return nil
}

func ExtractManager(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(pgx.Tx)
	return tx, ok
}
