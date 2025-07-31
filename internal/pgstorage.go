package internal

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

type PgStorage struct {
	DB *pgxpool.Pool
}

const (
	insertSql = `INSERT INTO payments(correlation_id, amount, processor_id, created_at) 
		VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`
	summarySql = `
		SELECT processor_id, COUNT(*), SUM(amount)
		FROM payments WHERE created_at BETWEEN $1 AND $2
		GROUP BY processor_id`
	deleteAllSql = `DELETE FROM payments WHERE processor_id in (0,1)`
)

func (s *PgStorage) Save(p ProcessedPayment) error {
	ctx := context.Background()
	conn, err := s.DB.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	_, err = conn.Conn().Exec(ctx, insertSql, p.CorrelationId, p.Amount, p.Processor, p.CreatedAt)
	if err != nil {
		return err
	}
	return nil
}

func (s *PgStorage) GetSummary(from, to time.Time) (Summary, error) {
	ctx := context.Background()
	conn, err := s.DB.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	var summary = Summary{
		Default.Name():  {},
		Fallback.Name(): {},
	}
	rows, err := conn.Conn().Query(ctx, summarySql, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid ProcessorId
		var ps PaymentSummary
		err = rows.Scan(&pid, &ps.TotalRequests, &ps.TotalAmount)
		if err != nil {
			return nil, err
		}
		summary[pid.Name()] = ps
	}

	return summary, nil
}

func (s *PgStorage) CleanUp() error {
	ctx := context.Background()
	conn, err := s.DB.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	_, err = conn.Conn().Exec(ctx, deleteAllSql)
	if err != nil {
		return err
	}
	return nil
}
