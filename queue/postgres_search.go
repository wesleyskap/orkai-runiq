package queue

import (
	"context"
	"database/sql"
	"strconv"
	"time"
)

func buildPostgresSearchQuery(q, status string) (string, []interface{}) {
	where := "WHERE 1=1"
	var args []interface{}
	idx := 1
	if status != "" {
		where += " AND status = $" + strconv.Itoa(idx)
		args = append(args, status)
		idx++
	}
	if q != "" {
		where += " AND (job_id LIKE $" + strconv.Itoa(idx) +
			" OR name LIKE $" + strconv.Itoa(idx+1) +
			" OR trace_id LIKE $" + strconv.Itoa(idx+2) + ")"
		likePat := "%" + q + "%"
		args = append(args, likePat, likePat, likePat)
	}
	return where, args
}

func scanOnePgJobDetail(rows *sql.Rows) (JobDetail, error) {
	var jd JobDetail
	var createdAt time.Time
	var errMsg sql.NullString
	err := rows.Scan(&jd.JobID, &jd.Queue, &jd.Name, &jd.Status, &jd.TraceID, &errMsg, &createdAt)
	if err != nil {
		return jd, err
	}
	jd.ErrorMessage = errMsg.String
	jd.CreatedAt = createdAt.Format(time.RFC3339)
	return jd, nil
}

func scanPgJobDetails(rows *sql.Rows) ([]JobDetail, error) {
	var jobs []JobDetail
	var err error
	for rows.Next() && err == nil {
		var jd JobDetail
		jd, err = scanOnePgJobDetail(rows)
		jobs = append(jobs, jd)
	}
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

// GetJobs queries jobs with pagination and filters for Postgres.
func (p *PostgresStorage) GetJobs(ctx context.Context, q, status string, page, limit int) ([]JobDetail, int64, error) {
	limit, page = sanitizePageLimit(limit, page)
	offset := (page - 1) * limit
	where, args := buildPostgresSearchQuery(q, status)
	var total int64
	countQuery := "SELECT COUNT(*) FROM runiq_jobs " + where
	if err := p.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	selectQuery := "SELECT job_id, queue, name, status, trace_id, error_message, created_at FROM runiq_jobs " +
		where + " ORDER BY created_at DESC LIMIT $" + strconv.Itoa(len(args)+1) + " OFFSET $" + strconv.Itoa(len(args)+2)
	selectArgs := append(args, limit, offset)
	rows, err := p.db.QueryContext(ctx, selectQuery, selectArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	jobs, err := scanPgJobDetails(rows)
	return jobs, total, err
}

// BulkRetry retries multiple jobs for Postgres.
func (p *PostgresStorage) BulkRetry(ctx context.Context, jobIDs []string) error {
	if len(jobIDs) == 0 {
		return nil
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	query := `UPDATE runiq_jobs SET status = 'pending', attempts = 0, run_at = CURRENT_TIMESTAMP, error_message = '' WHERE job_id = $1`
	for _, id := range jobIDs {
		if _, err := tx.ExecContext(ctx, query, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (p *PostgresStorage) cancelOneJob(ctx context.Context, tx *sql.Tx, id string) error {
	var uniqueKey, queueName string
	err := tx.QueryRowContext(ctx, "SELECT unique_key, queue FROM runiq_jobs WHERE job_id = $1", id).Scan(&uniqueKey, &queueName)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM runiq_jobs WHERE job_id = $1", id); err != nil {
		return err
	}
	return p.deleteUniqueLock(ctx, tx, queueName, uniqueKey)
}

// BulkCancel cancels multiple jobs for Postgres.
func (p *PostgresStorage) BulkCancel(ctx context.Context, jobIDs []string) error {
	if len(jobIDs) == 0 {
		return nil
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, id := range jobIDs {
		if err := p.cancelOneJob(ctx, tx, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// BulkPurge deletes multiple jobs for Postgres.
func (p *PostgresStorage) BulkPurge(ctx context.Context, jobIDs []string) error {
	if len(jobIDs) == 0 {
		return nil
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	query := "DELETE FROM runiq_jobs WHERE job_id = $1"
	for _, id := range jobIDs {
		if _, err := tx.ExecContext(ctx, query, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}
