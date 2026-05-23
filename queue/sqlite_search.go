package queue

import (
	"context"
	"database/sql"
	"time"
)

func buildSearchQuery(q, status string) (string, []interface{}) {
	where := "WHERE 1=1"
	var args []interface{}
	if status != "" {
		where += " AND status = ?"
		args = append(args, status)
	}
	if q != "" {
		where += " AND (job_id LIKE ? OR name LIKE ? OR trace_id LIKE ?)"
		likePat := "%" + q + "%"
		args = append(args, likePat, likePat, likePat)
	}
	return where, args
}

func scanOneJobDetail(rows *sql.Rows) (JobDetail, error) {
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

func scanJobDetails(rows *sql.Rows) ([]JobDetail, error) {
	var jobs []JobDetail
	var err error
	for rows.Next() && err == nil {
		var jd JobDetail
		jd, err = scanOneJobDetail(rows)
		jobs = append(jobs, jd)
	}
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func sanitizePageLimit(limit, page int) (int, int) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if page <= 0 {
		page = 1
	}
	return limit, page
}

// GetJobs queries jobs with pagination and filters.
func (s *SqliteStorage) GetJobs(ctx context.Context, q, status string, page, limit int) ([]JobDetail, int64, error) {
	limit, page = sanitizePageLimit(limit, page)
	offset := (page - 1) * limit
	where, args := buildSearchQuery(q, status)
	var total int64
	countQuery := "SELECT COUNT(*) FROM runiq_jobs " + where
	if err := s.db.QueryRowContext(ctx, s.q(countQuery), args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	selectQuery := "SELECT job_id, queue, name, status, trace_id, error_message, created_at FROM runiq_jobs " + where + " ORDER BY created_at DESC LIMIT ? OFFSET ?"
	selectArgs := append(args, limit, offset)
	rows, err := s.db.QueryContext(ctx, s.q(selectQuery), selectArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	jobs, err := scanJobDetails(rows)
	return jobs, total, err
}

// BulkRetry retries multiple jobs.
func (s *SqliteStorage) BulkRetry(ctx context.Context, jobIDs []string) error {
	if len(jobIDs) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	query := `UPDATE runiq_jobs SET status = 'pending', attempts = 0, run_at = CURRENT_TIMESTAMP, error_message = '' WHERE job_id = ?`
	for _, id := range jobIDs {
		if _, err := tx.ExecContext(ctx, s.q(query), id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *SqliteStorage) cancelOneJob(ctx context.Context, tx *sql.Tx, id string) error {
	var uniqueKey, queueName string
	err := tx.QueryRowContext(ctx, s.q("SELECT unique_key, queue FROM runiq_jobs WHERE job_id = ?"), id).Scan(&uniqueKey, &queueName)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if _, err := tx.ExecContext(ctx, s.q("DELETE FROM runiq_jobs WHERE job_id = ?"), id); err != nil {
		return err
	}
	return s.deleteUniqueLock(ctx, tx, queueName, uniqueKey)
}

// BulkCancel cancels multiple jobs.
func (s *SqliteStorage) BulkCancel(ctx context.Context, jobIDs []string) error {
	if len(jobIDs) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, id := range jobIDs {
		if err := s.cancelOneJob(ctx, tx, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// BulkPurge deletes multiple jobs.
func (s *SqliteStorage) BulkPurge(ctx context.Context, jobIDs []string) error {
	if len(jobIDs) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	query := "DELETE FROM runiq_jobs WHERE job_id = ?"
	for _, id := range jobIDs {
		if _, err := tx.ExecContext(ctx, s.q(query), id); err != nil {
			return err
		}
	}
	return tx.Commit()
}
