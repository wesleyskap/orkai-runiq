package queue

import (
	"context"
	"database/sql"
)

// EnqueueWorkflow transactionally inserts multiple dependent jobs.
// Usage example:
//	err := storage.EnqueueWorkflow(ctx, parentJob, childJob)
func (s *SqliteStorage) EnqueueWorkflow(ctx context.Context, jobs ...*JobEnvelope) error {
	if len(jobs) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.insertWorkflowJobs(ctx, tx, jobs); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SqliteStorage) insertWorkflowJobs(ctx context.Context, tx *sql.Tx, jobs []*JobEnvelope) error {
	for _, job := range jobs {
		if err := s.insertWorkflowJob(ctx, tx, job); err != nil {
			return err
		}
	}
	return nil
}

func (s *SqliteStorage) insertWorkflowJob(ctx context.Context, tx *sql.Tx, job *JobEnvelope) error {
	if err := s.acquireUniqueLock(ctx, tx, job); err != nil {
		return err
	}
	status := "pending"
	if len(job.Dependencies) > 0 {
		status = "blocked"
	}
	if err := s.execInsertJob(ctx, tx, job, status); err != nil {
		return err
	}
	return s.insertJobDependencies(ctx, tx, job)
}

func (s *SqliteStorage) execInsertJob(ctx context.Context, tx *sql.Tx, job *JobEnvelope, status string) error {
	runAt := getRunAt(job.RunAt)
	maxAttempts := getMaxAttempts(job.MaxAttempts)
	query := `INSERT INTO runiq_jobs (job_id, queue, name, args, trace_id, span_id, status, attempts, max_attempts, run_at, unique_key)
		VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
		ON CONFLICT (job_id) DO UPDATE SET
			status = ?, attempts = 0, max_attempts = ?, run_at = ?, unique_key = ?, updated_at = CURRENT_TIMESTAMP`
	_, err := tx.ExecContext(ctx, query,
		job.JobID, job.Queue, job.Name, job.Args,
		job.TraceContext.TraceID, job.TraceContext.SpanID,
		status, maxAttempts, runAt, job.UniqueKey,
		status, maxAttempts, runAt, job.UniqueKey,
	)
	return err
}

func (s *SqliteStorage) insertJobDependencies(ctx context.Context, tx *sql.Tx, job *JobEnvelope) error {
	for _, parentID := range job.Dependencies {
		query := `
			INSERT OR IGNORE INTO runiq_job_dependencies (job_id, parent_job_id)
			VALUES (?, ?)`
		if _, err := tx.ExecContext(ctx, query, job.JobID, parentID); err != nil {
			return err
		}
	}
	return nil
}

func queryChildJobIDs(ctx context.Context, tx *sql.Tx, parentJobID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, "SELECT job_id FROM runiq_job_dependencies WHERE parent_job_id = ?", parentJobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var childIDs []string
	for rows.Next() {
		var cid string
		if err := rows.Scan(&cid); err != nil {
			return nil, err
		}
		childIDs = append(childIDs, cid)
	}
	return childIDs, nil
}

func (s *SqliteStorage) cascadeDependencyFailure(ctx context.Context, tx *sql.Tx, failedJobID string) error {
	childIDs, err := queryChildJobIDs(ctx, tx, failedJobID)
	if err != nil {
		return err
	}
	return s.failChildren(ctx, tx, failedJobID, childIDs)
}

func (s *SqliteStorage) failChildren(ctx context.Context, tx *sql.Tx, parentID string, childIDs []string) error {
	for _, cid := range childIDs {
		var uniqueKey, queueName, batchID string
		err := tx.QueryRowContext(ctx, "SELECT unique_key, queue, batch_id FROM runiq_jobs WHERE job_id = ?", cid).Scan(&uniqueKey, &queueName, &batchID)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		errMsg := "Dependency " + parentID + " failed"
		if err := s.transitionToDead(ctx, tx, cid, errMsg, uniqueKey, queueName, batchID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM runiq_job_dependencies WHERE job_id = ?", cid); err != nil {
			return err
		}
	}
	return nil
}

func (s *SqliteStorage) resolveDependencies(ctx context.Context, tx *sql.Tx, parentJobID string) error {
	childIDs, err := queryChildJobIDs(ctx, tx, parentJobID)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM runiq_job_dependencies WHERE parent_job_id = ?", parentJobID); err != nil {
		return err
	}
	return s.checkBlockedChildren(ctx, tx, childIDs)
}

func (s *SqliteStorage) checkBlockedChildren(ctx context.Context, tx *sql.Tx, childIDs []string) error {
	for _, cid := range childIDs {
		var count int
		err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM runiq_job_dependencies WHERE job_id = ?", cid).Scan(&count)
		if err != nil {
			return err
		}
		if count == 0 {
			_, err = tx.ExecContext(ctx, "UPDATE runiq_jobs SET status = 'pending', updated_at = CURRENT_TIMESTAMP WHERE job_id = ?", cid)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
