package queue

import (
	"context"
	"database/sql"
)

// EnqueueWorkflow transactionally inserts multiple dependent jobs.
func (p *PostgresStorage) EnqueueWorkflow(ctx context.Context, jobs ...*JobEnvelope) error {
	if len(jobs) == 0 {
		return nil
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := p.insertWorkflowJobs(ctx, tx, jobs); err != nil {
		return err
	}
	return tx.Commit()
}

func (p *PostgresStorage) insertWorkflowJobs(ctx context.Context, tx *sql.Tx, jobs []*JobEnvelope) error {
	for _, job := range jobs {
		if err := p.insertWorkflowJob(ctx, tx, job); err != nil {
			return err
		}
	}
	return nil
}

func (p *PostgresStorage) insertWorkflowJob(ctx context.Context, tx *sql.Tx, job *JobEnvelope) error {
	if err := p.acquireUniqueLock(ctx, tx, job); err != nil {
		return err
	}
	status := "pending"
	if len(job.Dependencies) > 0 {
		status = "blocked"
	}
	if err := p.execInsertJob(ctx, tx, job, status); err != nil {
		return err
	}
	return p.insertJobDependencies(ctx, tx, job)
}

func (p *PostgresStorage) execInsertJob(ctx context.Context, tx *sql.Tx, job *JobEnvelope, status string) error {
	runAt := getRunAt(job.RunAt)
	maxAttempts := getMaxAttempts(job.MaxAttempts)
	query := `INSERT INTO runiq_jobs (job_id, queue, name, args, trace_id, span_id, status, attempts, max_attempts, run_at, unique_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 0, $8, $9, $10)
		ON CONFLICT (job_id) DO UPDATE SET
			status = $7, attempts = 0, max_attempts = $8, run_at = $9, unique_key = $10, updated_at = CURRENT_TIMESTAMP`
	_, err := tx.ExecContext(ctx, p.q(query),
		job.JobID, job.Queue, job.Name, job.Args,
		job.TraceContext.TraceID, job.TraceContext.SpanID,
		status, maxAttempts, runAt, job.UniqueKey,
	)
	return err
}

func (p *PostgresStorage) insertJobDependencies(ctx context.Context, tx *sql.Tx, job *JobEnvelope) error {
	for _, parentID := range job.Dependencies {
		query := `
			INSERT INTO runiq_job_dependencies (job_id, parent_job_id)
			VALUES ($1, $2)
			ON CONFLICT (job_id, parent_job_id) DO NOTHING`
		if _, err := tx.ExecContext(ctx, p.q(query), job.JobID, parentID); err != nil {
			return err
		}
	}
	return nil
}

func (p *PostgresStorage) queryChildJobIDs(ctx context.Context, tx *sql.Tx, parentJobID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, p.q("SELECT job_id FROM runiq_job_dependencies WHERE parent_job_id = $1"), parentJobID)
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

func (p *PostgresStorage) cascadeDependencyFailure(ctx context.Context, tx *sql.Tx, failedJobID string) error {
	childIDs, err := p.queryChildJobIDs(ctx, tx, failedJobID)
	if err != nil {
		return err
	}
	return p.failChildren(ctx, tx, failedJobID, childIDs)
}

func (p *PostgresStorage) failChildren(ctx context.Context, tx *sql.Tx, parentID string, childIDs []string) error {
	for _, cid := range childIDs {
		var uniqueKey, queueName, batchID string
		err := tx.QueryRowContext(ctx, p.q("SELECT unique_key, queue, batch_id FROM runiq_jobs WHERE job_id = $1"), cid).Scan(&uniqueKey, &queueName, &batchID)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		errMsg := "Dependency " + parentID + " failed"
		if err := p.transitionToDead(ctx, tx, cid, errMsg, uniqueKey, queueName, batchID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, p.q("DELETE FROM runiq_job_dependencies WHERE job_id = $1"), cid); err != nil {
			return err
		}
	}
	return nil
}

func (p *PostgresStorage) resolveDependencies(ctx context.Context, tx *sql.Tx, parentJobID string) error {
	childIDs, err := p.queryChildJobIDs(ctx, tx, parentJobID)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, p.q("DELETE FROM runiq_job_dependencies WHERE parent_job_id = $1"), parentJobID); err != nil {
		return err
	}
	return p.checkBlockedChildren(ctx, tx, childIDs)
}

func (p *PostgresStorage) checkBlockedChildren(ctx context.Context, tx *sql.Tx, childIDs []string) error {
	for _, cid := range childIDs {
		var count int
		err := tx.QueryRowContext(ctx, p.q("SELECT COUNT(*) FROM runiq_job_dependencies WHERE job_id = $1"), cid).Scan(&count)
		if err != nil {
			return err
		}
		if count == 0 {
			_, err = tx.ExecContext(ctx, p.q("UPDATE runiq_jobs SET status = 'pending', updated_at = CURRENT_TIMESTAMP WHERE job_id = $1"), cid)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
