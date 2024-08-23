package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/amitiwary999/task-scheduler/model"
	util "github.com/amitiwary999/task-scheduler/util"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type PostgresDbClient struct {
	db       *sql.DB
	jobTable string
}

func NewPostgresClient(connectionUrl string, poolLimit int16, jobTable string) (*PostgresDbClient, error) {
	db, err := sql.Open("postgres", connectionUrl)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(int(poolLimit))
	db.SetMaxIdleConns(int(poolLimit))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = db.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	return &PostgresDbClient{
		db:       db,
		jobTable: jobTable,
	}, nil
}

func (pgdb *PostgresDbClient) CreateJobTable() error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s
	(
		id             varchar(50)                           NOT NULL PRIMARY KEY,
	    meta           JSONB                                 NOT NULL,
		retryCount     INTEGER     DEFAULT 3                 NOT NULL,
		status         varchar(20) DEFAULT 'pending'         NOT NULL,
		created_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
	);
	CREATE INDEX IF NOT EXISTS "%s_status_idx" ON %s (status);
	CREATE INDEX IF NOT EXISTS "%s_created_at_idx" ON %s (created_at);
	`, pgdb.jobTable, pgdb.jobTable, pgdb.jobTable, pgdb.jobTable, pgdb.jobTable)
	ctx, cancel := context.WithTimeout(context.Background(), util.POSTGRES_QUERY_TIMEOUT*time.Second)
	defer cancel()
	_, err := pgdb.db.QueryContext(ctx, query)
	fmt.Printf("query %v \n", query)
	return err
}

func (pgdb *PostgresDbClient) SaveTask(meta *model.TaskMeta) (string, error) {
	id := uuid.New().String()
	metaB, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	query := "INSERT INTO jobdetail(id, meta) VALUES($1, $2)"
	ctx, cancel := context.WithTimeout(context.Background(), util.POSTGRES_QUERY_TIMEOUT*time.Second)
	defer cancel()
	_, err = pgdb.db.ExecContext(ctx, query, id, metaB)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (pgdb *PostgresDbClient) UpdateTaskStatus(id, status string) error {
	query := "UPDATE jobdetail SET status = $1 WHERE id = $2"
	ctx, cancel := context.WithTimeout(context.Background(), util.POSTGRES_QUERY_TIMEOUT*time.Second)
	defer cancel()
	_, err := pgdb.db.ExecContext(ctx, query, status, id)
	return err
}

func (pgdb *PostgresDbClient) GetPendingTask() ([]model.PendingTask, error) {
	query := "SELECT id, meta FROM jobdetail WHERE status = $1"
	ctx, cancel := context.WithTimeout(context.Background(), util.POSTGRES_QUERY_TIMEOUT*time.Second)
	defer cancel()
	rows, err := pgdb.db.QueryContext(ctx, query, util.JOB_DETAIL_STATUS_PENDING)
	if err != nil {
		return nil, err
	}
	var pendingTasks []model.PendingTask
	for rows.Next() {
		var pendingTask model.PendingTask
		rows.Scan(&pendingTask.Id, &pendingTask.Meta)
		pendingTasks = append(pendingTasks, pendingTask)
	}
	return pendingTasks, nil
}

func (pgdb *PostgresDbClient) GetFailTask() ([]model.PendingTask, error) {
	query := "UPDATE jobdetail SET status = $1 WHERE status = $2 ORDER BY created_at ASC LIMIT 100 RETURNING id,meta"
	ctx, cancel := context.WithTimeout(context.Background(), util.POSTGRES_QUERY_TIMEOUT*time.Second)
	defer cancel()
	rows, err := pgdb.db.QueryContext(ctx, query, util.JOB_DETAIL_STATUS_PENDING, util.JOB_DETAIL_STATUS_FAILED)
	if err != nil {
		return nil, err
	}
	var pendingTasks []model.PendingTask
	for rows.Next() {
		var pendingTask model.PendingTask
		rows.Scan(&pendingTask.Id, &pendingTask.Meta)
		pendingTasks = append(pendingTasks, pendingTask)
	}
	return pendingTasks, nil
}
