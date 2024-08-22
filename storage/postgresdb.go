package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/amitiwary999/task-scheduler/model"
	util "github.com/amitiwary999/task-scheduler/util"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type PostgresDbClient struct {
	DB *sql.DB
}

func NewPostgresClient(connectionUrl string, poolLimit int16) (*PostgresDbClient, error) {
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
		DB: db,
	}, nil
}

func (db *PostgresDbClient) SaveTask(meta *model.TaskMeta) (string, error) {
	id := uuid.New().String()
	metaB, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	query := "INSERT INTO jobdetail(id, meta) VALUES($1, $2)"
	ctx, cancel := context.WithTimeout(context.Background(), util.POSTGRES_QUERY_TIMEOUT*time.Second)
	defer cancel()
	_, err = db.DB.ExecContext(ctx, query, id, metaB)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (db *PostgresDbClient) UpdateTaskStatus(id, status string) error {
	query := "UPDATE jobdetail SET status = $1 WHERE id = $2"
	ctx, cancel := context.WithTimeout(context.Background(), util.POSTGRES_QUERY_TIMEOUT*time.Second)
	defer cancel()
	_, err := db.DB.ExecContext(ctx, query, status, id)
	return err
}

func (db *PostgresDbClient) GetPendingTask() ([]model.PendingTask, error) {
	query := "SELECT id, meta FROM jobdetail WHERE status = $1"
	ctx, cancel := context.WithTimeout(context.Background(), util.POSTGRES_QUERY_TIMEOUT*time.Second)
	defer cancel()
	rows, err := db.DB.QueryContext(ctx, query, util.JOB_DETAIL_STATUS_PENDING)
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

func (db *PostgresDbClient) GetFailTask() ([]model.PendingTask, error) {
	query := "UPDATE jobdetail SET status = $1 WHERE status = $2 ORDER BY created_at ASC LIMIT 100 RETURNING id,meta"
	ctx, cancel := context.WithTimeout(context.Background(), util.POSTGRES_QUERY_TIMEOUT*time.Second)
	defer cancel()
	rows, err := db.DB.QueryContext(ctx, query, util.JOB_DETAIL_STATUS_PENDING, util.JOB_DETAIL_STATUS_FAILED)
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
