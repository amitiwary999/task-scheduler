package storage

import (
	"context"
	"database/sql"
	"time"

	"github.com/amitiwary999/task-scheduler/model"
	util "github.com/amitiwary999/task-scheduler/util"
	"github.com/google/uuid"
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

func (db *PostgresDbClient) GetTaskConfig() (*[]model.TaskWeight, error) {
	query := `SELECT type, weight FROM jobconfig`
	ctx, cancel := context.WithTimeout(context.Background(), util.POSTGRES_QUERY_TIMEOUT*time.Second)
	defer cancel()
	rows, err := db.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	var taskWeights []model.TaskWeight
	for rows.Next() {
		var taskWeight model.TaskWeight
		rows.Scan(&taskWeight.Type, &taskWeight.Weight)
		taskWeights = append(taskWeights, taskWeight)
	}
	return &taskWeights, nil
}

func (db *PostgresDbClient) SaveTask(meta *model.TaskMeta) (string, error) {
	id := uuid.New().String()
	query := "INSERT INTO jobdetail(id, meta) VALUES($1, $2)"
	ctx, cancel := context.WithTimeout(context.Background(), util.POSTGRES_QUERY_TIMEOUT*time.Second)
	defer cancel()
	_, err := db.DB.ExecContext(ctx, query, id, meta)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (db *PostgresDbClient) UpdateTaskComplete(id string) error {
	query := "UPDATE jobdetail SET status = $1 WHERE id = $2"
	ctx, cancel := context.WithTimeout(context.Background(), util.POSTGRES_QUERY_TIMEOUT*time.Second)
	defer cancel()
	_, err := db.DB.ExecContext(ctx, query, "completed", id)
	return err
}

func (db *PostgresDbClient) GetPendingTask() (*[]model.PendingTask, error) {
	query := "SELECT id, meta FROM jobdetail WHERE status = $1"
	ctx, cancel := context.WithTimeout(context.Background(), util.POSTGRES_QUERY_TIMEOUT*time.Second)
	defer cancel()
	rows, err := db.DB.QueryContext(ctx, query, "pending")
	if err != nil {
		return nil, err
	}
	var pendingTasks []model.PendingTask
	for rows.Next() {
		var pendingTask model.PendingTask
		rows.Scan(&pendingTask.Id, &pendingTask.Meta)
		pendingTasks = append(pendingTasks, pendingTask)
	}
	return &pendingTasks, nil
}

func (db *PostgresDbClient) GetAllUsedServer() (*[]model.JoinData, error) {
	query := "SELECT serverId, status FROM jobservers WHERE status = 1"
	ctx, cancel := context.WithTimeout(context.Background(), util.POSTGRES_QUERY_TIMEOUT*time.Second)
	defer cancel()
	rows, err := db.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	var joinDatas []model.JoinData
	for rows.Next() {
		var joinData model.JoinData
		rows.Scan(&joinData.ServerId, &joinData.Status)
		joinDatas = append(joinDatas, joinData)
	}
	return &joinDatas, nil
}
