package storage

import (
	"context"
	"database/sql"
	"time"

	"github.com/amitiwary999/task-scheduler/model"
	util "github.com/amitiwary999/task-scheduler/util"
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
