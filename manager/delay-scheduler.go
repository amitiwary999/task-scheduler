package manager

import (
	model "github.com/amitiwary999/task-scheduler/model"
)

type DelayTask struct {
	Task *model.Task
	Time int64
}

type PriorityQueue []*DelayTask

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Time < pq[j].Time
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(task interface{}) {
	*pq = append(*pq, task.(*DelayTask))
}

func (pq *PriorityQueue) Pop() interface{} {
	prev := *pq
	if len(prev) == 0 {
		return nil
	}
	task := prev[len(prev)-1]
	*pq = prev[0 : len(prev)-1]
	return task
}
