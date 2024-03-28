package task

import "time"

func FirstTask() int {
	time.Sleep(time.Duration(time.Second * 60))
	return 1
}
