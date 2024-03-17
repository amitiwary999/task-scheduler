package task

import "time"

func FirstTask() int {
	time.Sleep(time.Duration(time.Second * 30))
	return 1
}
