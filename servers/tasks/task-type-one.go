package task

import "time"

func FirstTask() int {
	time.Sleep(time.Duration(time.Minute * 3))
	return 1
}
