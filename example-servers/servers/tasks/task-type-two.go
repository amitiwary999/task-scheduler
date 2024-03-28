package task

import "time"

func SecondTask() int {
	time.Sleep(time.Duration(time.Second * 60))
	return 2
}
