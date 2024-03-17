package task

import "time"

func SecondTask() int {
	time.Sleep(time.Duration(time.Second * 30))
	return 2
}
