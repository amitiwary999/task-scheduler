package task

import "time"

func SecondTask() int {
	time.Sleep(time.Duration(time.Minute * 3))
	return 2
}
