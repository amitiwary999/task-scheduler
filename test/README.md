Benchmark the task-manager

We Benchmark the task-manager and for that we by pass the rabbitmq call and database call. We use the sleep to replicate that some database operation is going on and then return the data.

go test -timeout 30m  -bench=.  -benchmem -benchtime=40s -count=20

RESULT:
goos: darwin
goarch: arm64
pkg: tskscheduler/test
BenchmarkTaskScheduler-10    	 9109641	      5268 ns/op	    1651 B/op	      34 allocs/op
BenchmarkTaskScheduler-10    	 9113974	      5255 ns/op	    1651 B/op	      34 allocs/op
BenchmarkTaskScheduler-10    	 9115160	      5286 ns/op	    1651 B/op	      34 allocs/op
BenchmarkTaskScheduler-10    	 9112540	      5267 ns/op	    1651 B/op	      34 allocs/op
BenchmarkTaskScheduler-10    	 9032022	      5332 ns/op	    1651 B/op	      34 allocs/op
BenchmarkTaskScheduler-10    	 8468679	      5492 ns/op	    1649 B/op	      34 allocs/op
BenchmarkTaskScheduler-10    	 9156434	      5254 ns/op	    1651 B/op	      34 allocs/op
BenchmarkTaskScheduler-10    	 9181759	      5252 ns/op	    1651 B/op	      34 allocs/op
BenchmarkTaskScheduler-10    	 9132068	      5244 ns/op	    1651 B/op	      34 allocs/op
BenchmarkTaskScheduler-10    	 9005600	      5249 ns/op	    1651 B/op	      34 allocs/op
BenchmarkTaskScheduler-10    	 9156337	      5226 ns/op	    1651 B/op	      34 allocs/op
BenchmarkTaskScheduler-10    	 9076789	      5265 ns/op	    1651 B/op	      34 allocs/op
BenchmarkTaskScheduler-10    	 9175722	      5245 ns/op	    1651 B/op	      34 allocs/op
BenchmarkTaskScheduler-10    	 9200334	      5264 ns/op	    1651 B/op	      34 allocs/op
BenchmarkTaskScheduler-10    	 9181983	      5262 ns/op	    1651 B/op	      34 allocs/op
BenchmarkTaskScheduler-10    	 9231900	      5255 ns/op	    1651 B/op	      34 allocs/op
BenchmarkTaskScheduler-10    	 8622510	      5526 ns/op	    1649 B/op	      34 allocs/op
BenchmarkTaskScheduler-10    	 8859440	      5218 ns/op	    1651 B/op	      34 allocs/op
BenchmarkTaskScheduler-10    	 9193017	      5224 ns/op	    1650 B/op	      34 allocs/op
BenchmarkTaskScheduler-10    	 9224497	      5232 ns/op	    1650 B/op	      34 allocs/op
PASS
ok  	tskscheduler/test	1065.383s