Benchmark the task-manager

We Benchmark the task-manager and for that we by pass the rabbitmq call and database call. We use the sleep to replicate that some database operation is going on and then return the data.

go test -timeout 30m  -bench=.  -benchmem -benchtime=30s -count=20

RESULT:
goos: darwin
goarch: arm64
pkg: tskscheduler/test
BenchmarkTaskScheduler-10    	
 9620655	      3333 ns/op	     744 B/op	      18 allocs/op
BenchmarkTaskScheduler-10   
11825894	      2939 ns/op	     744 B/op	      18 allocs/op
BenchmarkTaskScheduler-10    	
12400381	      2946 ns/op	     744 B/op	      18 allocs/op
BenchmarkTaskScheduler-10    	
12198896	      2958 ns/op	     744 B/op	      18 allocs/op
BenchmarkTaskScheduler-10    	
12126535	      2974 ns/op	     744 B/op	      18 allocs/op
BenchmarkTaskScheduler-10    	
12218697	      2974 ns/op	     744 B/op	      18 allocs/op
BenchmarkTaskScheduler-10    	
12022581	      2964 ns/op	     744 B/op	      18 allocs/op
BenchmarkTaskScheduler-10    	
11420341	      2953 ns/op	     744 B/op	      18 allocs/op
BenchmarkTaskScheduler-10    	
12148280	      2983 ns/op	     744 B/op	      18 allocs/op
BenchmarkTaskScheduler-10    	
12197222	      2969 ns/op	     744 B/op	      18 allocs/op
BenchmarkTaskScheduler-10    	
12131200	      2971 ns/op	     744 B/op	      18 allocs/op
BenchmarkTaskScheduler-10    	
12250216	      2980 ns/op	     744 B/op	      18 allocs/op
BenchmarkTaskScheduler-10    	
12153454	      2974 ns/op	     744 B/op	      18 allocs/op
BenchmarkTaskScheduler-10    	
11963482	      3358 ns/op	     744 B/op	      18 allocs/op
BenchmarkTaskScheduler-10    	
10866364	      3090 ns/op	     744 B/op	      18 allocs/op
BenchmarkTaskScheduler-10    	
12100737	      2984 ns/op	     744 B/op	      18 allocs/op
BenchmarkTaskScheduler-10    	
12052598	      2977 ns/op	     744 B/op	      18 allocs/op
BenchmarkTaskScheduler-10    	
11999469	      2989 ns/op	     744 B/op	      18 allocs/op
BenchmarkTaskScheduler-10    	
12190858	      2973 ns/op	     744 B/op	      18 allocs/op
BenchmarkTaskScheduler-10    	
11988660	      2982 ns/op	     744 B/op	      18 allocs/op
PASS
ok  	tskscheduler/test	778.975s