Hadoop automatic failover using golang

### Build

```bash
git clone https://github.com/antonchen/hadoop-fc.git
cd hadoop-fc
go build -o hadoop-fc main.go
```

### Running

```bash
nohup ./hadoop-fc > /dev/null 2>&1 &
```

### Help

```
Usage of ./hadoop-fc:
  -hadoopHome string
    	Hadoop home (default "/data/app/hadoop")
  -interval int
    	Check interval (default 2)
  -logFile string
    	Log file path (default "/data/logs/hadoop-fc/hadoop-fc.log")
  -outFile string
    	Last check time (default "/tmp/hadoop-fc-check.out")
  -pidFile string
    	PIDfile path (default "/data/pid/hadoop-fc.pid")
```
