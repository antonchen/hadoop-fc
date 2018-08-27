package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	"./conf"
	zk "github.com/samuel/go-zookeeper/zk"
)

const (
	activeHex  string = "\x00\x00\x00\x4c\x1a\x08\x02\x10\x00\x18\x05\x22\x10\x43\x32\x79\x7b\xde\x66\x40\xe9\x8e\x96\xa6\x57\x70\xc4\x46\x94\x28\x01\x30\x12\x06\x0a\x04\x72\x6f\x6f\x74\x1a\x26\x6f\x72\x67\x2e\x61\x70\x61\x63\x68\x65\x2e\x68\x61\x64\x6f\x6f\x70\x2e\x68\x61\x2e\x48\x41\x53\x65\x72\x76\x69\x63\x65\x50\x72\x6f\x74\x6f\x63\x6f\x6c\x00\x00\x00\x5f\x1a\x08\x02\x10\x00\x18\x00\x22\x10\x43\x32\x79\x7b\xde\x66\x40\xe9\x8e\x96\xa6\x57\x70\xc4\x46\x94\x28\x00\x3e\x0a\x12\x74\x72\x61\x6e\x73\x69\x74\x69\x6f\x6e\x54\x6f\x41\x63\x74\x69\x76\x65\x12\x26\x6f\x72\x67\x2e\x61\x70\x61\x63\x68\x65\x2e\x68\x61\x64\x6f\x6f\x70\x2e\x68\x61\x2e\x48\x41\x53\x65\x72\x76\x69\x63\x65\x50\x72\x6f\x74\x6f\x63\x6f\x6c\x18\x01\x04\x0a\x02\x08\x01"
	standbyHex string = "\x00\x00\x00\x4c\x1a\x08\x02\x10\x00\x18\x05\x22\x10\x86\x57\xec\xe7\xba\xe6\x4a\xe2\x9d\xb4\x65\x42\x63\xae\xad\x76\x28\x01\x30\x12\x06\x0a\x04\x72\x6f\x6f\x74\x1a\x26\x6f\x72\x67\x2e\x61\x70\x61\x63\x68\x65\x2e\x68\x61\x64\x6f\x6f\x70\x2e\x68\x61\x2e\x48\x41\x53\x65\x72\x76\x69\x63\x65\x50\x72\x6f\x74\x6f\x63\x6f\x6c\x00\x00\x00\x60\x1a\x08\x02\x10\x00\x18\x00\x22\x10\x86\x57\xec\xe7\xba\xe6\x4a\xe2\x9d\xb4\x65\x42\x63\xae\xad\x76\x28\x00\x3f\x0a\x13\x74\x72\x61\x6e\x73\x69\x74\x69\x6f\x6e\x54\x6f\x53\x74\x61\x6e\x64\x62\x79\x12\x26\x6f\x72\x67\x2e\x61\x70\x61\x63\x68\x65\x2e\x68\x61\x64\x6f\x6f\x70\x2e\x68\x61\x2e\x48\x41\x53\x65\x72\x76\x69\x63\x65\x50\x72\x6f\x74\x6f\x63\x6f\x6c\x18\x01\x04\x0a\x02\x08\x01"
	getHex     string = "\x00\x00\x00\x4c\x1a\x08\x02\x10\x00\x18\x05\x22\x10\xa0\xe4\x32\xfa\xa4\xc3\x49\xda\x94\x4c\x5f\x22\xf3\xd4\x84\xa8\x28\x01\x30\x12\x06\x0a\x04\x72\x6f\x6f\x74\x1a\x26\x6f\x72\x67\x2e\x61\x70\x61\x63\x68\x65\x2e\x68\x61\x64\x6f\x6f\x70\x2e\x68\x61\x2e\x48\x41\x53\x65\x72\x76\x69\x63\x65\x50\x72\x6f\x74\x6f\x63\x6f\x6c\x00\x00\x00\x59\x1a\x08\x02\x10\x00\x18\x00\x22\x10\xa0\xe4\x32\xfa\xa4\xc3\x49\xda\x94\x4c\x5f\x22\xf3\xd4\x84\xa8\x28\x00\x3c\x0a\x10\x67\x65\x74\x53\x65\x72\x76\x69\x63\x65\x53\x74\x61\x74\x75\x73\x12\x26\x6f\x72\x67\x2e\x61\x70\x61\x63\x68\x65\x2e\x68\x61\x64\x6f\x6f\x70\x2e\x68\x61\x2e\x48\x41\x53\x65\x72\x76\x69\x63\x65\x50\x72\x6f\x74\x6f\x63\x6f\x6c\x18\x01\x00"
)

func getZKConn(zkList []string) (conn *zk.Conn) {
	conn, _, err := zk.Connect(zkList, 10*time.Second)
	checkError(err)
	return
}

func setActiveToZK(conn *zk.Conn, clusterName string, activeData string) error {
	activePath := "/hadoop-ha/" + clusterName + "/ActiveBreadCrumb"
	data := []byte(activeData)

	_, zNodeStat, err := conn.Get(activePath)
	if err != nil {
		return err
	}

	zkPathStat, _, err := conn.Exists(activePath)
	if err != nil {
		return err
	}

	if zkPathStat == true {
		_, err = conn.Set(activePath, data, zNodeStat.Version)
	} else {
		_, err = conn.Create(activePath, data, 0, zk.WorldACL(zk.PermAll))
	}
	if err != nil {
		return err
	}

	return nil
}

func sendRPC(addr string, Data []byte) (result []byte, err error) {
	// 捕获异常不让程序退出
	defer func() {
		if err := recover(); err != nil {
			log.Printf("Caught error: %s\n", err)
		}
	}()

	const RPCHEAD string = "\x68\x72\x70\x63\x09\x00\x00"
	var buf = make([]byte, 1024)

	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// 发送 RPC 头
	_, err = conn.Write([]byte(RPCHEAD))
	if err != nil {
		return nil, err
	}
	// 发送请求
	_, err = conn.Write(Data)
	if err != nil {
		return nil, err
	}

	// 读取返回
	reqLen, err := conn.Read(buf)
	return buf[:reqLen-1], nil
}

func checkActive(addr string, Data []byte) (active bool, err error) {
	result, err := sendRPC(addr, Data)
	if err != nil {
		return active, errors.New("Get Namenode status error")
	}
	activeStatus := string(result[len(result)-2 : len(result)-1])
	if activeStatus == "\x01" {
		active = true
	}

	log.Printf("Node: %s active is %t, Hex: %x\n", addr, active, activeStatus)

	return active, nil
}

func openFile(path string, flag int) (fileObj *os.File, err error) {
	logDir := strings.Join(strings.Split(path, "/")[0:len(strings.Split(path, "/"))-1], "/")
	_, err = os.Stat(logDir)
	if err != nil {
		err := os.Mkdir(logDir, os.ModePerm)
		if err != nil {
			return nil, err
		}
		err = nil
	}
	fileObj, err = os.OpenFile(path, flag, 0644)
	return
}

func checkError(err error) {
	if err != nil {
		log.Fatalf("Error: %s\n", err.Error())
	}
}

func main() {
	var (
		hadoopHome = flag.String("hadoopHome", "/data/app/hadoop", "Hadoop home")
		interval   = flag.Int("interval", 2, "Check interval")
		pidFile    = flag.String("pidFile", "/data/pid/hadoop-fc.pid", "PIDfile path")
		logFile    = flag.String("logFile", "/data/logs/hadoop-fc/hadoop-fc.log", "Log file path")
		outFile    = flag.String("outFile", "/tmp/hadoop-fc-check.out", "Last check time")
	)

	flag.Parse()

	logFileObj, err := openFile(*logFile, os.O_CREATE|os.O_RDWR|os.O_APPEND)
	checkError(err)
	defer logFileObj.Close()
	log.SetOutput(logFileObj)

	pidFileObj, err := openFile(*pidFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC)
	checkError(err)
	_, err = pidFileObj.WriteString(fmt.Sprintf("%d\n", os.Getpid()))
	checkError(err)
	pidFileObj.Close()

	hadoopConfDir := *hadoopHome + "/etc/hadoop"
	_, err = os.Stat(hadoopConfDir)
	checkError(err)
	hadoopConfData, err := conf.Load(hadoopConfDir)
	checkError(err)

	clusterName := conf.GetClusterName(hadoopConfData)
	NameNodes := conf.GetNameNodes(hadoopConfData)

	zkList := conf.GetZookeeper(hadoopConfData)
	zkConn := getZKConn(zkList)
	defer zkConn.Close()

Loop:
	for {
		onlyOneActive := false
		needElection := true

		goodNNs := make(map[string]string)
		statNNs := make(map[string]bool)

		// 写入本次时间
		outFileObj, _ := openFile(*outFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC)
		outFileObj.WriteString(fmt.Sprintf("%d\n", time.Now().Unix()))
		outFileObj.Close()

		// 获取存活节点状态
		for alias, addr := range NameNodes {
			nnStat, err := checkActive(addr, []byte(getHex))
			if err == nil {
				goodNNs[alias] = addr
				statNNs[alias] = nnStat
			}
		}

		// 判断是否有且仅有一个 active
		for _, v := range statNNs {
			if v == true && onlyOneActive == false {
				onlyOneActive = true
				needElection = false
			} else if v == true && onlyOneActive == true {
				needElection = true
			}
		}

		// 需要选举一个新 active
		if needElection == true {
			var (
				newActiveAlias string
				newActiveNode  string
			)

			log.Println("Need to elect new active NameNode.")
			// 随机选取一个存活 NameNode 作为新的 active
			rand.Seed(time.Now().Unix())
			nodeIndex := rand.Intn(len(goodNNs))
			getNNi := 0
			for k, v := range goodNNs {
				if nodeIndex == getNNi {
					newActiveAlias = k
					newActiveNode = v
				}
				getNNi++
			}

			log.Printf("New active NameNode is %s\n", newActiveNode)

			// 从可用节点中删除新 Active
			delete(goodNNs, newActiveAlias)
			// 标记其它节点为 standby
			for _, v := range goodNNs {
				log.Printf("Make NameNode at %s standby.\n", v)
				_, err := sendRPC(v, []byte(standbyHex))
				if err != nil {
					log.Printf("Make %s standby miss, Error: %s\n", v, err.Error())
				}
			}

			// 标记 Active
			log.Printf("Make NameNode at %s active.\n", newActiveNode)
			_, err := sendRPC(newActiveNode, []byte(activeHex))
			if err != nil {
				log.Printf("Make %s active miss, Error: %s\n", newActiveNode, err.Error())
				continue Loop
			}
			time.Sleep(time.Duration(300) * time.Millisecond)

			// 更新 active 到 zookeeper
			log.Println("Set new active to zookeeper.")
			zkActiveData := "\x0a\x0a" + clusterName + "\x12\x03" + newActiveAlias + "\x1a\x0f" + strings.Split(newActiveNode, ":")[0] + "\x20\xa8\x46\x28\xd3\x3e"
			err = setActiveToZK(zkConn, clusterName, zkActiveData)
			if err != nil {
				log.Printf("Set zookeeper error: %s\n", err.Error())
			}
		}

		log.Printf("Check end.\n\n")
		time.Sleep(time.Duration(*interval) * time.Second)
	}
}
