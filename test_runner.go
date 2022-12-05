package main

import (
	"500lines_ci_by_go/helper"
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os/exec"
	"strings"
	"time"
)

type RunnerMgr struct {
	host             string // host
	dispatcherServer string
	repoFolder       string
}

func initRunnerMgr(host string, dispatcherServer string, repo string) *RunnerMgr {
	return &RunnerMgr{
		host:             host,
		dispatcherServer: dispatcherServer,
		repoFolder:       repo,
	}
}

func (runnerMgr *RunnerMgr) startServer() {
	l, err := net.Listen("tcp", runnerMgr.host)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		panic(err)
	}
	defer l.Close()
	fmt.Println("startTcpServer start")
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			panic(err)
		}
		fmt.Println("get new connection !")

		go runnerMgr.handleConnection(conn)
	}
	fmt.Println("startTcpServer end")
}

func (runnerMgr *RunnerMgr) handleConnection(conn net.Conn) {
	// defer关掉连接
	defer func() {
		conn.Close()
	}()
	for {
		// 读取固定长度， 比较简单操作
		buf := make([]byte, 1024)
		reqLen, err := conn.Read(buf)
		if err != nil {
			if err.Error() != "EOF" {
				fmt.Println("Error reading: ", err.Error())
			}
			return
		}
		// 读取指定长度的内容
		data := string(buf[0:reqLen])
		// 协议规定， 按照：拆分
		row := strings.Split(data, ":")
		// 指令名称
		command := row[0]
		if command == "ping" { // observer 探活调度器是否存在
			fmt.Println("in ping")
			conn.Write([]byte("pong"))
		} else if command == "runtest" { // runner 进行注册
			fmt.Println("runtest ing 1111")
			commitId := row[1]
			fmt.Println(commitId)
			// go 出去新的协程， 等待执行
			runnerMgr.runTests(commitId)
			// 发送回传
			conn.Write([]byte("OK"))
		}
	}
}

// 进行测试工作
func (runnerMgr *RunnerMgr) runTests(commitId string) {
	cmd := exec.Command("./test_runner_script.sh", runnerMgr.repoFolder, commitId)
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("cmd.Run() failed with %s, repoFolder:%s \n", err, runnerMgr.repoFolder)
		// 测试失败
	}
	// 测试成功
	fmt.Printf("test_runner_script commitId: %s, combined out:\n%s\n", commitId, string(out))

	// 回传给dispatcher
	output := string(out)
	outputLen := len(output)
	helper.Communicate(runnerMgr.dispatcherServer, fmt.Sprintf("results:%s:%d:%s", commitId, outputLen, output))
}

// 探活
func (runnerMgr *RunnerMgr) dispatcherChecker() {
	for {
		fmt.Println("ping----")
		resp, err := helper.Communicate(runnerMgr.dispatcherServer, "ping")
		if err != nil || resp != "pong" {
			fmt.Println("Dispatcher is no longer functional", err, resp)
		}
		fmt.Println(resp)
		time.Sleep(3 * time.Second)
	}
}

// 注册
func (runnerMgr *RunnerMgr) register() (err error) {
	_, err = helper.Communicate(runnerMgr.dispatcherServer, fmt.Sprintf("register:%s", runnerMgr.host))
	if err != nil {
		fmt.Println("register fail ", err.Error())
	}
	return
}

func main() {
	// 参数定义
	dispatcherServer := flag.String("dispatcher_server", "localhost:3333", "dispatcher host:port, by default it uses localhost:8888")
	repo := flag.String("repo", "", "path to the respository this will observe")
	// host名称
	host := flag.String("host", "localhost:8900", "runner's host, by default it uses localhost")
	// 解析参数
	flag.Parse()
	// 初始化runner
	runnerMgr := initRunnerMgr(*host, *dispatcherServer, *(repo))
	// 启动守护
	go runnerMgr.startServer()

	err := runnerMgr.register()
	if err != nil {
		return
	}
	// 启动checker
	go runnerMgr.dispatcherChecker()

	ctx, _ := context.WithCancel(context.TODO())
	// 守护进程， 优雅退出
	helper.GraceExit(ctx)

	fmt.Println("hello test_runner!")
}
