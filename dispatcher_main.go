package main

import (
	"500lines_ci_by_go/helper"
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

var currentRunner []string
var currentCommitMap map[string]bool = make(map[string]bool)
var dispatchedCommits = make(map[string]string)
var pendingCommits = []string{}

// 管理分发器
type DispatcherMgr struct {
	allRunner        map[string]bool     // 全部runner
	commitBindRunner map[string]string   // commit和runner的关系绑定 1:1
	runnerBindCommit map[string][]string // runner 和commit的关系绑定 1:n
	queueCommits     chan string
	// 锁机制
	rwlock *sync.RWMutex

	addr string
}

// 初始化dispatcher管理器
func initDispatcherMgr() *DispatcherMgr {
	return &DispatcherMgr{
		allRunner:        make(map[string]bool),
		commitBindRunner: make(map[string]string),
		runnerBindCommit: make(map[string][]string),
		queueCommits:     make(chan string, 100), // 固定长度
		rwlock:           &sync.RWMutex{},
		addr:             "localhost:3333",
	}
}

// 启动server, startServer能力
func (dispatcherMgr *DispatcherMgr) startServer() {
	l, err := net.Listen("tcp", dispatcherMgr.addr)
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

		go dispatcherMgr.handleConnection(conn)
	}
	fmt.Println("startTcpServer end")
}

// 调度方案
func (dispatcherMgr *DispatcherMgr) dispatch() {
	for {
		select {
		case commitId := <-dispatcherMgr.queueCommits:
			// 找一个空闲的runner
			runner := dispatcherMgr.gerRandomRunner()
			// 然后发送调度请求
			resp, err := helper.Communicate(runner, fmt.Sprintf("runtest:%s", commitId))
			if err != nil || resp != "OK" {
				if err != nil {
					fmt.Println("dispatch fail！ ", err.Error(), resp)
				}
				// 失败， 那么重新将commitId, 提交到队列中， 且sleep1秒
				dispatcherMgr.pushCommit(commitId)
				time.Sleep(3 * time.Second)
			} else {
				// 建立两者之间的绑定
				dispatcherMgr.bindRunner(commitId, runner)
			}
		}
	}
}

// 检查runner是否存在
func (dispatcherMgr *DispatcherMgr) checkExistsRunner(runner string) bool {
	dispatcherMgr.rwlock.RLock()
	defer dispatcherMgr.rwlock.RUnlock()
	if _, ok := dispatcherMgr.allRunner[runner]; ok {
		return true
	}
	return false
}

// 注册runner
func (dispatcherMgr *DispatcherMgr) registerRunner(runner string) {
	dispatcherMgr.rwlock.Lock()
	defer dispatcherMgr.rwlock.Unlock()
	dispatcherMgr.allRunner[runner] = true
}

// 从runner列表中， 随机获取一个runner
func (dispatcherMgr *DispatcherMgr) gerRandomRunner() string {
	dispatcherMgr.rwlock.RLock()
	defer dispatcherMgr.rwlock.RUnlock()
	if len(dispatcherMgr.allRunner) > 0 {
		index := rand.Intn(len(dispatcherMgr.allRunner))
		i := 0
		for runner, _ := range dispatcherMgr.allRunner {
			if index == i {
				return runner
			}
			i = i + 1
		}
	}

	return ""
}

// 下线runner
func (dispatcherMgr *DispatcherMgr) offlineRunner(runner string) {
	dispatcherMgr.rwlock.Lock()
	defer dispatcherMgr.rwlock.Unlock()

	// 删除runner
	delete(dispatcherMgr.allRunner, runner)

	// 获取 runner上绑定的commit_id
	if commitIdList, ok := dispatcherMgr.runnerBindCommit[runner]; ok {
		for _, commitId := range commitIdList {
			delete(dispatcherMgr.commitBindRunner, commitId)
			dispatcherMgr.pushCommit(commitId)
		}
		delete(dispatcherMgr.runnerBindCommit, runner)
	}
}

// commitId 执行完毕后， 归还使用
func (dispatcherMgr *DispatcherMgr) unbindRunner(commitId string) {
	dispatcherMgr.rwlock.Lock()
	defer dispatcherMgr.rwlock.Unlock()

	// 获取 runner上绑定的commit_id
	if runner, ok := dispatcherMgr.commitBindRunner[commitId]; ok {
		// 拿到runner后， 去runnerBindCommit列表中， 删除 runner
		commitIdList := dispatcherMgr.runnerBindCommit[runner]
		newCommitIdList := []string{}
		for _, ele := range commitIdList {
			if ele != commitId {
				newCommitIdList = append(newCommitIdList, ele)
			}
		}
		dispatcherMgr.runnerBindCommit[runner] = newCommitIdList
	}
}

// commitId 和runner的 绑定
func (dispatcherMgr *DispatcherMgr) bindRunner(commitId string, runner string) {
	dispatcherMgr.rwlock.Lock()
	defer dispatcherMgr.rwlock.Unlock()

	dispatcherMgr.commitBindRunner[commitId] = runner

	if _, ok := dispatcherMgr.runnerBindCommit[runner]; ok {
		dispatcherMgr.runnerBindCommit[runner] = append(dispatcherMgr.runnerBindCommit[runner], commitId)
	}
}

// 获取全部runner
func (dispatcherMgr *DispatcherMgr) getRunnerNum() int {
	dispatcherMgr.rwlock.RLock()
	defer dispatcherMgr.rwlock.RUnlock()
	return len(dispatcherMgr.allRunner)
}

// 主动 pushCommit
func (dispatcherMgr *DispatcherMgr) pushCommit(commitId string) {
	dispatcherMgr.queueCommits <- commitId
}

// 阻塞 popCommit
func (dispatcherMgr *DispatcherMgr) popCommit() (commitId string) {
	select {
	case commitId = <-dispatcherMgr.queueCommits:
		return commitId
	}
}

// 心跳runner， 如果runner下线， 那么 释放runner资源
func (dispatcherMgr *DispatcherMgr) runnerHeartbeat() {
	for {
		for runner, _ := range dispatcherMgr.allRunner { // TODO 这里没有用锁保护， 可能会成为一个隐患
			response, err := helper.Communicate(runner, "ping")
			if err != nil || response != "pong" {
				if err != nil {
					fmt.Println("runner heartBeat fail error  ", err.Error())
				}
				// 下线该runner
				dispatcherMgr.offlineRunner(runner)
			}
		}
		time.Sleep(1 * time.Second)
	}
}

func (dispatcherMgr *DispatcherMgr) handleConnection(conn net.Conn) {
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
		} else if command == "register" { // runner 进行注册
			// 解析协议， 获取runner
			runner := row[1]
			port := row[2]
			runner = fmt.Sprintf("%s:%s", runner, port)
			fmt.Println("dispatcher handle runner registering ", runner)
			// 检查runner是否已注册过
			if dispatcherMgr.checkExistsRunner(runner) {
				fmt.Println("runner has registered ", runner)
				conn.Write([]byte("fail"))
			} else {
				dispatcherMgr.registerRunner(runner)
				conn.Write([]byte("OK"))
			}
		} else if command == "dispatch" { // observer 提交commit_id, 触发任务调度
			commitId := row[1]
			fmt.Println("dispatch ing commitId ", commitId)
			if dispatcherMgr.getRunnerNum() == 0 {
				fmt.Println("No runner are registered")
				conn.Write([]byte("No runner are registered"))
			} else {
				// commitId 加入到待执行的队列中
				dispatcherMgr.pushCommit(commitId)
				conn.Write([]byte("OK"))
			}
		} else if command == "results" { // runner 执行完毕后， dispatcher获取到执行结果， 进行日志写入
			// 执行完毕后， 结果回传
			fmt.Println("get test results=======================")
			commitId := row[1]
			msgLenStr := row[2]
			msgLen, _ := strconv.Atoi(msgLenStr)
			// 3 is the number of ":" in the sent command
			// results:commit_id:length_msg:data
			remaining := strings.Join(row[3:], "")
			otherLen := msgLen - len(remaining)
			if otherLen > 0 {
				buf := make([]byte, otherLen)
				reqLen, err := conn.Read(buf)
				if err != nil {
					fmt.Println("in result read data error ", err.Error())
					return
				}
				data := string(buf[0:reqLen])
				remaining = remaining + data
			}
			fmt.Println("remaining data==============", remaining)

			// 将其从正在执行的列表中删除
			// 标记commitId 已在runner上执行完毕
			dispatcherMgr.unbindRunner(commitId)

			// 结果存储 TODO
			// 发送回传
			conn.Write([]byte("OK"))
		}
	}
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// 服务器管理
func serve() {
	// 初始化调度管理器
	dispatcherMgr := initDispatcherMgr()

	// 启动server， 监听tcp连接
	go dispatcherMgr.startServer()

	// 分发协程， 将接收到的构建通知，分发到线程中
	go dispatcherMgr.dispatch()

	// 启动对runner的心跳守护
	go dispatcherMgr.runnerHeartbeat()

}

func main() {
	ctx, _ := context.WithCancel(context.TODO())
	serve()
	helper.GraceExit(ctx)
}
