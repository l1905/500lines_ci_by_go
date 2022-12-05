package main

import (
	"500lines_ci_by_go/helper"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"time"
)

func readline(path string) string {
	f, err := os.Open(path)
	if err != nil {
		fmt.Println("read file fail", err)
		return ""
	}
	defer f.Close()

	fd, err := ioutil.ReadAll(f)
	if err != nil {
		fmt.Println("read to fd fail", err)
		return ""
	}
	data := string(fd)
	info := strings.Split(data, "\n")
	return info[0]
}

// 轮询监测目录
func poll() {
	dispatcherServer := flag.String("dispatcher_server", "localhost:3333", "dispatcher host:port, by default it uses localhost:8888")
	repo := flag.String("repo", "", "path to the respository this will observe")

	flag.Parse()

	// 死循环
	for true {
		cmd := exec.Command("./update_repo.sh", *repo)
		out, err := cmd.CombinedOutput()
		if err != nil {
			fmt.Println("cmd.Run() failed with ", err)
			return
		}
		fmt.Printf("combined out:\n%s\n", string(out))
		if _, err := os.Stat(".commit_id"); err != nil {
			fmt.Println("stat file .commit_id fail", err.Error())
		} else {
			response, _ := helper.Communicate(*dispatcherServer, "ping")
			response = strings.TrimSpace(response)
			fmt.Println([]byte(response))
			fmt.Println([]byte("pong"))

			if response == "pong" {
				commitId := readline(".commit_id")

				if len(commitId) == 0 {
					fmt.Println("get commit_id fail ", commitId)
					continue
				}
				fmt.Println("request dispatch ", commitId)
				helper.Communicate(*dispatcherServer, fmt.Sprintf("dispatch:%s", commitId))
			}
		}
		time.Sleep(5 * time.Second)
	}
}

func main() {
	poll()
	fmt.Println("hello observer!")
}
