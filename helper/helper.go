package helper

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func Communicate(address, body string) (response string, err error) {
	//fmt.Printf("communicate with %s, body:%s \n", address, body)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println("communicate is error ", err.Error())
		return
	}
	defer conn.Close()
	conn.Write([]byte(body))
	buf := make([]byte, 1024)
	bufio.NewReader(conn).Read(buf)

	n := bytes.Index(buf[:], []byte{0})
	response = string(buf[:n])
	//fmt.Printf("communicate reult :%s \n", response)
	return
}

func GraceExit(ctx context.Context) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		select {
		case s := <-c:
			switch s {
			case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
				fmt.Println("信号量主动退出！")
				return
			case syscall.SIGHUP:
			default:
			}
		case <-ctx.Done():
			fmt.Println("程序主动退出....")
		}
	}
	fmt.Println("完全退出啦")
}
