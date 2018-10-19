package main

import (
	"github.com/pborman/uuid"
	"os"
	"fmt"
	"bufio"
	"io"
)

func main() {
	numOfDoc := 100
	filename := "./docIDs.txt"
	// write doc id to file
	f, err := os.Create(filename)
	if err != nil {
		fmt.Println("init data err, fail to create file")
		panic(err)
	}

	for i := 0; i < numOfDoc; i++ {
		_, err = f.WriteString(uuid.New() + "\n")
		if err != nil {
			fmt.Println("init data err, fail to write to file")
			panic(err)
		}
	}
	f.Close()

	f, err = os.Open(filename)
	if err != nil {
		fmt.Println("init data err, fail to create file")
		panic(err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var cnt int
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}

			fmt.Printf("read file line error: %v\n", err)
			return
		}
		fmt.Print(line[:len(line)-1])
		cnt++
	}
	fmt.Println(cnt)
}
