package main

import (
	"context"
	"fmt"
	"time"

	"github.com/olivere/elastic"
	"github.com/pborman/uuid"
	"sync"
	"strconv"
)

type ClosedWorkflow struct {
	WorkflowID       string `json:"workflow_id"`
	RunID            string `json:"run_id"`
	WorkflowTypeName string `json:"workflow_type_name"`
	Status           int    `json:"status"`
	StartTime        int64  `json:"start_time"`
	CloseTime        int64  `json:"close_time"`
	HistoryLength    int    `json:"history_length"`
	Info             string `json:"info,omitempty"`
}

const index_setting = `
{
	"settings":{
		"number_of_shards": 5,
		"number_of_replicas": 1
	}
}`

func insertDoc(threadID string, done *sync.WaitGroup) {
	defer done.Done()

	times := 10000

	domainID := "12324ea2-69f9-4495-a1b2-6ea71b5fa459"
	workflowTypeName := "code.uber.internal/devexp/cadence-bench/load/basic.stressWorkflowExecute"
	info := "some info"

	ctx := context.Background()
	client, err := elastic.NewClient()
	if err != nil {
		panic(err)
	}
	exists, err := client.IndexExists(domainID).Do(ctx)
	if !exists {
		fmt.Println("create index ", domainID)
		createIndex, err := client.CreateIndex(domainID).BodyString(index_setting).Do(ctx)
		if err != nil {
			panic(err)
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
		}
	}

	fmt.Println("start execute query")
	startTime := time.Now()

	i := 0
	for i < times {
		millis := time.Now().UnixNano() / 1e6
		rid := uuid.New()

		id := rid + "_" + rid
		body := ClosedWorkflow{
			WorkflowID:       rid,
			RunID:            rid,
			WorkflowTypeName: workflowTypeName,
			Status:           0,
			StartTime:        millis - 3600,
			CloseTime:        millis,
			HistoryLength:    1024,
			Info:             info,
		}

		_, err := client.Index().Index(domainID).Type("_doc").Id(id).BodyJson(body).Do(ctx)
		if err != nil {
			fmt.Println(err)
		}
		//fmt.Println(put)

		if i%2000 == 0 {
			fmt.Println(threadID, i)
		}
		i += 1
	}

	elapsedTime := time.Since(startTime)
	fmt.Println(threadID, elapsedTime)
}

func main() {

	var numOfThread int
	fmt.Println("Number of go routines: ")
	fmt.Scanln(&numOfThread)

	if numOfThread <= 0 {
		numOfThread = 1
	}

	var done sync.WaitGroup
	done.Add(numOfThread)
	for i := 0; i < numOfThread; i += 1 {
		go insertDoc(strconv.Itoa(i), &done)
	}
	done.Wait()
}

