package main

import (
	"context"
	"fmt"
	"time"

	"github.com/olivere/elastic"
	"github.com/pborman/uuid"
	"github.com/vancexu/stress-es/common"
	"strconv"
	"sync"
)

type ClosedWorkflowBulk struct {
	WorkflowID       string `json:"workflow_id"`
	RunID            string `json:"run_id"`
	WorkflowTypeName string `json:"workflow_type_name"`
	Status           int    `json:"status"`
	StartTime        int64  `json:"start_time"`
	CloseTime        int64  `json:"close_time"`
	HistoryLength    int    `json:"history_length"`
	Info             string `json:"info,omitempty"`
}

const index_bulk_setting = `
{
	"settings":{
		"number_of_shards": 5,
		"number_of_replicas": 1
	}
}`

func insertDocBulk(threadID string, done *sync.WaitGroup, times, batchSize int,
	duration, durationWithoutPrep *time.Duration, bulkTook *int64, reqUsed *time.Duration) {
	defer done.Done()

	domainID := "bulk4ea2-69f9-4495-a1b2-6ea71b5fa459"
	workflowTypeName := "code.uber.internal/devexp/cadence-bench/load/basic.stressWorkflowExecute"
	info := "some info"

	ctx := context.Background()
	client, err := common.NewElasticClient()
	if err != nil {
		panic(err)
	}
	exists, err := client.IndexExists(domainID).Do(ctx)
	if !exists {
		fmt.Println("create index ", domainID)
		createIndex, err := client.CreateIndex(domainID).BodyString(index_bulk_setting).Do(ctx)
		if err != nil {
			panic(err)
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
		}
	}

	bulkUsed := int64(0)
	timeUsed := time.Duration(0)
	startTime := time.Now()
	for t := 1; t <= times; t++ {

		bulkRequest := client.Bulk()
		for i := 0; i < batchSize; i++ {
			millis := time.Now().UnixNano() / 1e6
			rid := uuid.New()

			id := rid + "_" + rid
			body := ClosedWorkflowBulk{
				WorkflowID:       rid,
				RunID:            rid,
				WorkflowTypeName: workflowTypeName,
				Status:           0,
				StartTime:        millis - 3600,
				CloseTime:        millis,
				HistoryLength:    1024,
				Info:             info,
			}

			req := elastic.NewBulkIndexRequest().Index(domainID).Type("_doc").Id(id).Doc(body)
			bulkRequest.Add(req)
		}

		if bulkRequest.NumberOfActions() != batchSize {
			fmt.Printf("warning: number of actions is %d\n", bulkRequest.NumberOfActions())
		}

		reqStartTime := time.Now()

		bulkResponse, err := bulkRequest.Do(context.Background())
		if err != nil {
			fmt.Println("bulk failed", err)
		}

		timeUsed += time.Since(reqStartTime)

		if bulkRequest.NumberOfActions() != 0 {
			fmt.Printf("bulk request not done %d\n", bulkRequest.NumberOfActions())
		}

		bulkUsed += int64(bulkResponse.Took)

		if t%2000 == 0 {
			fmt.Println(threadID, t)
		}
	}

	elapsedTime := time.Since(startTime)
	fmt.Println(threadID, elapsedTime)
	*duration += elapsedTime
	*bulkTook += bulkUsed / int64(times)
	*reqUsed += time.Duration(int64(timeUsed) / int64(times))
	*durationWithoutPrep += timeUsed
}

func main() {

	var numOfThread int
	fmt.Println("Number of go routines: ")
	fmt.Scanln(&numOfThread)

	var numOfRequestPerThread int
	fmt.Println("Number of request per go routines: ")
	fmt.Scanln(&numOfRequestPerThread)

	var bulkSize int //10-15MB would be better
	fmt.Println("Bulk size: ")
	fmt.Scanln(&bulkSize)

	if numOfThread <= 0 {
		numOfThread = 1
	}

	if numOfRequestPerThread <= 0 {
		numOfRequestPerThread = 10
	}

	if bulkSize <= 0 {
		bulkSize = 20000
	}

	var done sync.WaitGroup
	done.Add(numOfThread)
	var duration time.Duration
	var durationWithoutPrep time.Duration
	var reqUsed time.Duration
	var bulkTook int64
	for i := 0; i < numOfThread; i += 1 {
		go insertDocBulk(strconv.Itoa(i), &done, numOfRequestPerThread, bulkSize, &duration, &durationWithoutPrep, &bulkTook, &reqUsed)
	}
	done.Wait()
	fmt.Println("avg time: ", time.Duration(int64(duration)/int64(numOfThread)))
	fmt.Println("avg time on request: ", time.Duration(int64(durationWithoutPrep)/int64(numOfThread)))
	fmt.Println("avg bulk took: ", bulkTook/int64(numOfThread))
	fmt.Println("avg req took: ", time.Duration(int64(reqUsed)/int64(numOfThread)))
}
