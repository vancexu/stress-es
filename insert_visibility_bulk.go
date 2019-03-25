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
	DomainID         string `json:"DomainID"`
	WorkflowID       string `json:"WorkflowID"`
	RunID            string `json:"RunID"`
	WorkflowTypeName string `json:"WorkflowType"`
	Status           int    `json:"CloseStatus"`
	StartTime        int64  `json:"StartTime"`
	CloseTime        int64  `json:"CloseTime"`
	HistoryLength    int    `json:"HistoryLength"`
}

const index_bulk_setting = `
{
	"settings":{
		"number_of_shards": 20,
		"number_of_replicas": 2
	}
}`

func insertDocBulk(threadID string, done *sync.WaitGroup, times, batchSize int,
	duration, durationWithoutPrep *time.Duration, bulkTook *int64, reqUsed *time.Duration) {
	defer done.Done()

	index := "cadence-visibility-perf-dca1a"
	domainID := "3006499f-37b1-48e7-9d53-5a6a6363e72a"
	workflowTypeName := "code.uber.internal/devexp/cadence-bench/load/basic.stressWorkflowExecute"

	ctx := context.Background()
	client, err := common.NewElasticClient()
	if err != nil {
		panic(err)
	}
	exists, err := client.IndexExists(index).Do(ctx)
	if !exists {
		fmt.Println("create index ", index)
		createIndex, err := client.CreateIndex(index).BodyString(index_bulk_setting).Do(ctx)
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
				DomainID:         domainID,
				WorkflowID:       rid,
				RunID:            rid,
				WorkflowTypeName: workflowTypeName,
				Status:           0,
				StartTime:        millis - 3600,
				CloseTime:        millis,
				HistoryLength:    1024,
			}

			req := elastic.NewBulkIndexRequest().Index(index).Type("_doc").Id(id).Doc(body)
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
