package main

import (
	"context"
	"fmt"
	"time"

	"encoding/json"
	"github.com/olivere/elastic"
	"github.com/pborman/uuid"
	"math/rand"
	"strconv"
	"sync"
)

const insight_bulk_update_setting = `
{
	"settings":{
		"number_of_shards": 5,
		"number_of_replicas": 1
	}
}`

var stateKeys []string
var stateValues [][]string
var docIDs []string
var numOfStates int
var numOfValues int
var numOfDoc int

func updateInsightBulk(threadID string, done *sync.WaitGroup, times, batchSize int,
	duration, durationWithoutPrep *time.Duration, bulkTook *int64, reqUsed *time.Duration) {
	defer done.Done()

	domainID := "bulkupda-843c-4055-8baa-de52d697335d"

	ctx := context.Background()
	client, err := elastic.NewClient()
	if err != nil {
		panic(err)
	}
	exists, err := client.IndexExists(domainID).Do(ctx)
	if !exists {
		fmt.Println("create index ", domainID)
		createIndex, err := client.CreateIndex(domainID).BodyString(insight_bulk_update_setting).Do(ctx)
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
			src := rand.NewSource(time.Now().UnixNano())
			r := rand.New(src)

			id := getDocID(docIDs[r.Intn(numOfDoc)])

			keyIndex := r.Intn(numOfStates)
			k := stateKeys[keyIndex]
			v := stateValues[keyIndex][r.Intn(numOfValues)]
			body := []byte(fmt.Sprintf("{\"%s\" : \"%s\", \"update_time\" : %d}", k, v, millis))
			var b map[string]interface{}
			if err := json.Unmarshal(body, &b); err != nil {
				panic(err)
			}

			req := elastic.NewBulkUpdateRequest().Index(domainID).Type("_doc").Id(id).Doc(b).DocAsUpsert(true)
			bulkRequest.Add(req)
		}

		if bulkRequest.NumberOfActions() != batchSize {
			fmt.Printf("warning: number of actions is %d\n", bulkRequest.NumberOfActions())
		}

		reqStartTime := time.Now()

		bulkResponse, err := bulkRequest.Do(context.Background())
		if err != nil {
			fmt.Println("bulk failed", err)
			fmt.Println("remainning requeset: ", bulkRequest.NumberOfActions())
			panic("bulk failed")
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

	initData()

	var done sync.WaitGroup
	done.Add(numOfThread)
	var duration time.Duration
	var durationWithoutPrep time.Duration
	var reqUsed time.Duration
	var bulkTook int64
	for i := 0; i < numOfThread; i += 1 {
		go updateInsightBulk(strconv.Itoa(i), &done, numOfRequestPerThread, bulkSize, &duration, &durationWithoutPrep, &bulkTook, &reqUsed)
	}
	done.Wait()
	fmt.Println("avg time: ", time.Duration(int64(duration)/int64(numOfThread)))
	fmt.Println("avg time on request: ", time.Duration(int64(durationWithoutPrep)/int64(numOfThread)))
	fmt.Println("avg bulk took: ", bulkTook/int64(numOfThread))
	fmt.Println("avg req took: ", time.Duration(int64(reqUsed)/int64(numOfThread)))
}

func initData() {
	numOfStates = 5000
	numOfValues = 100
	numOfDoc = 1000000 // 1M
	for i := 0; i < numOfStates; i++ {
		stateKeys = append(stateKeys, uuid.New())

		var values []string
		for j := 0; j < numOfValues; j++ {
			values = append(values, uuid.New())
		}
		stateValues = append(stateValues, values)
	}

	for i := 0; i < numOfDoc; i++ {
		docIDs = append(docIDs, uuid.New())
	}
}

func getDocID(s string) string {
	return s + "_" + reverse(s)
}

func reverse(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < len(r)/2; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}
