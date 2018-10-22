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
	"hash/fnv"
)

const insight_bulk2_update_setting = `
{
	"settings":{
		"number_of_shards": 5,
		"number_of_replicas": 1
	}
}`

var stateKeys2 []string
var stateValues2 [][]string
var baseDocID2 string
var numOfStates2 int
var numOfValues2 int
var numOfDoc2 int
var numOfStatesPerDoc2 int

func updateInsightBulk2(threadID string, done *sync.WaitGroup, times, batchSize int,
	duration, durationWithoutPrep *time.Duration, bulkTook *int64, reqUsed *time.Duration) {
	defer done.Done()

	domainID := "bulkupd2-843c-4055-8baa-de52d697335d"

	ctx := context.Background()
	client, err := elastic.NewClient()
	if err != nil {
		panic(err)
	}
	exists, err := client.IndexExists(domainID).Do(ctx)
	if !exists {
		fmt.Println("create index ", domainID)
		createIndex, err := client.CreateIndex(domainID).BodyString(insight_bulk2_update_setting).Do(ctx)
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
	retry := 0
	for t := 1; t <= times && retry < 20; t++ {

		bulkRequest := client.Bulk()
		for i := 0; i < batchSize; i++ {
			millis := time.Now().UnixNano() / 1e6
			src := rand.NewSource(time.Now().UnixNano())
			r := rand.New(src)

			tmp := baseDocID2 + strconv.Itoa(r.Intn(numOfDoc2))

			keyIndex := getKeyIndex2(tmp, r.Intn(numOfStatesPerDoc2))
			k := stateKeys2[keyIndex]
			v := stateValues2[keyIndex][r.Intn(numOfValues2)]
			id := tmp + "_" + k

			body := []byte(fmt.Sprintf("{\"state\" : \"%s\", \"value\" : \"%s\", \"update_time\" : %d}", k, v, millis))
			var b map[string]interface{}
			if err := json.Unmarshal(body, &b); err != nil {
				panic(err)
			}

			req := elastic.NewBulkIndexRequest().Index(domainID).Type("_doc").Id(id).Doc(b).VersionType("external").Version(millis)
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
			//panic("bulk failed")
			t--
			retry++
			continue
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

	initData2()

	var done sync.WaitGroup
	done.Add(numOfThread)
	var duration time.Duration
	var durationWithoutPrep time.Duration
	var reqUsed time.Duration
	var bulkTook int64
	for i := 0; i < numOfThread; i += 1 {
		go updateInsightBulk2(strconv.Itoa(i), &done, numOfRequestPerThread, bulkSize, &duration, &durationWithoutPrep, &bulkTook, &reqUsed)
	}
	done.Wait()
	fmt.Println("avg time: ", time.Duration(int64(duration)/int64(numOfThread)))
	fmt.Println("avg time on request: ", time.Duration(int64(durationWithoutPrep)/int64(numOfThread)))
	fmt.Println("avg bulk took: ", bulkTook/int64(numOfThread))
	fmt.Println("avg req took: ", time.Duration(int64(reqUsed)/int64(numOfThread)))
}

func initData2() {
	baseDocID2 = uuid.New()
	baseDocID2 = baseDocID2 + "_" + reverse2(baseDocID2) + "_"

	numOfStatesPerDoc2 = 50

	numOfStates2 = 5000
	numOfValues2 = 100
	numOfDoc2 = 1000000 // 1M
	for i := 0; i < numOfStates2; i++ {
		stateKeys2 = append(stateKeys2, uuid.New())

		var values []string
		for j := 0; j < numOfValues2; j++ {
			values = append(values, uuid.New())
		}
		stateValues2 = append(stateValues2, values)
	}
}

func reverse2(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < len(r)/2; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}

func getKeyIndex2(uid string, offset int) uint32 {
	h := fnv.New32a()
	h.Write([]byte(uid))
	hash := h.Sum32()

	n := uint32(numOfStates2)
	return (hash % n + uint32(offset)) % n
}