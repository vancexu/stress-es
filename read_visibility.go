package main

import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
	"math/rand"
	"time"
	"github.com/vancexu/stress-es/common"
	"sync"
)

func read_visibility(client *elastic.Client, low, high int64, from, pagesize int) (int64, int64) {
	ctx := context.Background()

	indexName := "cadence-visibility-dev-dca1a"
	domainID := "3006499f-37b1-48e7-9d53-5a6a6363e72a"
	workflowTypeName := "code.uber.internal/devexp/cadence-bench/load/basic.stressWorkflowExecute"

	matchQuery := elastic.NewMatchQuery("WorkflowType", workflowTypeName)
	matchDomain := elastic.NewMatchQuery("DomainID", domainID)
	rangeQuery := elastic.NewRangeQuery("CloseTime").Gte(low).Lte(high)
	boolQuery := elastic.NewBoolQuery().Must(matchQuery).Must(matchDomain).Filter(rangeQuery)

	searchResult, err := client.Search().Index(indexName).Query(boolQuery).
		Sort("CloseTime", false).
		From(from).
		Size(pagesize).
		Do(ctx)
	if err != nil {
		panic(err)
	}

	return searchResult.TookInMillis, searchResult.TotalHits()
}

func main() {
	var times int
	fmt.Println("Number of requests: ")
	fmt.Scanln(&times)
	var timeBackInHours int
	fmt.Println("Number of hours back: ")
	fmt.Scanln(&timeBackInHours)
	var numOfThreads int
	fmt.Println("Number of threads: ")
	fmt.Scanln(&numOfThreads)
	if numOfThreads == 0 {
		numOfThreads = 1
	}

	client, err := common.NewElasticClient()
	if err != nil {
		panic(err)
	}

	fn := func(id int, wg *sync.WaitGroup) {
		var totalTime int64
		var totalHits int64

		for i := 0; i < times; i += 1 {
			lo := time.Now().Add(time.Duration(-timeBackInHours*60)*time.Minute).UnixNano()
			hi := time.Now().UnixNano()
			src := rand.NewSource(lo)
			r := rand.New(src)
			t, h := read_visibility(client, lo, hi, r.Intn(10), 100)
			//fmt.Println(t)
			totalTime += t
			totalHits += h
		}
		fmt.Println(id)
		fmt.Println("avg read time millis: ", totalTime/int64(times))
		fmt.Println("avg hits: ", totalHits/int64(times))
		wg.Done()
	}

	wg := &sync.WaitGroup{}
	wg.Add(numOfThreads)
	testStartTime := time.Now()
	for i := 0; i < numOfThreads; i++ {
		go fn(i, wg)
	}
	wg.Wait()
	fmt.Println(time.Since(testStartTime))
}
