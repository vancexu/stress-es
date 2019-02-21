package main

import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
	"math/rand"
	"time"
	"github.com/vancexu/stress-es/common"
)

func read_visibility(low, high int64, from, pagesize int) (int64, int64) {
	ctx := context.Background()

	client, err := common.NewElasticClient()
	if err != nil {
		panic(err)
	}

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

	var totalTime int64
	var totalHits int64
	for i := 0; i < times; i += 1 {
		millis := time.Now().UnixNano() / 1e6
		src := rand.NewSource(millis)
		r := rand.New(src)
		t, h := read_visibility(millis-3600000, millis, r.Intn(10), 100)
		//fmt.Println(t)
		totalTime += t
		totalHits += h
	}

	fmt.Println("avg read time millis: ", totalTime/int64(times))
	fmt.Println("avg hits: ", totalHits/int64(times))
}
