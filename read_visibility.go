package main

import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
	"math/rand"
	"time"
)

func read_visibility(low, high int64, from, pagesize int) (int64, int64) {
	ctx := context.Background()

	client, err := elastic.NewClient()
	if err != nil {
		panic(err)
	}

	domainID := "bulk4ea2-69f9-4495-a1b2-6ea71b5fa459"
	workflowTypeName := "code.uber.internal/devexp/cadence-bench/load/basic.stressWorkflowExecute"

	matchQuery := elastic.NewMatchQuery("workflow_type_name", workflowTypeName)
	rangeQuery := elastic.NewRangeQuery("close_time").Gte(low).Lte(high)
	boolQuery := elastic.NewBoolQuery().Must(matchQuery).Filter(rangeQuery)

	searchResult, err := client.Search().Index(domainID).Query(boolQuery).
		Sort("close_time", false).
		From(from).Size(pagesize).
		Pretty(true).
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
		t, h := read_visibility(millis-3600000, millis, r.Intn(10), 10)
		//fmt.Println(t)
		totalTime += t
		totalHits += h
	}

	fmt.Println("avg read time millis: ", totalTime/int64(times))
	fmt.Println("avg hits: ", totalHits/int64(times))
}
