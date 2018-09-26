package main

import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
	"math/rand"
	"time"
	"strconv"
)

func readInsight(low, high int64, from, pagesize int, stateKey, stateValue string) (int64, int64) {
	ctx := context.Background()

	client, err := elastic.NewClient()
	if err != nil {
		panic(err)
	}

	domainID := "100cd4ec-843c-4055-8baa-de52d697335d"

	matchQuery := elastic.NewMatchPhraseQuery(stateKey, stateValue)
	rangeQuery := elastic.NewRangeQuery("update_time").Gte(low).Lte(high)
	boolQuery := elastic.NewBoolQuery().Must(matchQuery).Filter(rangeQuery)

	searchResult, err := client.Search().Index(domainID).Query(boolQuery).
		Sort("update_time", false).
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

	numOfStateKey := 50
	numOfStateValue := 100
	var stateKey []string
	var stateValue []string
	for i := 0; i < numOfStateKey; i += 1 {
		stateKey = append(stateKey, "state_key_"+strconv.Itoa(i))
	}
	for i := 0; i < numOfStateValue; i += 1 {
		stateValue = append(stateValue, "state_value_"+strconv.Itoa(i))
	}

	var totalTime int64
	var totalHits int64
	for i := 0; i < times; i += 1 {
		millis := time.Now().UnixNano() / 1e6
		src := rand.NewSource(millis)
		r := rand.New(src)
		k := stateKey[r.Intn(numOfStateKey)]
		v := stateValue[r.Intn(numOfStateValue)]
		t, h := readInsight(millis-3600000, millis, r.Intn(10), 10, k, v)
		//fmt.Println(t)
		totalTime += t
		totalHits += h
	}

	fmt.Println("avg read time millis: ", totalTime/int64(times))
	fmt.Println("avg hits: ", totalHits/int64(times))
}
