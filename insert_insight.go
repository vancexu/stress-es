package main

import (
	"sync"
	"strconv"
	"github.com/olivere/elastic"
	"fmt"
	"time"
	"github.com/pborman/uuid"
	"math/rand"
	"encoding/json"
	"context"
)

const insight_index_setting = `
{
	"settings":{
		"number_of_shards": 5,
		"number_of_replicas": 1
	}
}`


func insertInsight(threadID string, done *sync.WaitGroup) {
	defer done.Done()

	times := 10000

	domainID := "100cd4ec-843c-4055-8baa-de52d697335d"
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

	ctx := context.Background()
	client, err := elastic.NewClient()
	if err != nil {
		panic(err)
	}
	exists, err := client.IndexExists(domainID).Do(ctx)
	if !exists {
		fmt.Println("create index ", domainID)
		createIndex, err := client.CreateIndex(domainID).BodyString(insight_index_setting).Do(ctx)
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
		src := rand.NewSource(time.Now().UnixNano())
		r := rand.New(src)
		k := stateKey[r.Intn(numOfStateKey)]
		v := stateValue[r.Intn(numOfStateValue)]
		body := []byte(fmt.Sprintf("{\"%s\" : \"%s\", \"update_time\" : %d}", k, v, millis))
		var req map[string]interface{}
		if err := json.Unmarshal(body, &req); err != nil {
			panic(err)
		}

		_, err := client.Update().Index(domainID).Type("_doc").Id(id).Doc(req).DocAsUpsert(true).Do(ctx)
		if err != nil {
			fmt.Println(err)
		}
		//fmt.Println(upd)

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
		go insertInsight(strconv.Itoa(i), &done)
	}
	done.Wait()
}
