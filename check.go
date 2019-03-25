package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic"
	"github.com/vancexu/stress-es/common"
	"os"
	"strings"
	"time"
)

type visibilityRecord struct {
	WorkflowID    string
	RunID         string
	WorkflowType  string
	StartTime     int64
	CloseTime     int64
	CloseStatus   int
	HistoryLength int64
}

func main() {
	client, err := common.NewElasticClient()
	//client, err := elastic.NewClient(elastic.SetDecoder(&elastic.NumberDecoder{}))
	if err != nil {
		panic(err)
	}

	file, err := os.Open("es_check")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var ids []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		ids = append(ids, strings.Join(strings.Split(line[1:len(line) - 1], " "), "~"))
	}

	//index := "cadence-visibility-dev"
	index := "cadence-visibility-prod-dca1a"

	var items []*elastic.MultiGetItem
	for i := 0; i < len(ids); i++ {
		items = append(items, elastic.NewMultiGetItem().Index(index).Id(ids[i]))
		if i%1000 == 999 {
			check(client, items)
			items = nil
			time.Sleep(1 * time.Second)
		}
	}
	check(client, items)
	items = nil
}

func check(client *elastic.Client, items []*elastic.MultiGetItem) {
	ctx := context.Background()
	resp, err := client.Mget().Add(items...).Do(ctx)
	if err != nil {
		panic(err)
	}
	for _, doc := range resp.Docs {
		var source *visibilityRecord
		err := json.Unmarshal(*doc.Source, &source)
		if err != nil {
			panic(err)
		}
		if source.CloseTime == 0 {
			fmt.Println(source)
		}
	}
}