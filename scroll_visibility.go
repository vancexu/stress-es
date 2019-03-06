package main

import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
	"github.com/vancexu/stress-es/common"
	"io"
	"sync"
	"time"
)

func scroll_visibility(client *elastic.Client, low, high int64, pagesize int) (int64, int64, int64, int64) {
	return scroll_helper(client, low, high, pagesize, false)
}

func scroll_visibility_sort(client *elastic.Client, low, high int64, pagesize int) (int64, int64, int64, int64) {
	return scroll_helper(client, low, high, pagesize, true)
}

func scroll_helper(client *elastic.Client, low, high int64, pagesize int, sorted bool) (int64, int64, int64, int64) {
	ctx := context.Background()

	var tookInMillis int64
	var totalHits int64
	var maxTook int64
	var avgTook int64

	indexName := "cadence-visibility-dev-dca1a"
	domainID := "3006499f-37b1-48e7-9d53-5a6a6363e72a"
	workflowTypeName := "code.uber.internal/devexp/cadence-bench/load/basic.stressWorkflowExecute"

	matchQuery := elastic.NewMatchQuery("WorkflowType", workflowTypeName)
	matchDomain := elastic.NewMatchQuery("DomainID", domainID)
	//rangeQuery := elastic.NewRangeQuery("CloseTime").Gte(low).Lte(high)
	boolQuery := elastic.NewBoolQuery().Must(matchQuery).Must(matchDomain)

	var scroll *elastic.ScrollService
	if sorted {
		scroll = client.Scroll().Index(indexName).Query(boolQuery).
			Sort("CloseTime", false).Size(pagesize)
	} else {
		scroll = client.Scroll().Index(indexName).Query(boolQuery).Size(pagesize)
	}

	i := int64(0)
	for {
		i++
		results, err := scroll.Do(ctx)
		if err == io.EOF {
			//scroll.Clear(context.Background())
			break // all results retrieved
		}
		if err != nil {
			fmt.Println("scroll err: ", err)
			break // something went wrong
		}

		tookInMillis += results.TookInMillis
		totalHits += int64(len(results.Hits.Hits))
		if results.TookInMillis > maxTook {
			maxTook = results.TookInMillis
		}

	}
	avgTook = tookInMillis / i
	fmt.Println("scroll a page avg took: ", avgTook)
	fmt.Println("scroll a page max took: ", maxTook)

	if err != nil {
		panic(err)
	}
	return tookInMillis, totalHits, avgTook, maxTook
}

func main() {
	var times int
	fmt.Println("Number of requests: ")
	fmt.Scanln(&times)

	var pageSize int
	fmt.Println("Page size: ")
	fmt.Scanln(&pageSize)

	if times <= 0 {
		times = 1
	}

	client, err := common.NewElasticClient()
	if err != nil {
		panic(err)
	}

	var done sync.WaitGroup
	done.Add(times)

	var lock sync.Mutex
	var totalTime int64
	var totalHits int64
	var maxTook int64
	var avgTook int64
	for i := 0; i < times; i += 1 {
		go func() {
			millis := time.Now().UnixNano()
			t, h, at, mt := scroll_visibility(client, 0, millis, pageSize)
			lock.Lock()
			totalTime += t
			totalHits += h
			avgTook += at
			if mt > maxTook {
				maxTook = mt
			}
			lock.Unlock()
			done.Done()
		}()
	}
	done.Wait()
	fmt.Println("------ Scroll Visibility ------")
	fmt.Println("avg read 1 page takes millis: ", avgTook/int64(times))
	fmt.Println("max read 1 page takes millis: ", maxTook)
	fmt.Println("avg read total time millis: ", totalTime/int64(times))
	fmt.Println("avg hits: ", totalHits/int64(times))

	//// for sort scroll
	//totalTime = 0
	//totalHits = 0
	//maxTook = 0
	//avgTook = 0
	//done.Add(times)
	//for i := 0; i < times; i += 1 {
	//	go func() {
	//		millis := time.Now().UnixNano() / 1e6
	//		t, h, at, mt := scroll_visibility_sort(client, 0, millis, pageSize)
	//		lock.Lock()
	//		totalTime += t
	//		totalHits += h
	//		avgTook += at
	//		if mt > maxTook {
	//			maxTook = mt
	//		}
	//		lock.Unlock()
	//		done.Done()
	//	}()
	//}
	//done.Wait()
	//fmt.Println("------ Scroll Visibility Sort ------")
	//fmt.Println("avg read 1 page takes millis: ", avgTook/int64(times))
	//fmt.Println("max read 1 page takes millis: ", maxTook)
	//fmt.Println("avg read total time millis: ", totalTime/int64(times))
	//fmt.Println("avg hits: ", totalHits/int64(times))
}
