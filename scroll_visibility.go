package main

import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
	"time"
	"io"
	"sync"
	"github.com/vancexu/stress-es/common"
)

func scroll_visibility(low, high int64, pagesize int) (int64, int64, int64, int64) {
	return scroll_helper(low, high, pagesize, false)
}

func scroll_visibility_sort(low, high int64, pagesize int) (int64, int64, int64, int64) {
	return scroll_helper(low, high, pagesize, true)
}

func scroll_helper(low, high int64, pagesize int, sorted bool) (int64, int64, int64, int64) {
	ctx := context.Background()

	client, err := common.NewElasticClient()
	if err != nil {
		panic(err)
	}

	var tookInMillis int64
	var totalHits int64
	var maxTook int64
	var avgTook int64

	domainID := "bulk4ea2-69f9-4495-a1b2-6ea71b5fa459"
	workflowTypeName := "code.uber.internal/devexp/cadence-bench/load/basic.stressWorkflowExecute"

	matchQuery := elastic.NewMatchQuery("workflow_type_name", workflowTypeName)
	rangeQuery := elastic.NewRangeQuery("close_time").Gte(low).Lte(high)
	boolQuery := elastic.NewBoolQuery().Must(matchQuery).Filter(rangeQuery)

	var scroll *elastic.ScrollService
	if sorted {
		scroll = client.Scroll().Index(domainID).Query(boolQuery).
			Sort("close_time", false).Size(pagesize)
	} else {
		scroll = client.Scroll().Index(domainID).Query(boolQuery).Size(pagesize)
	}

	i := int64(0)
	for {
		i++
		results, err := scroll.Do(ctx)
		if err == io.EOF {
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
	fmt.Println( "Page size: ")
	fmt.Scanln(&pageSize)

	if times <= 0 {
		times = 1
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
			millis := time.Now().UnixNano() / 1e6
			t, h, at, mt := scroll_visibility(0, millis, pageSize)
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

	// for sort scroll
	totalTime = 0
	totalHits = 0
	maxTook = 0
	avgTook = 0
	done.Add(times)
	for i := 0; i < times; i += 1 {
		go func() {
			millis := time.Now().UnixNano() / 1e6
			t, h, at, mt := scroll_visibility_sort(0, millis, pageSize)
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
	fmt.Println("------ Scroll Visibility Sort ------")
	fmt.Println("avg read 1 page takes millis: ", avgTook/int64(times))
	fmt.Println("max read 1 page takes millis: ", maxTook)
	fmt.Println("avg read total time millis: ", totalTime/int64(times))
	fmt.Println("avg hits: ", totalHits/int64(times))
}