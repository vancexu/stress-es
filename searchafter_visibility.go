package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic"
	"github.com/vancexu/stress-es/common"
	"sync"
	"time"
)

type SearchAfterRequest struct {
	IndexName        string
	DomainID         string
	WorkflowTypeName string
	Low              int64
	High             int64
	PageSize         int
	NextPageToken    []byte
}

type SearchAfterResponse struct {
	TookInMillis  int64
	TotalHits     int64
	ActualHits    int64
	Latency       time.Duration // real time used
	Records       []*VisibilityRecord
	NextPageToken []byte
}

type PageToken struct {
	CloseTime int64
	RunID     string
}

func deserializePageToken(data []byte) (*PageToken, error) {
	var token PageToken
	err := json.Unmarshal(data, &token)
	if err != nil {
		return nil, err
	}
	return &token, nil
}

func serializePageToken(token *PageToken) ([]byte, error) {
	data, err := json.Marshal(token)
	if err != nil {
		return nil, err
	}
	return data, nil
}

type VisibilityRecord struct {
	WorkflowID    string
	RunID         string
	WorkflowType  string
	StartTime     int64
	CloseTime     int64
	CloseStatus   int
	HistoryLength int64
}

func prettyPrint(i interface{}) string {
	s, _ := json.MarshalIndent(i, "", "\t")
	return string(s)
}

func searchOnePage(client *elastic.Client, request *SearchAfterRequest) (*SearchAfterResponse, error) {
	ctx := context.Background()
	matchQuery := elastic.NewMatchQuery("WorkflowType", request.WorkflowTypeName)
	matchDomain := elastic.NewMatchQuery("DomainID", request.DomainID)
	rangeQuery := elastic.NewRangeQuery("CloseTime").Gte(request.Low).Lte(request.High)
	boolQuery := elastic.NewBoolQuery().Must(matchQuery).Must(matchDomain).Filter(rangeQuery)
	pageSize := request.PageSize

	var err error
	var searchResult *elastic.SearchResult
	var nextPageToken []byte
	var actualHits []*elastic.SearchHit
	var latency time.Duration
	searchService := client.Search().Index(request.IndexName).Query(boolQuery).
		Sort("CloseTime", false).Sort("RunID", false).
		Size(pageSize)
	if request.NextPageToken == nil {
		// first page search
		startTime := time.Now()
		searchResult, err = searchService.Do(ctx)
		latency += time.Since(startTime)
		if err != nil {
			panic(err)
		}
		actualHits = searchResult.Hits.Hits
		if len(actualHits) == pageSize {
			sorts := actualHits[pageSize-1].Sort
			closeTime, err := sorts[0].(json.Number).Int64()
			if err != nil {
				panic(err)
			}
			token := &PageToken{
				CloseTime: closeTime,
				RunID:     sorts[1].(string),
			}
			nextPageToken, err = serializePageToken(token)
			if err != nil {
				panic(err)
			}
		}
	} else {
		// search after
		token, err := deserializePageToken(request.NextPageToken)
		if err != nil {
			panic(err)
		}
		//fmt.Println(token)
		startTime := time.Now()
		searchResult, err = searchService.SearchAfter(token.CloseTime, token.RunID).Do(ctx)
		latency += time.Since(startTime)
		if err != nil {
			panic(err)
		}

		actualHits = searchResult.Hits.Hits
		if len(actualHits) != 0 {
			sorts := actualHits[len(actualHits)-1].Sort
			closeTime, err := sorts[0].(json.Number).Int64()
			if err != nil {
				panic(err)
			}
			token = &PageToken{
				CloseTime: closeTime,
				RunID:     sorts[1].(string),
			}
			nextPageToken, err = serializePageToken(token)
			if err != nil {
				panic(err)
			}
			//fmt.Println(token)
			//fmt.Println("first hit ", actualHits[0].Sort)
			//fmt.Println("second hit ", actualHits[len(actualHits) - 1].Sort)
		}
	}

	resp := &SearchAfterResponse{
		TookInMillis:  searchResult.TookInMillis,
		TotalHits:     searchResult.TotalHits(),
		ActualHits:    int64(len(actualHits)),
		Latency:       latency,
		Records:       hitsToRecords(actualHits),
		NextPageToken: nextPageToken,
	}
	return resp, err
}

//var saw = make(map[string]struct{})

func hitsToRecords(hits []*elastic.SearchHit) []*VisibilityRecord {
	res := make([]*VisibilityRecord, len(hits))
	for i, hit := range hits {
		var source *VisibilityRecord
		err := json.Unmarshal(*hit.Source, &source)
		if err != nil {
			panic(err)
		}
		res[i] = source
		//if _, ok := saw[source.RunID]; ok {
		//	fmt.Println("found duplicate in page: ", source.CloseTime, source.RunID)
		//}
		//saw[source.RunID] = struct{}{}
	}
	return res
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
	if pageSize <= 0 {
		pageSize = 1000
	}
	client, err := common.NewElasticClient()
	//client, err := elastic.NewClient(elastic.SetDecoder(&elastic.NumberDecoder{}))
	if err != nil {
		panic(err)
	}

	var done sync.WaitGroup
	done.Add(times)

	var lock sync.Mutex
	var totalTime int64
	var totalHits int64
	var avgTook int64
	var firstPageTook int64
	var numOfPages int64
	var latency time.Duration
	var avgLatency time.Duration
	index := "cadence-visibility-perf-dca1a"
	domainID := "3006499f-37b1-48e7-9d53-5a6a6363e72a"
	//// local
	//index := "cadence-visibility-dev"
	//domainID := "33fd075c-0d65-47b3-bd5e-80a2069f2044"
	for i := 0; i < times; i += 1 {
		go func() {
			nanos := time.Now().UnixNano()
			request := &SearchAfterRequest{
				IndexName:        index,
				DomainID:         domainID,
				WorkflowTypeName: "code.uber.internal/devexp/cadence-bench/load/basic.stressWorkflowExecute",
				Low:              0,
				High:             nanos,
				PageSize:         pageSize,
			}
			resp, _ := searchOnePage(client, request)

			lock.Lock()
			numOfPages += 1
			firstPageTook = resp.TookInMillis
			totalTime += resp.TookInMillis
			totalHits += resp.ActualHits
			avgTook += resp.TookInMillis
			latency += resp.Latency
			lock.Unlock()

			for resp.ActualHits == int64(pageSize) {
				request.NextPageToken = resp.NextPageToken
				resp, _ = searchOnePage(client, request)

				lock.Lock()
				numOfPages += 1
				totalTime += resp.TookInMillis
				totalHits += resp.ActualHits
				avgTook += resp.TookInMillis
				latency += resp.Latency
				lock.Unlock()
			}

			lock.Lock()
			avgTook = avgTook / numOfPages
			avgLatency = time.Duration(latency.Nanoseconds()/numOfPages) * time.Nanosecond
			lock.Unlock()

			done.Done()
		}()
	}
	done.Wait()
	fmt.Println("------ Search After Visibility ------")
	fmt.Println("read 1st page takes millis: ", firstPageTook/int64(times))
	fmt.Println("read 1 page in server takes millis: ", avgTook/int64(times))
	fmt.Println("read total in server takes millis: ", totalTime/int64(times))
	fmt.Println("read 1 page latency takes: ", time.Duration(avgLatency.Nanoseconds()/int64(times)))
	fmt.Println("read total latency takes: ", time.Duration(latency.Nanoseconds()/int64(times)))
	fmt.Println("avg hits: ", totalHits/int64(times))
}
