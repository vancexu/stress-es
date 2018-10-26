package common

import (
	"github.com/olivere/elastic"
	"net/http"
	"net/url"
)
// NewElasticClient create new es client to cadence test cluster
func NewElasticClient() (*elastic.Client, error) {
	httpClient := &http.Client{
		Transport: &muttleyTransport{
			source:      "cadence-stress-es",
			destination: "es-cadence-query",
		},
	}

	esURL := &url.URL{
		Scheme: "http",
		Host:   "localhost:5436",
		Path:   "cadence",
	}

	return elastic.NewClient(
		elastic.SetHttpClient(httpClient),
		elastic.SetURL(esURL.String()),
	)
}
