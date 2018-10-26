package common

import "net/http"

const (
	headerSource      = "rpc-caller"
	headerDestination = "rpc-service"
)

// muttleyTransport wraps around default http.Transport to add muttley specific headers to all requests
type muttleyTransport struct {
	http.Transport

	source      string
	destination string
}

func (t *muttleyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r.Header.Set(headerSource, t.source)
	r.Header.Set(headerDestination, t.destination)
	return t.Transport.RoundTrip(r)
}
