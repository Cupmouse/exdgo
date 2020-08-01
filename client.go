package exdgo

import (
	"errors"
	"time"
)

// ClientParam is the config for a client.
type ClientParam struct {
	// API-key used to access Exchangedataset API server.
	APIKey string
	// Connection timeout.
	Timeout *time.Duration
}

// Client for accessing to Exchangedataset API.
type Client struct {
	apikey  string
	timeout time.Duration
}

// setupClient finalize ClientParam and returns `Client`
func setupClient(param ClientParam) (cli Client, err error) {
	if param.APIKey == "" {
		err = errors.New("empty parameter 'APIKey'")
		return
	}
	if !regexAPIKey.MatchString(param.APIKey) {
		err = errors.New("parameter 'APIKey' not a valid API-key")
		return
	}
	cli.apikey = param.APIKey
	if param.Timeout == nil {
		// Set the default value
		cli.timeout = clientDefaultTimeout
	} else {
		if *param.Timeout < 0 {
			err = errors.New("parameter 'Timeout' negative")
			return
		}
		cli.timeout = *param.Timeout
	}
	return
}

// CreateClient creates new Client and returns pointer to it.
func CreateClient(param ClientParam) (*Client, error) {
	// Create new Client
	client, serr := setupClient(param)
	if serr != nil {
		return nil, serr
	}
	return &client, nil
}
