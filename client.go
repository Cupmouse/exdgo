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

type clientSetting struct {
	apikey  string
	timeout time.Duration
}

// setupClientSetting finalize ClientParam to get clientSetting
func setupClientSetting(param ClientParam) (setting clientSetting, err error) {
	if param.APIKey == "" {
		err = errors.New("empty parameter 'APIKey'")
		return
	}
	if !regexAPIKey.MatchString(param.APIKey) {
		err = errors.New("parameter 'APIKey' not a valid API-key")
		return
	}
	setting.apikey = param.APIKey
	if param.Timeout == nil {
		// Set the default value
		setting.timeout = clientDefaultTimeout
	} else {
		if *param.Timeout < 0 {
			err = errors.New("parameter 'Timeout' negative")
			return
		}
		setting.timeout = *param.Timeout
	}
	return
}

// Client for accessing to Exchangedataset API.
type Client struct {
	setting clientSetting
}

// func (c *Client) Replay(params ReplayRequestParam) {

// }

// CreateClient creates new Client and returns pointer to it.
func CreateClient(param ClientParam) (*Client, error) {
	var serr error
	// Create new Client and set API-key
	client := new(Client)
	client.setting, serr = setupClientSetting(param)
	if serr != nil {
		return nil, serr
	}
	return client, nil
}
