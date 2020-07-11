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
		// set the default value
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

type Client struct {
	setting clientSetting
}

// func (c *Client) Raw(params RawRequestParam) {

// }

// func (c *Client) Replay(params ReplayRequestParam) {

// }

// CreateClient creates new Client and returns pointer to it.
// func CreateClient(setting ClientSetting) (*Client, error) {
// 	// Check if API-key provided is in base64 URL formats
// 	_, serr := base64.RawURLEncoding.DecodeString(apikey)
// 	if serr != nil {
// 		return nil, fmt.Errorf("API-key provided is not valid: %v", serr)
// 	}
// 	// Create new Client and set API-key
// 	client := new(Client)
// 	client.setting.apikey = apikey
// 	return client, nil
// }
