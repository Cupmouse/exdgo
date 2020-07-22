package exdgo

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// httpDownload will send HTTP GET request to HTTP Endpoint with timeout.
// clientSetting's Timeout duration is used.
// Response is nil if and only if error is non-nil.
func httpDownloadWithTimeout(ctx context.Context, setting clientSetting, path string, params url.Values) (statusCode int, body []byte, err error) {
	childCtx, cancel := context.WithTimeout(ctx, setting.timeout)
	// Free resources anyway
	defer cancel()

	req, serr := http.NewRequestWithContext(childCtx, http.MethodGet, urlAPI+path, nil)
	if serr != nil {
		err = fmt.Errorf("creating request %s: %v", path, serr)
		return
	}
	// Set query parameter
	req.URL.RawQuery = params.Encode()
	// Set authorization header
	req.Header.Add("Authorization", "Bearer "+setting.apikey)
	res, serr := http.DefaultClient.Do(req)
	if serr != nil {
		err = fmt.Errorf("request %s: %v", path, serr)
		return
	}
	// Assures closing of res.Body.
	defer func() {
		serr := res.Body.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("closing body: %v, another error: %v", err, serr)
			} else {
				err = fmt.Errorf("closing body: %v", serr)
			}
		}
	}()
	// Read all response and store it on byte slice.
	body, serr = ioutil.ReadAll(res.Body)
	if serr != nil {
		err = fmt.Errorf("body read: %v", serr)
		return
	}
	// Check status-code.
	statusCode = res.StatusCode
	if statusCode != http.StatusOK && statusCode != http.StatusNotFound {
		// An error has returned from server
		// This struct is used to marshal an error message from server.
		var obj *struct {
			Error   *string `json:"error"`
			Message *string `json:"message,Message"`
		}
		var errMsg string
		// This error is used to determine if bytes are in correct JSON format.
		ierr := json.Unmarshal(body, obj)
		if ierr == nil {
			if obj.Error != nil {
				errMsg = *obj.Error
			} else if obj.Message != nil {
				errMsg = *obj.Message
			} else {
				// Fallback to use a raw response as error string
				errMsg = string(body)
			}
		} else {
			errMsg = string(body)
		}
		err = fmt.Errorf("request %s bad status code %d: %s", path, statusCode, errMsg)
		return
	}

	// Check Content-Type header.
	contentType := res.Header.Get("Content-Type")
	if contentType != "text/plain" {
		err = fmt.Errorf("request %s bad content-type %s", path, contentType)
		return
	}

	// Compression is automatically processed by http library.

	return
}

// Snapshot holds a line from Snapshot HTTP Endpoint.
type Snapshot struct {
	// Channel name.
	Channel string
	// Timestamp of this snapshot.
	Timestamp int64
	// Non-nil Snapshot.
	Snapshot []byte
}

// SnapshotParam is the parameters to make new {@link SnapshotRequest}.
type SnapshotParam struct {
	// What exchange to filter-in.
	Exchange string
	// What channels to filter-in.
	// nil value will be comprehended as empty slice.
	Channels []string
	// Date-time to take snapshot at.
	At time.Time
	// What format to get response in.
	Format *string
}

type snapshotSetting struct {
	exchange string
	channels []string
	at       int64
	format   *string
}

func setupSnapshotSetting(param SnapshotParam) (setting snapshotSetting, err error) {
	if !regexName.MatchString(param.Exchange) {
		err = errors.New("invalid characters in 'Exchange'")
		return
	}
	setting.exchange = param.Exchange
	for _, ch := range setting.channels {
		if !regexName.MatchString(ch) {
			err = errors.New("invalid characters in 'Channels'")
			return
		}
	}
	setting.channels = param.Channels
	setting.at = param.At.UnixNano()
	// Optional parameter
	if param.Format != nil {
		if !regexName.MatchString(*param.Format) {
			err = errors.New("invalid characters in 'Format'")
			return
		}
		setting.format = param.Format
	}
	return
}

// httpSnapshot is internal function for requesting Snapshot HTTP Endpoint
// using settings for both client and snapshot.
func httpSnapshot(ctx context.Context, clientSetting clientSetting, setting snapshotSetting) ([]Snapshot, error) {
	path := fmt.Sprintf("snapshot/%s/%d", setting.exchange, setting.at)
	params := make(url.Values)
	params["channels"] = setting.channels
	// Optional parameter
	if setting.format != nil {
		params["format"] = []string{*setting.format}
	}

	// Send a request to server
	statusCode, body, serr := httpDownloadWithTimeout(ctx, clientSetting, path, params)
	if serr != nil {
		return nil, serr
	}
	if statusCode == http.StatusNotFound {
		// 404, return empty slice
		return make([]Snapshot, 0), nil
	}
	// Conversion to line structs
	// Construct buffered reader from byte slice
	reader := bytes.NewReader(body)
	breader := bufio.NewReader(reader)
	// Slice to store result
	lines := make([]Snapshot, 0, 10)
	for {
		timestampStr, serr := breader.ReadString('\t')
		if serr != nil {
			if serr == io.EOF {
				// EOF at the begining of line, break from loop
				break
			}
			return nil, fmt.Errorf("timestamp read error: %v", serr)
		}
		// Strip tab character at the end
		timestampStr = timestampStr[:len(timestampStr)-1]
		timestamp, serr := strconv.ParseInt(timestampStr, 10, 64)
		if serr != nil {
			return nil, fmt.Errorf("timestamp conversion: %v", serr)
		}
		channel, serr := breader.ReadString('\t')
		if serr != nil {
			return nil, fmt.Errorf("channel read error: %v", serr)
		}
		channel = channel[:len(channel)-1]
		snapshot, serr := breader.ReadBytes('\n')
		if serr != nil {
			return nil, fmt.Errorf("snapshot read error: %v", serr)
		}
		snapshot = snapshot[:len(snapshot)]
		// Store line to result
		lines = append(lines, Snapshot{
			Timestamp: timestamp,
			Channel:   channel,
			Snapshot:  snapshot,
		})
	}
	return lines, nil
}

// HTTPSnapshot create and return request to Snapshot HTTP-API endpoint.
// Returns nil as `Snapshot` slice when error had occurred and non-nil error was returned.
func HTTPSnapshot(clientParam ClientParam, param SnapshotParam) ([]Snapshot, error) {
	return HTTPSnapshotWithContext(context.Background(), clientParam, param)
}

// HTTPSnapshotWithContext create and return request to Snapshot HTTP-API endpoint with given context.
// Returns nil as `Snapshot` slice when error had occurred and non-nil error was returned.
func HTTPSnapshotWithContext(ctx context.Context, clientParam ClientParam, param SnapshotParam) ([]Snapshot, error) {
	cs, serr := setupClientSetting(clientParam)
	if serr != nil {
		return nil, fmt.Errorf("invalid field in client parameter: %v", serr)
	}
	ss, serr := setupSnapshotSetting(param)
	if serr != nil {
		return nil, fmt.Errorf("invalid field in snapshot parameter: %v", serr)
	}
	return httpSnapshot(ctx, cs, ss)
}

// HTTPSnapshot create and return request to Snapshot HTTP-API endpoint.
// Returns nil as `Snapshot` slice when error had occurred and non-nil error was returned.
func (c *Client) HTTPSnapshot(param SnapshotParam) (snapshots []Snapshot, err error) {
	return c.HTTPSnapshotWithContext(context.Background(), param)
}

// HTTPSnapshotWithContext create and return request to Snapshot HTTP-API endpoint with the given context.
// Returns nil as `Snapshot` slice when error had occurred and non-nil error was returned.
func (c *Client) HTTPSnapshotWithContext(ctx context.Context, param SnapshotParam) (snapshots []Snapshot, err error) {
	ss, err := setupSnapshotSetting(param)
	if err != nil {
		// User can distinguish parameter error from other error.
		return
	}
	return httpSnapshot(ctx, c.setting, ss)
}

// FilterParam is the parameters to make new request to Filter HTTP Endpoint.
type FilterParam struct {
	// What exchange to filter-in.
	Exchange string
	// What channels to filter-in.
	Channels []string
	// Minute of the shard.
	Minute time.Time
	// Start date-time.
	Start *time.Time
	// End date-time.
	End *time.Time
	// What format to get response in.
	Format *string
}

type filterSetting struct {
	exchange string
	channels []string
	minute   int64
	start    *int64
	end      *int64
	format   *string
}

func setupFilterSetting(param FilterParam) (setting filterSetting, err error) {
	if !regexName.MatchString(param.Exchange) {
		err = errors.New("invalid characters in 'Exchange'")
		return
	}
	setting.exchange = param.Exchange
	for _, ch := range setting.channels {
		if !regexName.MatchString(ch) {
			err = errors.New("invalid characters in 'Exchange'")
			return
		}
	}
	// Copy channels from original parameter
	channels := make([]string, len(param.Channels))
	copy(channels, param.Channels)
	setting.channels = channels
	setting.minute = param.Minute.Unix() / 60
	// Optional parameter
	var start int64
	if param.Start != nil {
		start = param.Start.UnixNano()
		setting.start = &start
	}
	// Optional parameter
	var end int64
	if param.End != nil {
		end = param.End.UnixNano()
		setting.end = &end
	}
	if param.Start != nil && param.End != nil && start >= end {
		err = errors.New("'Start' >= 'End'")
	}
	// Optional parameter
	if param.Format != nil {
		if !regexName.MatchString(*param.Format) {
			err = errors.New("invalid characters in 'Format'")
			return
		}
		setting.format = param.Format
	}
	return
}

// httpFilter is internal function for requesting filter HTTP Endpoint
// using settings for both client and filter.
// Returns nil as a slice of `StringLine` if and only if error was not nil.
func httpFilter(ctx context.Context, clientSetting clientSetting, setting filterSetting) ([]StringLine, error) {
	path := fmt.Sprintf("filter/%s/%d", setting.exchange, setting.minute)
	params := make(url.Values)
	// Don't have to copy, this slice is supposed read-only
	params["channels"] = setting.channels
	// Optional parameters
	if setting.start != nil {
		params["start"] = []string{strconv.FormatInt(*setting.start, 10)}
	}
	if setting.end != nil {
		params["end"] = []string{strconv.FormatInt(*setting.end, 10)}
	}
	if setting.format != nil {
		params["format"] = []string{*setting.format}
	}
	// Send a request to server
	statusCode, body, serr := httpDownloadWithTimeout(ctx, clientSetting, path, params)
	if serr != nil {
		return nil, serr
	}
	if statusCode == http.StatusNotFound {
		// Return empty slice if data were not recorded
		return make([]StringLine, 0), nil
	}
	// Conversion to line structs
	// Construct buffered reader from byte slice
	reader := bytes.NewReader(body)
	breader := bufio.NewReader(reader)
	// Slice to store result
	lines := make([]StringLine, 0, 1000)
	for {
		typ, serr := breader.ReadString('\t')
		if serr != nil {
			if serr == io.EOF {
				// EOF at the begining of line, break from loop
				break
			}
			return nil, fmt.Errorf("type read error: %v", serr)
		}
		typ = typ[:len(typ)-1]
		// Strip tab character at the end and convert it to LineType
		lineType := LineType(typ)

		if lineType == LineTypeEnd {
			// End line is an exception as only timestamp is supplied
			timestampStr, serr := breader.ReadString('\n')
			if serr != nil {
				return nil, fmt.Errorf("timestamp read error: %v", serr)
			}
			timestampStr = timestampStr[:len(timestampStr)-1]
			timestamp, serr := strconv.ParseInt(timestampStr, 10, 64)
			if serr != nil {
				return nil, fmt.Errorf("timestamp conversion: %v", serr)
			}
			lines = append(lines, StringLine{
				Exchange:  setting.exchange,
				Type:      lineType,
				Timestamp: timestamp,
				Channel:   nil,
				Message:   nil,
			})
			break
		}
		timestampStr, serr := breader.ReadString('\t')
		if serr != nil {
			return nil, fmt.Errorf("timestamp read error: %v", serr)
		}
		timestampStr = timestampStr[:len(timestampStr)-1]
		timestamp, serr := strconv.ParseInt(timestampStr, 10, 64)
		if serr != nil {
			return nil, fmt.Errorf("timestamp conversion: %v", serr)
		}
		// Read additional values according to LineType
		if lineType == LineTypeMessage || lineType == LineTypeSend {
			channel, serr := breader.ReadString('\t')
			if serr != nil {
				return nil, fmt.Errorf("channel read error: %v", serr)
			}
			channel = channel[:len(channel)-1]
			message, serr := breader.ReadBytes('\n')
			if serr != nil {
				return nil, fmt.Errorf("message read error: %v", serr)
			}
			message = message[:len(message)-1]
			lines = append(lines, StringLine{
				Exchange:  setting.exchange,
				Type:      lineType,
				Timestamp: timestamp,
				Channel:   &channel,
				Message:   message,
			})
		} else if lineType == LineTypeStart || lineType == LineTypeError {
			message, serr := breader.ReadBytes('\n')
			if serr != nil {
				return nil, fmt.Errorf("message read error: %v", serr)
			}
			message = message[:len(message)]
			lines = append(lines, StringLine{
				Exchange:  setting.exchange,
				Type:      lineType,
				Timestamp: timestamp,
				Channel:   nil,
				Message:   message,
			})
		} else {
			return nil, fmt.Errorf("unknown line type: %v", typ)
		}
	}
	return lines, nil
}

// HTTPFilter create and return request to Filter HTTP-API endpoint.
// Returns nil as `StringLine` slice when error had occurred and non-nil error was returned.
func HTTPFilter(clientParam ClientParam, param FilterParam) ([]StringLine, error) {
	return HTTPFilterWithContext(context.Background(), clientParam, param)
}

// HTTPFilterWithContext create and return request to Filter HTTP-API endpoint with the given context.
// Returns nil as `StringLine` slice when error had occurred and non-nil error was returned.
func HTTPFilterWithContext(ctx context.Context, clientParam ClientParam, param FilterParam) ([]StringLine, error) {
	cs, serr := setupClientSetting(clientParam)
	if serr != nil {
		return nil, fmt.Errorf("invalid client parameter: %v", serr)
	}
	fs, serr := setupFilterSetting(param)
	if serr != nil {
		return nil, fmt.Errorf("invalid filter parameter: %v", serr)
	}
	return httpFilter(ctx, cs, fs)
}

// HTTPFilter create and return request to Filter HTTP-API endpoint.
// Returns nil as `StringLine` slice when error had occurred and non-nil error was returned.
func (c *Client) HTTPFilter(param FilterParam) (lines []StringLine, err error) {
	return c.HTTPFilterWithContext(context.Background(), param)
}

// HTTPFilterWithContext create and return request to Filter HTTP-API endpoint with the given context.
// Returns nil as `StringLine` slice when error had occurred and non-nil error was returned.
func (c *Client) HTTPFilterWithContext(ctx context.Context, param FilterParam) (lines []StringLine, err error) {
	fs, err := setupFilterSetting(param)
	if err != nil {
		return
	}
	return httpFilter(ctx, c.setting, fs)
}
