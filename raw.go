package exdgo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// RawRequestParam is the parameters to make new `RawRequest`.
type RawRequestParam struct {
	// Map of exchanges and and its channels to filter-in.
	Filter map[string][]string
	// Start date-time.
	Start time.Time
	// End date-time.
	End time.Time
	// What format to receive response with.
	// If you specify raw, then you will get result in raw format that the exchanges are providing with.
	// If you specify json, then you will get result formatted in JSON format.
	Format *string
}

// RawRequest replays market data in raw format.
//
// You can pick the way to read the response:
// - `download` to immidiately start downloading the whole response as one array.
// - `stream` to return iterable object yields line by line.
type RawRequest struct {
	cliSetting clientSetting
	filter     map[string][]string
	start      int64
	end        int64
	format     *string
}

// setupRawRequest validates parameter and creates new `RawRequest`.
// req is nil if err is reported.
func setupRawRequest(cliSetting clientSetting, param RawRequestParam) (*RawRequest, error) {
	req := new(RawRequest)
	req.cliSetting = cliSetting
	// Copy filter map and validate content at the same time
	filterCopied := make(map[string][]string)
	for exc, chs := range param.Filter {
		// Validate exchange name
		if !regexName.MatchString(exc) {
			return nil, errors.New("invalid characters in exchange in 'Filter'")
		}
		// Validate channel names
		for _, ch := range chs {
			if !regexName.MatchString(ch) {
				return nil, errors.New("invalid characters in channel in 'Filter'")
			}
		}
		// Perform slice copy
		chsCopied := make([]string, len(chs))
		if copied := copy(chsCopied, chs); copied != len(chs) {
			return nil, errors.New("copy of slice failed")
		}
		filterCopied[exc] = chsCopied
	}
	req.filter = filterCopied
	start := param.Start.UnixNano()
	end := param.End.UnixNano()
	if start >= end {
		return nil, errors.New("'Start' >= 'End'")
	}
	req.start = start
	req.end = end
	// Optional parameter
	if param.Format != nil {
		// Validate Format
		if !regexName.MatchString(*param.Format) {
			return nil, errors.New("invalid characters in 'Format'")
		}
		req.format = param.Format
	}
	return req, nil
}

// rawParallelResult is the result of individual execution of an element in queue
type rawParallelResult struct {
	Exchange string
	Index    int
	Shard    []StringLine
	Error    error
}

// Converts snapshots into lines.
// This function is called only once per a request so calling this is not that much of a bottleneck.
func convertSnapshotsToLines(exchange string, snapshots []Snapshot) []StringLine {
	converted := make([]StringLine, len(snapshots))
	for i, ss := range snapshots {
		converted[i] = StringLine{
			Type:      LineTypeMessage,
			Exchange:  exchange,
			Channel:   &ss.Channel,
			Timestamp: ss.Timestamp,
			Message:   ss.Snapshot,
		}
	}
	return converted
}

// rawParallelHTTPSnapshot call httpSnapshot but can be excecuted in paralell way.
// `id` is used to distinguish each goroutine.
func (r RawRequest) rawParallelHTTPSnapshot(ctx context.Context, setting snapshotSetting, id int, wg *sync.WaitGroup, result chan *rawParallelResult) {
	defer wg.Done()
	var serr error
	defer func() {
		// Check if error was reported
		if serr != nil {
			result <- &rawParallelResult{
				Exchange: setting.exchange,
				Index:    id,
				Error:    serr,
			}
		}
	}()
	defer func() {
		if perr := recover(); perr != nil {
			// On case panic happens, serr must be empty
			serr = fmt.Errorf("goroutine panic: %v", perr)
		}
	}()
	ret, serr := httpSnapshot(ctx, r.cliSetting, setting)
	if serr != nil {
		return
	}
	// Real struct is too big to send through channel
	result <- &rawParallelResult{
		Exchange: setting.exchange,
		Index:    0,
		Shard:    convertSnapshotsToLines(setting.exchange, ret),
	}
}

func (r RawRequest) rawParalellHTTPFilter(ctx context.Context, setting filterSetting, id int, wg *sync.WaitGroup, result chan *rawParallelResult) {
	defer wg.Done()
	var serr error
	defer func() {
		// Check if error was reported
		if serr != nil {
			result <- &rawParallelResult{
				Exchange: setting.exchange,
				Index:    id,
				Error:    serr,
			}
		}
	}()
	defer func() {
		if perr := recover(); perr != nil {
			// On case panic happens, serr must be empty
			serr = fmt.Errorf("goroutine panic: %v", perr)
		}
	}()
	ret, serr := httpFilter(ctx, r.cliSetting, setting)
	if serr != nil {
		return
	}
	result <- &rawParallelResult{
		Exchange: setting.exchange,
		Index:    id,
		Shard:    ret,
	}
}

func (r RawRequest) downloadAllShards(ctx context.Context, concurrency int) (map[string][][]StringLine, error) {
	// Calculate the size of the request
	startMinute := r.start / int64(time.Minute)
	// Exclude the exact nanosec of end
	endMinute := (r.end - 1) / int64(time.Minute)
	// Slice to store function to call HTTP Endpoint API
	// The size is exchanges * (snapshot + filter)
	shardsPerExchange := 1 + int(endMinute-startMinute+1)
	callFns := make([]func(context.Context, *sync.WaitGroup, chan *rawParallelResult), len(r.filter)*shardsPerExchange)
	i := 0
	for exchange, channels := range r.filter {
		// Those values have to be preserved to be used later in function
		exc := exchange
		chs := channels

		// Take snapshot of channels
		callFns[i] = func(ctx context.Context, wg *sync.WaitGroup, result chan *rawParallelResult) {
			r.rawParallelHTTPSnapshot(ctx, snapshotSetting{
				exchange: exc,
				channels: chs,
				at:       r.start,
				format:   r.format,
			}, 0, wg, result)
		}
		i++

		// Download the rest of data
		for minute := startMinute; minute <= endMinute; minute++ {
			// Preserve value
			min := minute
			callFns[i] = func(ctx context.Context, wg *sync.WaitGroup, result chan *rawParallelResult) {
				r.rawParalellHTTPFilter(ctx, filterSetting{
					exchange: exc,
					channels: chs,
					start:    &r.start,
					end:      &r.end,
					minute:   min,
					format:   r.format,
				}, int(min-startMinute)+1, wg, result)
			}
			i++
		}
	}

	// Map to store final result
	shards := make(map[string][][]StringLine)
	// Initialize slice in map
	for exchange := range r.filter {
		shards[exchange] = make([][]StringLine, shardsPerExchange)
	}
	// Waitgroup to wait for all goroutine running
	var wg sync.WaitGroup
	// Channel to get result from goroutine
	resultCh := make(chan *rawParallelResult)
	defer func() {
		// Wait for all goroutine to exit
		wg.Wait()
	}()
	// Create child context with cancel function
	childCtx, cancelChild := context.WithCancel(ctx)
	defer cancelChild()

	// Run `concurrency` goroutine at maximum
	// Next index of function in callFuncs to run
	next := 0
	// How much goroutine are successfully executed
	over := 0
	for ; next < concurrency && next < len(callFns); next++ {
		// Run goroutine
		wg.Add(1)
		go callFns[next](childCtx, &wg, resultCh)
	}
	var serr error
	for over < len(callFns) && serr == nil {
		// Wait for at least one goroutine to signal done
		// or for context be cancelled
		select {
		case result := <-resultCh:
			// Execution of a goroutine have finished
			over++
			if result.Error != nil {
				// Error is reported by a goroutine
				// Cancel child context to let all goroutine to stop
				cancelChild()
				serr = result.Error
				// Break from select
				break
			}
			// Store result from goroutine
			shards[result.Exchange][result.Index] = result.Shard
			if next < len(callFns) {
				// There is more work to do, run another goroutine
				wg.Add(1)
				go callFns[next](childCtx, &wg, resultCh)
				next++
			}
		case <-ctx.Done():
			// Store why parent context was cancelled
			serr = ctx.Err()
			// Child context is also cancelled
		}
	}

	// This will run only if error was reported
	for over < len(callFns) {
		// Serves channel message from all goroutine
		// FIXME Ignoring all error and result
		<-resultCh
		over++
	}

	// Wait for all goroutine to stop
	wg.Wait()

	if serr != nil {
		// Error was reported from goroutine
		return nil, serr
	}

	return shards, nil
}

type rawShardsLineIterator struct {
	Shards        [][]StringLine
	ShardPosition int
	Position      int
}

func (i *rawShardsLineIterator) Next() *StringLine {
	for i.ShardPosition < len(i.Shards) && len(i.Shards[i.ShardPosition]) <= i.Position {
		// This shard is all read
		i.ShardPosition++
		i.Position++
	}
	if i.ShardPosition == len(i.Shards) {
		return nil
	}
	line := i.Shards[i.ShardPosition][i.Position]
	i.Position++
	return &line
}

type rawIteratorAndLastLine struct {
	Iterator *rawShardsLineIterator
	LastLine *StringLine
}

// DownloadWithContext sends request and download response in an array with the given context.
// Data are downloaded parallelly by `concurrecy` goroutine.
// Returns slice if and only if error was not reported.
// Otherwise, slice is non-nil.
func (r RawRequest) DownloadWithContext(ctx context.Context, concurrency int) ([]StringLine, error) {
	mapped, serr := r.downloadAllShards(ctx, concurrency)
	if serr != nil {
		return nil, serr
	}
	// Prepare shards line iterator for all exchange
	states := make(map[string]rawIteratorAndLastLine)
	exchanges := make([]string, 0, len(r.filter))
	for exchange, shards := range mapped {
		itr := &rawShardsLineIterator{Shards: shards}
		nxt := itr.Next()
		// If next line does not exist, data for this exchange is empty, ignore
		if nxt != nil {
			exchanges = append(exchanges, exchange)
			states[exchange] = rawIteratorAndLastLine{
				Iterator: itr,
				LastLine: nxt,
			}
		}
	}
	// Process shards into single slice
	resultSize := 0
	for _, shards := range mapped {
		for _, singleShard := range shards {
			resultSize += len(singleShard)
		}
	}
	result := make([]StringLine, resultSize)
	for i := 0; len(exchanges) > 0; i++ {
		// Find the line that have the earliest timestamp
		// Have to set the initial value to calculate the minimum value
		argmin := 0
		mi := states[exchanges[argmin]].LastLine.Timestamp
		for j := 1; j < len(exchanges); j++ {
			exchange := exchanges[j]
			line := states[exchange].LastLine
			if line.Timestamp < mi {
				mi = line.Timestamp
				argmin = i
			}
		}
		state := states[exchanges[argmin]]
		result[i] = *state.LastLine
		nxt := state.Iterator.Next()
		if nxt != nil {
			state.LastLine = nxt
		} else {
			// Next line is absent
			// Remove exchange
			exchanges = append(exchanges[:argmin], exchanges[argmin+1:]...)
		}
	}

	return result, nil
}

// DownloadConcurrency sends request in desired concurrency and download response in an slice.
// Returns slice if and only if error was not reported.
// Otherwise, slice is non-nil.
func (r RawRequest) DownloadConcurrency(concurrency int) ([]StringLine, error) {
	return r.DownloadWithContext(context.Background(), concurrency)
}

// Download sends request and download response in an slice.
// Returns slice if and only if error was not reported.
// Otherwise, slice is non-nil.
func (r RawRequest) Download() ([]StringLine, error) {
	return r.DownloadWithContext(context.Background(), downloadBatchSize)
}

// Stream sends request to server and returns iterator to read response.
//
// Returns object yields response line by line.
// Iterator yields immidiately if a line is bufferred, waits for download if not avaliable.
//
// **Please note that buffering won't start by calling this method, calling {@link AsyncIterator.next()} will.**
//
// Higher responsiveness than `download` is expected as it does not have to wait for
// the entire data to be downloaded.
//
// bufferSize is optional. Desired buffer size to store streaming data. One shard is equavalent to one minute.
// func (r *RawRequest) Stream(bufferSize int) []StringLine {
// 	return r.StreamWithContext(context.Background())
// }

// // StreamWithContext sends request to server with the given context and returns iterator to read response.
// func (r *RawRequest) StreamWithContext(ctx context.Context) []StringLine {

// }

// Raw creates new `RawRequest` with the given parameters and returns its pointer.
// `*RawRequest` is nil if error was returned.
func Raw(clientParam ClientParam, param RawRequestParam) (*RawRequest, error) {
	cliSetting, serr := setupClientSetting(clientParam)
	if serr != nil {
		return nil, serr
	}
	return setupRawRequest(cliSetting, param)
}

// Raw is a lower-level API that processes data from Exchangedataset HTTP-API and generate raw (close to exchanges' format) data.
// Returns pointer to new `RawRequest` if and only if error was not returned.
// Otherwise, it's nil.
func (c *Client) Raw(param RawRequestParam) (*RawRequest, error) {
	return setupRawRequest(c.setting, param)
}
