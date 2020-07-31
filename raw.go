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

const (
	rawDownloadJobSnapshot = iota
	rawDonwloadJobFilter
)

// rawDownloadJob is the job used in rawDownloadWorker.
type rawDownloadJob struct {
	// Type of job.
	// Snapshot or filter.
	typ int
	// Setting for job.
	setting interface{}
}

type rawDownloadJobResult struct {
	job    *rawDownloadJob
	result []StringLine
}

// Works on job provided by `jobs` channel.
// `err` is used for multiple workers, so it won't be closed from worker.
// Instead, worker will close `stopped` channel to let others know that this worker had stopped.
func rawDownloadWorker(ctx context.Context, cliSetting clientSetting, jobs chan *rawDownloadJob, results chan *rawDownloadJobResult, err chan error, stopped chan struct{}) {
	defer close(stopped)
	// Do job if it can and jobs are available
	for job := range jobs {
		if job.typ == rawDownloadJobSnapshot {
			setting := job.setting.(snapshotSetting)
			ret, serr := httpSnapshot(ctx, cliSetting, setting)
			if serr != nil {
				err <- serr
			}
			// Real struct is too big to send through channel
			results <- &rawDownloadJobResult{
				job:    job,
				result: convertSnapshotsToLines(setting.exchange, ret),
			}
		} else if job.typ == rawDonwloadJobFilter {
			setting := job.setting.(filterSetting)
			ret, serr := httpFilter(ctx, cliSetting, setting)
			if serr != nil {
				err <- serr
			}
			// Real struct is too big to send through channel
			results <- &rawDownloadJobResult{
				job:    job,
				result: ret,
			}
		} else {
			err <- errors.New("unknown download job type")
			return
		}
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
	amountOfJobs := len(r.filter) * shardsPerExchange

	// Channel to get error from worker, used in all workers
	errCh := make(chan error)
	defer close(errCh)
	resultsCh := make(chan *rawDownloadJobResult)
	defer close(resultsCh)
	// Context for all worker
	childCtx, cancelChild := context.WithCancel(ctx)
	defer cancelChild()
	// Worker pool are the slice of channel that gets closed when a worker had stopped
	workerPool := make([]chan struct{}, concurrency)
	defer func() {
		// Defer function prevents worker from being left alone
		// This will stop http query
		cancelChild()
		for _, worker := range workerPool {
			// Wait for the worker to stop
			select {
			case <-worker:
				// This two are needed to unblock channels so a worker can stop
				// which not neccesarily the worker that we are waiting for
			case <-errCh:
			case <-resultsCh:
				// Jobs channels are already closed, so we don't have to think about it
			}
		}
	}()
	// Channel to send workers jobs, it won't get blocked by sending
	jobsCh := make(chan *rawDownloadJob, amountOfJobs)
	defer close(jobsCh)
	// Run all worker
	for i := 0; i < concurrency; i++ {
		workerPool[i] = make(chan struct{})
		go rawDownloadWorker(childCtx, r.cliSetting, jobsCh, resultsCh, errCh, workerPool[i])
	}

	// Send jobs to worker
	for exchange, channels := range r.filter {
		// Take snapshot of channels
		// FIXME Can change to blocking one if we use select
		jobsCh <- &rawDownloadJob{
			typ: rawDownloadJobSnapshot,
			setting: snapshotSetting{
				exchange: exchange,
				channels: channels,
				at:       r.start,
				format:   r.format,
			},
		}

		// Download the rest of data
		for minute := startMinute; minute <= endMinute; minute++ {
			jobsCh <- &rawDownloadJob{
				typ: rawDownloadJobSnapshot,
				setting: filterSetting{
					exchange: exchange,
					channels: channels,
					start:    &r.start,
					end:      &r.end,
					minute:   minute,
					format:   r.format,
				},
			}
		}
	}

	// Map to store final result
	shards := make(map[string][][]StringLine)
	// Initialize slice in map
	for exchange := range r.filter {
		shards[exchange] = make([][]StringLine, shardsPerExchange)
	}

	// How many jobs it have done
	over := 0
	for over >= amountOfJobs {
		select {
		case result := <-resultsCh:
			if result.job.typ == rawDownloadJobSnapshot {
				setting := result.job.setting.(snapshotSetting)
				shards[setting.exchange][0] = result.result
			} else if result.job.typ == rawDonwloadJobFilter {
				setting := result.job.setting.(filterSetting)
				shards[setting.exchange][setting.minute-startMinute+1] = result.result
			} else {
				return nil, errors.New("unknown download job type")
			}
			over++
		case serr := <-errCh:
			return nil, fmt.Errorf("worker: %v", serr)
		case <-ctx.Done():
			// Context is cancelled
			return nil, fmt.Errorf("context done: %v", ctx.Err())
		}
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

type rawDownloadIteratorAndLastLine struct {
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
	states := make(map[string]rawDownloadIteratorAndLastLine)
	exchanges := make([]string, 0, len(r.filter))
	for exchange, shards := range mapped {
		itr := &rawShardsLineIterator{Shards: shards}
		nxt := itr.Next()
		// If next line does not exist, data for this exchange is empty, ignore
		if nxt != nil {
			exchanges = append(exchanges, exchange)
			states[exchange] = rawDownloadIteratorAndLastLine{
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

type rawStreamShardSlot struct {
	// This channel will be closed to notify the goroutine who are responsible to set shard to
	// this slot is stopped
	downloaderStopped chan struct{}
	shard             []StringLine
}

type rawExchageStreamShardIterator struct {
	// Must be initilized before init()
	request *RawRequest
	// Must be initilized before init()
	exchange string
	// Must be initilized before init()
	buffer   [][]StringLine
	position int
	// If set, this channel will get closed by a background goroutine
	// when the shard on current position is set on the buffer and became available
	notifier chan struct{}
	// Lock on the buffer and notifier
	lock sync.Mutex
	// Context background goroutine will run on
	bgCtx context.Context
	// Function to cancel background context
	cancelBGCtx func()
	// Channel to receive error from background goroutine
	bgerr              chan error
	nextDownloadMinute int64
	startMinute        int64
	endMinute          int64
}

// downloadSnapshot is a goroutine to download a snapshot shard parallelly
func (i *rawExchageStreamShardIterator) downloadSnapshot(setPosition int) {
	result, serr := httpSnapshot(i.bgCtx, i.request.cliSetting, snapshotSetting{
		exchange: i.exchange,
		channels: i.request.filter[i.exchange],
		at:       i.request.start,
		format:   i.request.format,
	})
	if serr != nil {
		i.bgerr <- serr
	}
	// Lock when modify buffer
	i.lock.Lock()
	// Ensure unlocking
	defer i.lock.Unlock()
	i.buffer[setPosition%len(i.buffer)] = convertSnapshotsToLines(i.exchange, result)
	if i.position == setPosition && i.notifier != nil {
		close(i.notifier)
	}
}

// downloadSnapshot is a goroutine to download a filter shard parallelly
func (i *rawExchageStreamShardIterator) downloadFilter(setPosition int, minute int64) {
	result, serr := httpFilter(i.bgCtx, i.request.cliSetting, filterSetting{
		exchange: i.exchange,
		channels: i.request.filter[i.exchange],
		minute:   minute,
		start:    &i.request.start,
		end:      &i.request.end,
		format:   i.request.format,
	})
	if serr != nil {
		i.bgerr <- serr
	}
	// Lock when modify buffer
	i.lock.Lock()
	// Ensure unlocking
	defer i.lock.Unlock()
	i.buffer[setPosition%len(i.buffer)] = result
	if i.notifier != nil {
		close(i.notifier)
	}
}

// init initialize this iterator by setting field value and start downloading
// to fill buffer.
// Context given will be used by this iterator for its lifetime.
// This includes `next()` and other operations.
func (i *rawExchageStreamShardIterator) init(ctx context.Context) {
	i.startMinute = i.request.start / int64(time.Minute)
	i.nextDownloadMinute = i.startMinute
	// End is exclusive
	i.endMinute = (i.request.end - 1) / int64(time.Minute)
	i.bgCtx, i.cancelBGCtx = context.WithCancel(ctx)
	i.bgerr = make(chan error)
	// Fill the buffer
	go i.downloadSnapshot(0)
	for j := 0; j < len(i.buffer)-1; j++ {
		minute := i.startMinute + int64(j)
		go i.downloadFilter(j+1, minute)
		i.nextDownloadMinute++
	}
}

// next returns the next shard on this iterator.
//
// Returns a shard immediately if it is buffered, if not, it waits for
// the shard to be available.
//
// Returns error if background download goroutines had encountered an error which has not been reported yet.
//
// This function runs on the context which was given when an iterator was initialized.
func (i *rawExchageStreamShardIterator) next() ([]StringLine, error) {
	minute := i.startMinute + int64(len(i.buffer))
	if minute > i.endMinute {
		// No bufferred shard in buffer
		return nil, nil
	}
	// Acquire lock on buffer and notifier
	i.lock.Lock()
	if i.buffer[i.position%len(i.buffer)] == nil {
		// Shard has not been downloaded yet
		// Set notifier and wait for it
		i.notifier = make(chan struct{})
		// Don't forget to unlock
		i.lock.Unlock()
		select {
		case <-i.notifier:
			i.lock.Lock()
			// Let go
		case serr := <-i.bgerr:
			// Error reported from background
			return nil, serr
		}
	}
	// At this point, lock is always active
	defer i.lock.Unlock()
	shard := i.buffer[i.position]
	i.position++
	if i.nextDownloadMinute <= i.endMinute {
		// It have to buffer more shards
		go i.downloadFilter(i.position+len(i.buffer), i.nextDownloadMinute)
		i.nextDownloadMinute++
	}
	return shard, nil
}

// close stops goroutine used by this iterator
// After non-nil return from the call, this iterator could be
// safely ignored until gc will take care of them.
func (i *rawExchageStreamShardIterator) close() error {
	// Cancelling context will stop goroutine
	i.cancelBGCtx()
	// Wait for all goroutine to stop

	// Close channels
	close(i.bgerr)
}

type rawExchangeStreamIterator struct {
	request  *RawRequest
	shard    []StringLine
	position int
}

type rawStreamIteratorAndLastLine struct {
	iterator rawExchangeStreamIterator
	lastLine *StringLine
}

type rawStreamIterator struct {
	request   *RawRequest
	state     rawStreamIteratorAndLastLine
	exchanges []string
}

func (i rawStreamIterator) next() StringLine {
	if len(i.exchanges) == 0 {

	}
}

func newRawStreamIterator(cliSetting clientSetting) {
	itr := new(rawStreamIterator)
	itr.cliSetting = cliSetting
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
