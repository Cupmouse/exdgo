package exdgo

import (
	"context"
	"errors"
	"fmt"
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
// There is two ways of reading the response:
// - `download` to immidiately start downloading the whole response as one array.
// - `stream` to return iterable object yields line by line.
type RawRequest struct {
	cli    *Client
	filter map[string][]string
	start  int64
	end    int64
	format *string
}

// setupRawRequest validates parameter and creates new `RawRequest`.
// req is nil if err is reported.
func setupRawRequest(cli *Client, param RawRequestParam) (*RawRequest, error) {
	if cli == nil {
		return nil, errors.New("'cli' can not be nil")
	}
	req := new(RawRequest)
	req.cli = cli
	var serr error
	req.filter, serr = copyFilter(param.Filter)
	if serr != nil {
		return nil, fmt.Errorf("Filter: %v", serr)
	}
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
func rawDownloadWorker(ctx context.Context, cli *Client, jobs chan *rawDownloadJob, results chan *rawDownloadJobResult, err chan error, stopped chan struct{}) {
	defer close(stopped)
	// Do job if it can and jobs are available
	for job := range jobs {
		if job.typ == rawDownloadJobSnapshot {
			setting := job.setting.(snapshotSetting)
			ret, serr := httpSnapshot(ctx, cli, setting)
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
			ret, serr := httpFilter(ctx, cli, setting)
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

func (r *RawRequest) downloadAllShards(ctx context.Context, concurrency int) (map[string][][]StringLine, error) {
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
		go rawDownloadWorker(childCtx, r.cli, jobsCh, resultsCh, errCh, workerPool[i])
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
				typ: rawDonwloadJobFilter,
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

	// How many jobs has been done
	over := 0
	for over < amountOfJobs {
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
	shards        [][]StringLine
	shardPosition int
	position      int
}

func (i *rawShardsLineIterator) Next() *StringLine {
	for i.shardPosition < len(i.shards) && len(i.shards[i.shardPosition]) <= i.position {
		// This shard is all read
		i.shardPosition++
		i.position = 0
	}
	if i.shardPosition == len(i.shards) {
		return nil
	}
	line := i.shards[i.shardPosition][i.position]
	i.position++
	return &line
}

type rawDownloadIteratorAndLastLine struct {
	Iterator *rawShardsLineIterator
	LastLine *StringLine
}

// DownloadWithContext is same as `Download()`, but sends requests in given concurrency
// in given context.
func (r *RawRequest) DownloadWithContext(ctx context.Context, concurrency int) ([]StringLine, error) {
	mapped, serr := r.downloadAllShards(ctx, concurrency)
	if serr != nil {
		return nil, serr
	}
	// Prepare shards line iterator for all exchange
	states := make(map[string]*rawDownloadIteratorAndLastLine)
	exchanges := make([]string, 0, len(r.filter))
	for exchange, shards := range mapped {
		itr := &rawShardsLineIterator{shards: shards}
		nxt := itr.Next()
		// If next line does not exist, data for this exchange is empty, ignore
		if nxt != nil {
			exchanges = append(exchanges, exchange)
			states[exchange] = &rawDownloadIteratorAndLastLine{
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
				argmin = j
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

// DownloadConcurrency is same as `Download()`, but sends requests in given concurrency.
func (r *RawRequest) DownloadConcurrency(concurrency int) ([]StringLine, error) {
	return r.DownloadWithContext(context.Background(), concurrency)
}

// Download sends request and download response in an slice.
// Returns slice if and only if error was not reported.
// Otherwise, slice is non-nil.
func (r *RawRequest) Download() ([]StringLine, error) {
	return r.DownloadWithContext(context.Background(), downloadBatchSize)
}

type rawStreamShardResult struct {
	shard []StringLine
	// Index of buffer to put this result
	index int
	// This is non-nil if and only if shard is nil
	err error
}

// rawExchangeStreamShardIterator is the iterator that yields a shard
// of a certain exchange one by one.
// Always have to be closed after initialized.
//
// One and other multiple goroutine will be spawned by initializing,
// the main goroutine will serve as a manager for all of individual
// download routine and is called background, others are called download.
type rawExchangeStreamShardIterator struct {
	request    *RawRequest
	exchange   string
	bufferSize int
	// Channel to get result from background goroutine
	results chan []StringLine
	// Channel to receive error from background goroutine
	bgErr chan error
	// Cancels context background runs on
	cancelBGCtx context.CancelFunc
}

func (i *rawExchangeStreamShardIterator) downloadSnapshot(ctx context.Context, results chan *rawStreamShardResult) {
	result, serr := httpSnapshot(ctx, i.request.cli, snapshotSetting{
		exchange: i.exchange,
		channels: i.request.filter[i.exchange],
		at:       i.request.start,
		format:   i.request.format,
	})
	if serr != nil {
		results <- &rawStreamShardResult{err: serr}
		return
	}
	results <- &rawStreamShardResult{
		index: 0,
		shard: convertSnapshotsToLines(i.exchange, result),
	}
}

func (i *rawExchangeStreamShardIterator) downloadFilter(ctx context.Context, minute int64, index int, results chan *rawStreamShardResult) {
	result, serr := httpFilter(ctx, i.request.cli, filterSetting{
		exchange: i.exchange,
		channels: i.request.filter[i.exchange],
		minute:   minute,
		start:    &i.request.start,
		end:      &i.request.end,
		format:   i.request.format,
	})
	if serr != nil {
		results <- &rawStreamShardResult{err: serr}
		return
	}
	results <- &rawStreamShardResult{
		index: index,
		shard: result,
	}
}

// background is the goroutine to manage all download goroutine associated with this iterator.
// The goroutine will run on the context given, and stops its execution if the context was cancelled.
// out should be put to a results field in `rawExchageStreamShardIterator` by the caller.
func (i *rawExchangeStreamShardIterator) background(ctx context.Context, out chan []StringLine, err chan error) {
	defer close(out)
	defer close(err)
	startMinute := i.request.start / int64(time.Minute)
	nextMinute := startMinute
	// End is exclusive
	endMinute := (i.request.end - 1) / int64(time.Minute)
	results := make(chan *rawStreamShardResult)
	defer close(results)
	// Buffer to store results from download goroutines
	buffer := make([][]StringLine, i.bufferSize)
	// Current read position in the buffer
	position := 0
	// Number of running background goroutines
	// This routine will not stop until this value is 0
	running := 0
	defer func() {
		// Wait for all download routines to stop
		// Download goroutines stop either by throwing an error or returning a result
		for running > 0 {
			// Ignoring errors and results
			<-results
			running--
		}
	}()
	// Context for download routine
	downloadCtx, cancelDLCtx := context.WithCancel(ctx)
	defer cancelDLCtx()
	// Run snapshot download
	go i.downloadSnapshot(downloadCtx, results)
	running++
	// Run initial filter downloads
	for j := 1; j < i.bufferSize && nextMinute <= endMinute; j++ {
		// Execute download routine
		go i.downloadFilter(downloadCtx, nextMinute, int(nextMinute-startMinute+1), results)
		running++
		nextMinute++
	}
	// Set this flag true to stop this loop
	stop := false
	for !stop {
		if buffer[position%i.bufferSize] == nil {
			select {
			case res := <-results:
				// Got a result or an error
				running--
				if res.err != nil {
					// Received an error
					stop = true
					err <- fmt.Errorf("download: %v", res.err)
					// Cancel download routine context to stop them
					cancelDLCtx()
				}
				// Set shard in the buffer
				buffer[res.index%i.bufferSize] = res.shard
			case <-ctx.Done():
				// Context is cancelled
				stop = true
				err <- fmt.Errorf("context: %v", ctx.Err())
			}
		} else {
			select {
			case res := <-results:
				running--
				if res.err != nil {
					stop = true
					err <- fmt.Errorf("download: %v", res.err)
					cancelDLCtx()
				}
				buffer[res.index%i.bufferSize] = res.shard
			case out <- buffer[position%i.bufferSize]:
				buffer[position%i.bufferSize] = nil
				if nextMinute <= endMinute {
					go i.downloadFilter(downloadCtx, nextMinute, int(nextMinute-startMinute+1), results)
					nextMinute++
					running++
				} else if int64(position-1)+startMinute == endMinute {
					// Last shard was returned
					// This background routine's job is done
					stop = true
				}
				position++
			case <-ctx.Done():
				stop = true
				err <- fmt.Errorf("context: %v", ctx.Err())
			}
		}
	}
	// Stop is gracelly handled by defer functions defined before
}

// init initialize this iterator by setting field value and start downloading
// to fill buffer.
// Context given will be used by this iterator for its lifetime.
// This includes `next()` and other operations.
func newRawExchangeStreamShardIterator(ctx context.Context, request *RawRequest, exchange string, bufferSize int) *rawExchangeStreamShardIterator {
	i := new(rawExchangeStreamShardIterator)
	i.request = request
	i.exchange = exchange
	i.bufferSize = bufferSize
	// Make child context for background
	var childCtx context.Context
	childCtx, i.cancelBGCtx = context.WithCancel(ctx)
	// Make channels for communication
	i.results = make(chan []StringLine)
	i.bgErr = make(chan error)
	// Run background routine
	go i.background(childCtx, i.results, i.bgErr)
	return i
}

// next returns the next shard on this iterator.
//
// Returns a shard immediately if it is buffered or reached the end, if not, it waits for
// the shard to be available.
//
// Returns error if background download goroutines had encountered an error which has not been reported yet.
// Returns nil if next shard is absent.
//
// This function runs on the context which was given when an iterator was initialized.
func (i *rawExchangeStreamShardIterator) next() ([]StringLine, error) {
	// Check for background error
	select {
	case serr, ok := <-i.bgErr:
		if ok {
			return nil, serr
		}
		// Channel is closed
	case result := <-i.results:
		// result is nil if the channel is already closed
		return result, nil
	}
	// No next line to return
	return nil, nil
}

// close stops goroutine used by this iterator
// After non-nil return from the call, this iterator could be
// safely ignored until gc will take care of them.
// Returns error if there is an unreported error from background.
func (i *rawExchangeStreamShardIterator) close() error {
	// This will stop background
	i.cancelBGCtx()
	// Error channel will either be closed or return error
	serr, ok := <-i.bgErr
	if ok {
		// Report error from background
		return serr
	}
	return nil
}

type rawExchangeStreamIterator struct {
	shardIterator *rawExchangeStreamShardIterator
	shard         []StringLine
	position      int
}

func newRawExchangeStreamIterator(ctx context.Context, request *RawRequest, exchange string, bufferSize int) (*rawExchangeStreamIterator, error) {
	i := new(rawExchangeStreamIterator)
	i.shardIterator = newRawExchangeStreamShardIterator(ctx, request, exchange, bufferSize)
	// Get the very first shard
	var serr error
	i.shard, serr = i.shardIterator.next()
	if serr != nil {
		return nil, serr
	}
	return i, nil
}

func (i *rawExchangeStreamIterator) next() (*StringLine, error) {
	// Skip shards does not have any more lines (or empty) as long as available
	for i.shard != nil && len(i.shard) <= i.position {
		var serr error
		i.shard, serr = i.shardIterator.next()
		i.position = 0
		if serr != nil {
			return nil, serr
		}
	}
	if i.shard == nil {
		// Reached the last line
		return nil, nil
	}
	line := &i.shard[i.position]
	i.position++
	return line, nil
}

func (i *rawExchangeStreamIterator) close() error {
	return i.shardIterator.close()
}

type rawStreamIteratorAndLastLine struct {
	iterator *rawExchangeStreamIterator
	lastLine *StringLine
}

type rawStreamIterator struct {
	// Map of exchange vs struct
	states    map[string]*rawStreamIteratorAndLastLine
	exchanges []string
}

func newRawStreamIterator(ctx context.Context, request *RawRequest, bufferSize int) (*rawStreamIterator, error) {
	i := new(rawStreamIterator)
	i.states = make(map[string]*rawStreamIteratorAndLastLine)
	i.exchanges = make([]string, 0, len(request.filter))
	for exchange := range request.filter {
		iterator, serr := newRawExchangeStreamIterator(ctx, request, exchange, bufferSize)
		if serr != nil {
			return nil, serr
		}
		next, serr := iterator.next()
		if serr != nil {
			return nil, serr
		}
		// Skip if an exchange iterator returns no line
		if next == nil {
			continue
		}
		i.states[exchange] = &rawStreamIteratorAndLastLine{
			iterator: iterator,
			lastLine: next,
		}
		i.exchanges = append(i.exchanges, exchange)
	}
	return i, nil
}

func (i *rawStreamIterator) Next() (next *StringLine, ok bool, err error) {
	if len(i.exchanges) == 0 {
		// All lines returned
		return nil, false, nil
	}
	// Return the line that has the smallest timestamp across exchanges
	argmin := 0
	min := i.states[i.exchanges[argmin]].lastLine.Timestamp
	for j := 1; j < len(i.exchanges); j++ {
		lastLine := i.states[i.exchanges[j]].lastLine
		if lastLine.Timestamp < min {
			argmin = j
			min = lastLine.Timestamp
		}
	}
	// Get the next line for this exchange
	state := i.states[i.exchanges[argmin]]
	line := state.lastLine
	next, serr := state.iterator.next()
	if serr != nil {
		return nil, false, serr
	}
	if next == nil {
		// There is no next line, remove this exchange from the list
		i.exchanges = append(i.exchanges[:argmin], i.exchanges[argmin+1:]...)
		serr := state.iterator.close()
		if serr != nil {
			return nil, false, serr
		}
	}
	state.lastLine = next
	return line, true, nil
}

func (i *rawStreamIterator) Close() error {
	var serr error
	for _, exchange := range i.exchanges {
		// This will ignore errors other then the first one
		if serr == nil {
			// This could return error
			serr = i.states[exchange].iterator.close()
		} else {
			// Ignore other errors
			i.states[exchange].iterator.close()
		}
	}
	if serr != nil {
		return serr
	}
	return nil
}

// StringLineIterator is the interface of iterator which yields `*StringLine`.
type StringLineIterator interface {
	// Next returns the next line from the iterator.
	// If the next line exists, `ok` is true ad `line` is non-nil, otherwise false and `line` is nil.
	// `ok` is false if an error was returned.
	Next() (line *StringLine, ok bool, err error)

	// Close frees resources this iterator is using.
	// **Must** always be called after the use of this iterator.
	Close() error
}

// Stream sends requests to the server and returns an iterator for reading the response.
//
// Returns an iterator yields line by line if and only if an error is not returned.
// The iterator yields immidiately if a line is bufferred, waits for download if not avaliable.
//
// Lines will be bufferred immidiately after calling this function.
//
// Higher responsiveness than `download` is expected as it does not have to wait for
// the entire data to be downloaded.
func (r *RawRequest) Stream() (StringLineIterator, error) {
	return r.StreamWithContext(context.Background(), defaultBufferSize)
}

// StreamBufferSize is same as Stream but with custom bufferSize.
// `bufferSize` is the desired buffer size to store streaming data.
// One shard is equavalent to one minute.
func (r *RawRequest) StreamBufferSize(bufferSize int) (StringLineIterator, error) {
	return r.StreamWithContext(context.Background(), bufferSize)
}

// StreamWithContext is same as `Stream` but a context can be given.
// Background downloads and the returned iterator will use the context for their lifetime.
// Cancelling the context will stop running background downloads, and future `next` calls to the iterator might produce error.
func (r *RawRequest) StreamWithContext(ctx context.Context, bufferSize int) (StringLineIterator, error) {
	itr, serr := newRawStreamIterator(ctx, r, bufferSize)
	if serr != nil {
		return nil, serr
	}
	return itr, nil
}

// Raw creates new `RawRequest` with the given parameters and returns its pointer.
// `*RawRequest` is nil if an error was returned.
func Raw(clientParam ClientParam, param RawRequestParam) (*RawRequest, error) {
	cliSetting, serr := setupClient(clientParam)
	if serr != nil {
		return nil, serr
	}
	return setupRawRequest(&cliSetting, param)
}

// Raw is a lower-level API that processes data from Exchangedataset HTTP-API and generate raw (close to exchanges' format) data.
// Returns a pointer to new `RawRequest` if and only if an error was not returned.
// Otherwise, it's nil.
func (c *Client) Raw(param RawRequestParam) (*RawRequest, error) {
	return setupRawRequest(c, param)
}
