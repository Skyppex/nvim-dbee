package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type (
	CallID string

	Call struct {
		id        CallID
		query     string
		state     CallState
		timeTaken time.Duration
		timestamp time.Time

		results    []*Result
		archives   []*archive
		cancelFunc func()

		// any error that might occur during execution
		err  error
		done chan struct{}
	}
)

// callPersistent is used for marshaling and unmarshaling the call
type callPersistent struct {
	ID        string `json:"id"`
	Query     string `json:"query"`
	State     string `json:"state"`
	TimeTaken int64  `json:"time_taken_us"`
	Timestamp int64  `json:"timestamp_us"`
	Error     string `json:"error,omitempty"`
}

func (c *Call) toPersistent() *callPersistent {
	errMsg := ""
	if c.err != nil {
		errMsg = c.err.Error()
	}

	return &callPersistent{
		ID:        string(c.id),
		Query:     c.query,
		State:     c.state.String(),
		TimeTaken: c.timeTaken.Microseconds(),
		Timestamp: c.timestamp.UnixMicro(),
		Error:     errMsg,
	}
}

func (s *Call) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.toPersistent())
}

func (c *Call) UnmarshalJSON(data []byte) error {
	var alias callPersistent

	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}

	done := make(chan struct{})
	close(done)

	archives := newArchives(CallID(alias.ID))
	state := CallStateFromString(alias.State)
	if state == CallStateArchived && len(archives) == 0 {
		state = CallStateUnknown
	}

	var callErr error
	if alias.Error != "" {
		callErr = errors.New(alias.Error)
	}

	// Initialize results slice with same length as archives
	results := make([]*Result, len(archives))
	for i := range results {
		results[i] = new(Result)
	}
	// Ensure at least one result slot
	if len(results) == 0 {
		results = []*Result{new(Result)}
		archives = []*archive{newArchive(CallID(alias.ID), 0)}
	}

	*c = Call{
		id:        CallID(alias.ID),
		query:     alias.Query,
		state:     state,
		timeTaken: time.Duration(alias.TimeTaken) * time.Microsecond,
		timestamp: time.UnixMicro(alias.Timestamp),
		err:       callErr,

		results:  results,
		archives: archives,

		done: done,
	}

	return nil
}

func newCallFromExecutor(executor func(context.Context) ([]ResultStream, error), query string, onEvent func(CallState, *Call)) *Call {
	id := CallID(uuid.New().String())
	c := &Call{
		id:    id,
		query: query,
		state: CallStateUnknown,

		results:  []*Result{new(Result)},
		archives: []*archive{newArchive(id, 0)},

		done: make(chan struct{}),
	}

	eventsCh := make(chan CallState, 10)

	ctx, cancel := context.WithCancel(context.Background())
	c.timestamp = time.Now()
	c.cancelFunc = func() {
		cancel()
		c.timeTaken = time.Since(c.timestamp)
		eventsCh <- CallStateCanceled
	}

	// event function handler
	go func() {
		for state := range eventsCh {
			if c.state == CallStateExecutingFailed ||
				c.state == CallStateRetrievingFailed ||
				c.state == CallStateCanceled {
				return
			}
			c.state = state

			// trigger event callback
			if onEvent != nil {
				onEvent(state, c)
			}
		}
	}()

	go func() {
		defer close(eventsCh)

		// execute the function
		eventsCh <- CallStateExecuting
		iters, err := executor(ctx)
		if err != nil {
			c.timeTaken = time.Since(c.timestamp)
			c.err = err
			eventsCh <- CallStateExecutingFailed
			close(c.done)
			return
		}

		// Initialize results and archives for all result sets
		c.results = make([]*Result, len(iters))
		c.archives = make([]*archive, len(iters))
		for i := range iters {
			c.results[i] = new(Result)
			c.archives[i] = newArchive(id, i)
		}

		// Process each result set
		for i, iter := range iters {
			// set iterator to result (only fire retrieving event on first result)
			var onFirstRow func()
			if i == 0 {
				onFirstRow = func() { eventsCh <- CallStateRetrieving }
			}
			err = c.results[i].SetIter(iter, onFirstRow)
			if err != nil {
				c.timeTaken = time.Since(c.timestamp)
				c.err = err
				eventsCh <- CallStateRetrievingFailed
				close(c.done)
				return
			}

			// archive the result
			err = c.archives[i].setResult(c.results[i])
			if err != nil {
				c.timeTaken = time.Since(c.timestamp)
				c.err = err
				eventsCh <- CallStateArchiveFailed
				close(c.done)
				return
			}
		}

		c.timeTaken = time.Since(c.timestamp)
		eventsCh <- CallStateArchived
		close(c.done)
	}()

	return c
}

func (c *Call) GetID() CallID {
	return c.id
}

func (c *Call) GetQuery() string {
	return c.query
}

func (c *Call) GetState() CallState {
	return c.state
}

func (c *Call) GetTimeTaken() time.Duration {
	return c.timeTaken
}

func (c *Call) GetTimestamp() time.Time {
	return c.timestamp
}

func (c *Call) Err() error {
	return c.err
}

// Done returns a non-buffered channel that is closed when
// call finishes.
func (c *Call) Done() chan struct{} {
	return c.done
}

func (c *Call) Cancel() {
	if c.state > CallStateExecuting {
		return
	}
	if c.cancelFunc != nil {
		c.cancelFunc()
	}
}

// GetResult returns the result at the given index.
// If index is out of range, returns an error.
func (c *Call) GetResult(index int) (*Result, error) {
	if index < 0 || index >= len(c.results) {
		return nil, fmt.Errorf("result index %d out of range (0-%d)", index, len(c.results)-1)
	}

	if c.results[index].IsEmpty() {
		iter, err := c.archives[index].getResult()
		if err != nil {
			return nil, fmt.Errorf("c.archives[%d].getResult: %w", index, err)
		}
		err = c.results[index].SetIter(iter, nil)
		if err != nil {
			return nil, fmt.Errorf("c.results[%d].setIter: %w", index, err)
		}
	}

	return c.results[index], nil
}

// ResultCount returns the number of result sets in this call.
func (c *Call) ResultCount() int {
	return len(c.results)
}
