package builders

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/kndndrj/nvim-dbee/dbee/core"
)

// default sql client used by other specific implementations
type Client struct {
	db             *sql.DB
	typeProcessors map[string]func(any) any

	// Transaction support
	tx   *sql.Tx
	txMu sync.Mutex
}

func NewClient(db *sql.DB, opts ...ClientOption) *Client {
	config := clientConfig{
		typeProcessors: make(map[string]func(any) any),
	}
	for _, opt := range opts {
		opt(&config)
	}

	return &Client{
		db:             db,
		typeProcessors: config.typeProcessors,
	}
}

func (c *Client) Close() {
	c.txMu.Lock()
	defer c.txMu.Unlock()

	// Rollback any active transaction before closing
	if c.tx != nil {
		_ = c.tx.Rollback()
		c.tx = nil
	}
	c.db.Close()
}

// BeginTransaction starts a new database transaction.
func (c *Client) BeginTransaction() error {
	c.txMu.Lock()
	defer c.txMu.Unlock()

	if c.tx != nil {
		return core.ErrTransactionAlreadyActive
	}

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	c.tx = tx
	return nil
}

// CommitTransaction commits the current transaction.
func (c *Client) CommitTransaction() error {
	c.txMu.Lock()
	defer c.txMu.Unlock()

	if c.tx == nil {
		return core.ErrNoActiveTransaction
	}

	err := c.tx.Commit()
	c.tx = nil
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// RollbackTransaction rolls back the current transaction.
func (c *Client) RollbackTransaction() error {
	c.txMu.Lock()
	defer c.txMu.Unlock()

	if c.tx == nil {
		return core.ErrNoActiveTransaction
	}

	err := c.tx.Rollback()
	c.tx = nil
	if err != nil {
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}

	return nil
}

// HasActiveTransaction returns true if there's an active transaction.
func (c *Client) HasActiveTransaction() bool {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	return c.tx != nil
}

// Swap swaps current database connection for another one
// and closes the old one.
func (c *Client) Swap(db *sql.DB) {
	c.db.Close()
	c.db = db
}

// ColumnsFromQuery executes a given query on a new connection and
// converts the results to columns. A query should return a result that is
// at least 2 columns wide and have the following structure:
//
//	1st elem: name - string
//	2nd elem: type - string
//
// Query is sprintf-ed with args, so ColumnsFromQuery("select a from %s", "table_name") works.
func (c *Client) ColumnsFromQuery(query string, args ...any) ([]*core.Column, error) {
	result, err := c.Query(context.Background(), fmt.Sprintf(query, args...))
	if err != nil {
		return nil, err
	}

	return ColumnsFromResultStream(result)
}

// Exec executes a query and returns a stream with single row (number of affected results).
func (c *Client) Exec(ctx context.Context, query string) (*ResultStream, error) {
	var res sql.Result
	var err error

	c.txMu.Lock()
	tx := c.tx
	c.txMu.Unlock()

	if tx != nil {
		res, err = tx.ExecContext(ctx, query)
	} else {
		res, err = c.db.ExecContext(ctx, query)
	}
	if err != nil {
		return nil, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	rows := NewResultStreamBuilder().
		WithNextFunc(NextSingle(affected)).
		WithHeader(core.Header{"Rows Affected"}).
		Build()

	return rows, nil
}

// Query executes a query on a connection and returns a result stream.
func (c *Client) Query(ctx context.Context, query string) (*ResultStream, error) {
	var rows *sql.Rows
	var err error

	c.txMu.Lock()
	tx := c.tx
	c.txMu.Unlock()

	if tx != nil {
		rows, err = tx.QueryContext(ctx, query)
	} else {
		rows, err = c.db.QueryContext(ctx, query)
	}
	if err != nil {
		return nil, err
	}

	return c.parseRows(rows)
}

// QueryMultiple executes a query and returns all result sets.
// This properly handles queries that return multiple result sets (e.g., SQL Server stored procedures).
func (c *Client) QueryMultiple(ctx context.Context, query string) ([]*ResultStream, error) {
	var rows *sql.Rows
	var err error

	c.txMu.Lock()
	tx := c.tx
	c.txMu.Unlock()

	if tx != nil {
		rows, err = tx.QueryContext(ctx, query)
	} else {
		rows, err = c.db.QueryContext(ctx, query)
	}
	if err != nil {
		return nil, err
	}

	return c.parseRowsMultiple(rows)
}

// QueryUntilNotEmpty executes given queries on a single connection and returns when one of them
// has a nonempty result.
// Useful for specifying "fallback" queries like "ROWCOUNT()" when there are no results in query.
func (c *Client) QueryUntilNotEmpty(ctx context.Context, queries ...string) (*ResultStream, error) {
	if len(queries) < 1 {
		return nil, errors.New("no queries provided")
	}

	c.txMu.Lock()
	tx := c.tx
	c.txMu.Unlock()

	// If we have an active transaction, use it directly
	if tx != nil {
		for _, query := range queries {
			rows, err := tx.QueryContext(ctx, query)
			if err != nil {
				return nil, fmt.Errorf("tx.QueryContext: %w", err)
			}

			result, err := c.parseRows(rows)
			if err != nil {
				return nil, err
			}

			// has result
			if len(result.Header()) > 0 {
				return result, nil
			}

			result.Close()
		}

		// return an empty result
		return NewResultStreamBuilder().
			WithNextFunc(NextNil()).
			WithHeader(core.Header{"No Results"}).
			Build(), nil
	}

	// No transaction - use a dedicated connection from the pool
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("c.db.Conn: %w", err)
	}

	for _, query := range queries {
		rows, err := conn.QueryContext(ctx, query)
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("conn.QueryContext: %w", err)
		}

		result, err := c.parseRows(rows)
		if err != nil {
			_ = conn.Close()
			return nil, err
		}

		// has result
		if len(result.Header()) > 0 {
			result.AddCallback(func() { _ = conn.Close() })
			return result, nil
		}

		result.Close()
	}

	_ = conn.Close()

	// return an empty result
	return NewResultStreamBuilder().
		WithNextFunc(NextNil()).
		WithHeader(core.Header{"No Results"}).
		Build(), nil
}

func (c *Client) getTypeProcessor(typ string) func(any) any {
	proc, ok := c.typeProcessors[strings.ToLower(typ)]
	if ok {
		return proc
	}

	return func(val any) any {
		valb, ok := val.([]byte)
		if ok {
			return string(valb)
		}
		return val
	}
}

// parseRows transforms sql rows to result stream.
// Note: This only returns the first result set for backward compatibility.
func (c *Client) parseRows(rows *sql.Rows) (*ResultStream, error) {
	// create new rows
	header, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	hasNextFunc := func() bool {
		return rows.Next()
	}

	nextFunc := func() (core.Row, error) {
		dbCols, err := rows.ColumnTypes()
		if err != nil {
			return nil, err
		}

		columns := make([]any, len(dbCols))
		columnPointers := make([]any, len(dbCols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		row := make(core.Row, len(dbCols))
		for i := range dbCols {
			val := *columnPointers[i].(*any)

			proc := c.getTypeProcessor(dbCols[i].DatabaseTypeName())

			row[i] = proc(val)
		}

		return row, nil
	}

	result := NewResultStreamBuilder().
		WithNextFunc(nextFunc, hasNextFunc).
		WithHeader(header).
		WithCloseFunc(func() {
			_ = rows.Close()
		}).
		Build()

	return result, nil
}

// parseRowsMultiple transforms sql rows to multiple result streams,
// one for each result set returned by the query.
func (c *Client) parseRowsMultiple(rows *sql.Rows) ([]*ResultStream, error) {
	var results []*ResultStream

	for {
		// Get header for current result set
		header, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		// Drain all rows from current result set into memory
		var resultRows []core.Row
		for rows.Next() {
			dbCols, err := rows.ColumnTypes()
			if err != nil {
				return nil, err
			}

			columns := make([]any, len(dbCols))
			columnPointers := make([]any, len(dbCols))
			for i := range columns {
				columnPointers[i] = &columns[i]
			}

			if err := rows.Scan(columnPointers...); err != nil {
				return nil, err
			}

			row := make(core.Row, len(dbCols))
			for i := range dbCols {
				val := *columnPointers[i].(*any)
				proc := c.getTypeProcessor(dbCols[i].DatabaseTypeName())
				row[i] = proc(val)
			}

			resultRows = append(resultRows, row)
		}

		// Check for errors during iteration
		if err := rows.Err(); err != nil {
			return nil, err
		}

		// Create an iterator over the collected rows
		idx := 0
		hasNextFunc := func() bool {
			return idx < len(resultRows)
		}
		nextFunc := func() (core.Row, error) {
			if idx >= len(resultRows) {
				return nil, nil
			}
			row := resultRows[idx]
			idx++
			return row, nil
		}

		result := NewResultStreamBuilder().
			WithNextFunc(nextFunc, hasNextFunc).
			WithHeader(header).
			Build()

		results = append(results, result)

		// Try to move to next result set
		if !rows.NextResultSet() {
			break
		}
	}

	// Close the underlying rows when done
	_ = rows.Close()

	return results, nil
}
