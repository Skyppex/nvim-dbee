package adapters

import (
	"context"
	"database/sql"
	"fmt"
	nurl "net/url"
	"regexp"
	"time"

	"github.com/kndndrj/nvim-dbee/dbee/core"
	"github.com/kndndrj/nvim-dbee/dbee/core/builders"
)

// Regex patterns for transaction commands.
// "Standalone" patterns match when the command is the entire query.
// "Anywhere" patterns detect transaction commands mixed with other statements.
var (
	// Standalone patterns - safe to handle
	beginTxStandalonePattern    = regexp.MustCompile(`(?i)^\s*BEGIN\s+(TRAN(SACTION)?)?\s*;?\s*$`)
	commitTxStandalonePattern   = regexp.MustCompile(`(?i)^\s*COMMIT(\s+(TRAN(SACTION)?)?)?\s*;?\s*$`)
	rollbackTxStandalonePattern = regexp.MustCompile(`(?i)^\s*ROLLBACK(\s+(TRAN(SACTION)?)?)?\s*;?\s*$`)

	// Patterns to detect transaction commands anywhere in the query (for warning)
	// These use word boundaries to avoid matching things like "BEGINNING" or column names
	beginTxAnywherePattern    = regexp.MustCompile(`(?i)\bBEGIN\s+(TRAN(SACTION)?)\b`)
	commitTxAnywherePattern   = regexp.MustCompile(`(?i)\bCOMMIT(\s+(TRAN(SACTION)?))?\s*(;|$)`)
	rollbackTxAnywherePattern = regexp.MustCompile(`(?i)\bROLLBACK(\s+(TRAN(SACTION)?))?\s*(;|$)`)
)

var (
	_ core.Driver               = (*sqlServerDriver)(nil)
	_ core.DatabaseSwitcher     = (*sqlServerDriver)(nil)
	_ core.MultipleResultDriver = (*sqlServerDriver)(nil)
	_ core.TransactionManager   = (*sqlServerDriver)(nil)
)

type sqlServerDriver struct {
	c          *builders.Client
	url        *nurl.URL
	driverName string
}

func (c *sqlServerDriver) Query(ctx context.Context, query string) (core.ResultStream, error) {
	// Check for transaction commands and handle them automatically
	if result, handled, err := c.handleTransactionCommand(query); handled {
		return result, err
	}

	// run query, fallback to affected rows
	return c.c.QueryUntilNotEmpty(ctx, query, "select @@ROWCOUNT as 'Rows Affected'")
}

// handleTransactionCommand checks if the query is a transaction command and handles it.
// Returns (result, handled, error) - if handled is true, the caller should return the result.
func (c *sqlServerDriver) handleTransactionCommand(query string) (core.ResultStream, bool, error) {
	// Check for standalone BEGIN TRANSACTION
	if beginTxStandalonePattern.MatchString(query) {
		err := c.BeginTransaction()
		if err != nil {
			return nil, true, err
		}
		result := builders.NewResultStreamBuilder().
			WithNextFunc(builders.NextSingle("Transaction started")).
			WithHeader(core.Header{"Result"}).
			Build()
		return result, true, nil
	}

	// Check for standalone COMMIT
	if commitTxStandalonePattern.MatchString(query) {
		err := c.CommitTransaction()
		if err != nil {
			return nil, true, err
		}
		result := builders.NewResultStreamBuilder().
			WithNextFunc(builders.NextSingle("Transaction committed")).
			WithHeader(core.Header{"Result"}).
			Build()
		return result, true, nil
	}

	// Check for standalone ROLLBACK
	if rollbackTxStandalonePattern.MatchString(query) {
		err := c.RollbackTransaction()
		if err != nil {
			return nil, true, err
		}
		result := builders.NewResultStreamBuilder().
			WithNextFunc(builders.NextSingle("Transaction rolled back")).
			WithHeader(core.Header{"Result"}).
			Build()
		return result, true, nil
	}

	// Check for transaction commands mixed with other statements - this is unsafe!
	// Warn the user that these won't work as expected.
	if beginTxAnywherePattern.MatchString(query) {
		return nil, true, fmt.Errorf("BEGIN TRANSACTION detected in a mixed query batch. " +
			"Transaction commands must be executed as standalone queries to work correctly. " +
			"Run 'BEGIN TRANSACTION' separately, then run your other statements")
	}
	if commitTxAnywherePattern.MatchString(query) && !commitTxStandalonePattern.MatchString(query) {
		return nil, true, fmt.Errorf("COMMIT detected in a mixed query batch. " +
			"Transaction commands must be executed as standalone queries to work correctly. " +
			"Run 'COMMIT' separately")
	}
	if rollbackTxAnywherePattern.MatchString(query) && !rollbackTxStandalonePattern.MatchString(query) {
		return nil, true, fmt.Errorf("ROLLBACK detected in a mixed query batch. " +
			"Transaction commands must be executed as standalone queries to work correctly. " +
			"Run 'ROLLBACK' separately")
	}

	return nil, false, nil
}

func (c *sqlServerDriver) QueryMultiple(ctx context.Context, query string) ([]core.ResultStream, error) {
	// Check for transaction commands and handle them automatically
	if result, handled, err := c.handleTransactionCommand(query); handled {
		if err != nil {
			return nil, err
		}
		return []core.ResultStream{result}, nil
	}

	// Get all result sets from the query
	results, err := c.c.QueryMultiple(ctx, query)
	if err != nil {
		return nil, err
	}

	// If no results, fall back to row count
	if len(results) == 0 || (len(results) == 1 && len(results[0].Header()) == 0) {
		fallback, err := c.c.Query(ctx, "select @@ROWCOUNT as 'Rows Affected'")
		if err != nil {
			return nil, err
		}
		return []core.ResultStream{fallback}, nil
	}

	// Convert to []core.ResultStream
	streams := make([]core.ResultStream, len(results))
	for i, r := range results {
		streams[i] = r
	}
	return streams, nil
}

func (c *sqlServerDriver) Columns(opts *core.TableOptions) ([]*core.Column, error) {
	return c.c.ColumnsFromQuery(`
		SELECT
			column_name,
			data_type
		FROM information_schema.columns
			WHERE table_name='%s' AND
			table_schema = '%s'`,
		opts.Table,
		opts.Schema,
	)
}

func (c *sqlServerDriver) Structure() ([]*core.Structure, error) {
	query := `
    SELECT table_schema, table_name, table_type
    FROM INFORMATION_SCHEMA.TABLES`

	rows, err := c.Query(context.TODO(), query)
	if err != nil {
		return nil, err
	}

	return core.GetGenericStructure(rows, getPGStructureType)
}

func (c *sqlServerDriver) Close() {
	c.c.Close()
}

// BeginTransaction starts a new transaction.
func (c *sqlServerDriver) BeginTransaction() error {
	return c.c.BeginTransaction()
}

// CommitTransaction commits the current transaction.
func (c *sqlServerDriver) CommitTransaction() error {
	return c.c.CommitTransaction()
}

// RollbackTransaction rolls back the current transaction.
func (c *sqlServerDriver) RollbackTransaction() error {
	return c.c.RollbackTransaction()
}

// HasActiveTransaction returns true if there's an active transaction.
func (c *sqlServerDriver) HasActiveTransaction() bool {
	return c.c.HasActiveTransaction()
}

func (c *sqlServerDriver) ListDatabases() (current string, available []string, err error) {
	query := `
		SELECT DB_NAME(), name
		FROM sys.databases
		WHERE name != DB_NAME();
	`

	rows, err := c.Query(context.TODO(), query)
	if err != nil {
		return "", nil, err
	}

	for rows.HasNext() {
		row, err := rows.Next()
		if err != nil {
			return "", nil, err
		}

		// We know for a fact there are 2 string fields (see query above)
		current = row[0].(string)
		available = append(available, row[1].(string))
	}

	return current, available, nil
}

func (c *sqlServerDriver) SelectDatabase(name string) error {
	// Check for active transaction before allowing database switch
	if c.c.HasActiveTransaction() {
		return fmt.Errorf("cannot switch database while transaction is active - commit or rollback first")
	}

	q := c.url.Query()
	q.Set("database", name)
	c.url.RawQuery = q.Encode()

	db, err := sql.Open(c.driverName, c.url.String())
	if err != nil {
		return fmt.Errorf("unable to switch databases: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("unable to switch databases: %w", err)
	}

	c.c.Swap(db)

	return nil
}
