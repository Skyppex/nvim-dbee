package adapters

import (
	"context"
	"database/sql"
	"fmt"
	nurl "net/url"
	"time"

	"github.com/kndndrj/nvim-dbee/dbee/core"
	"github.com/kndndrj/nvim-dbee/dbee/core/builders"
)

var (
	_ core.Driver               = (*sqlServerDriver)(nil)
	_ core.DatabaseSwitcher     = (*sqlServerDriver)(nil)
	_ core.MultipleResultDriver = (*sqlServerDriver)(nil)
)

type sqlServerDriver struct {
	c          *builders.Client
	url        *nurl.URL
	driverName string
}

func (c *sqlServerDriver) Query(ctx context.Context, query string) (core.ResultStream, error) {
	// run query, fallback to affected rows
	return c.c.QueryUntilNotEmpty(ctx, query, "select @@ROWCOUNT as 'Rows Affected'")
}

func (c *sqlServerDriver) QueryMultiple(ctx context.Context, query string) ([]core.ResultStream, error) {
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
