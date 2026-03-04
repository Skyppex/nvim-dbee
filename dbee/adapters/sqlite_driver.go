package adapters

import (
	"context"

	"github.com/kndndrj/nvim-dbee/dbee/core"
	"github.com/kndndrj/nvim-dbee/dbee/core/builders"
)

var (
	_ core.Driver               = (*sqliteDriver)(nil)
	_ core.DatabaseSwitcher     = (*sqliteDriver)(nil)
	_ core.MultipleResultDriver = (*sqliteDriver)(nil)
)

type sqliteDriver struct {
	c               *builders.Client
	currentDatabase string
}

func (d *sqliteDriver) Query(ctx context.Context, query string) (core.ResultStream, error) {
	// run query, fallback to affected rows
	return d.c.QueryUntilNotEmpty(ctx, query, "select changes() as 'Rows Affected'")
}

func (d *sqliteDriver) QueryMultiple(ctx context.Context, query string) ([]core.ResultStream, error) {
	// Get all result sets from the query
	results, err := d.c.QueryMultiple(ctx, query)
	if err != nil {
		return nil, err
	}

	// If no results, fall back to row count
	if len(results) == 0 || (len(results) == 1 && len(results[0].Header()) == 0) {
		fallback, err := d.c.Query(ctx, "select changes() as 'Rows Affected'")
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

func (d *sqliteDriver) Columns(opts *core.TableOptions) ([]*core.Column, error) {
	return d.c.ColumnsFromQuery("SELECT name, type FROM pragma_table_info('%s')", opts.Table)
}

func (d *sqliteDriver) Structure() ([]*core.Structure, error) {
	// sqlite is single schema structure, so we hardcode the name of it.
	query := "SELECT 'sqlite_schema' as schema, name, type FROM sqlite_schema"

	rows, err := d.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}

	decodeStructureType := func(typ string) core.StructureType {
		switch typ {
		case "table":
			return core.StructureTypeTable
		case "view":
			return core.StructureTypeView
		default:
			return core.StructureTypeNone
		}
	}
	return core.GetGenericStructure(rows, decodeStructureType)
}

func (d *sqliteDriver) Close() { d.c.Close() }

func (d *sqliteDriver) ListDatabases() (string, []string, error) {
	return d.currentDatabase, []string{"not supported yet"}, nil
}

// SelectDatabase is a no-op, added to make the UI more pleasent.
func (d *sqliteDriver) SelectDatabase(name string) error { return nil }
