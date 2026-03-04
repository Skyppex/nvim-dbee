package adapters

import (
	"context"

	"github.com/kndndrj/nvim-dbee/dbee/core"
	"github.com/kndndrj/nvim-dbee/dbee/core/builders"
)

var (
	_ core.Driver               = (*mySQLDriver)(nil)
	_ core.MultipleResultDriver = (*mySQLDriver)(nil)
)

type mySQLDriver struct {
	c *builders.Client
}

func (c *mySQLDriver) Query(ctx context.Context, query string) (core.ResultStream, error) {
	// run query, fallback to affected rows
	return c.c.QueryUntilNotEmpty(ctx, query, "select ROW_COUNT() as 'Rows Affected'")
}

func (c *mySQLDriver) QueryMultiple(ctx context.Context, query string) ([]core.ResultStream, error) {
	// Get all result sets from the query
	results, err := c.c.QueryMultiple(ctx, query)
	if err != nil {
		return nil, err
	}

	// If no results, fall back to row count
	if len(results) == 0 || (len(results) == 1 && len(results[0].Header()) == 0) {
		fallback, err := c.c.Query(ctx, "select ROW_COUNT() as 'Rows Affected'")
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

func (c *mySQLDriver) Columns(opts *core.TableOptions) ([]*core.Column, error) {
	return c.c.ColumnsFromQuery("DESCRIBE `%s`.`%s`", opts.Schema, opts.Table)
}

func (c *mySQLDriver) Structure() ([]*core.Structure, error) {
	query := `SELECT table_schema, table_name FROM information_schema.tables`

	rows, err := c.Query(context.TODO(), query)
	if err != nil {
		return nil, err
	}

	children := make(map[string][]*core.Structure)

	for rows.HasNext() {
		row, err := rows.Next()
		if err != nil {
			return nil, err
		}

		// We know for a fact there are 2 string fields (see query above)
		schema := row[0].(string)
		table := row[1].(string)

		children[schema] = append(children[schema], &core.Structure{
			Name:   table,
			Schema: schema,
			Type:   core.StructureTypeTable,
		})

	}

	var structure []*core.Structure

	for k, v := range children {
		structure = append(structure, &core.Structure{
			Name:     k,
			Schema:   k,
			Type:     core.StructureTypeNone,
			Children: v,
		})
	}

	return structure, nil
}

func (c *mySQLDriver) Close() {
	c.c.Close()
}
