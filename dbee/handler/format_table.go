package handler

import (
	"bytes"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/kndndrj/nvim-dbee/dbee/core"
)

var _ core.Formatter = (*Table)(nil)

type Table struct{}

func newTable() *Table {
	return &Table{}
}

func cellString(val any) string {
	switch v := val.(type) {
	case []byte:
		if len(v) == 0 {
			return ""
		}
		return "<blob>"
	default:
		s := fmt.Sprintf("%v", v)
		s = strings.ReplaceAll(s, "\r\n", "⏎")
		s = strings.ReplaceAll(s, "\n", "⏎")
		return s
	}
}

func (tf *Table) Format(header core.Header, rows []core.Row, opts *core.FormatterOptions) ([]byte, error) {
	numCols := len(header) + 1

	colValues := make([][]string, 0, len(rows)+1)

	headerRow := make([]string, numCols)
	headerRow[0] = "#"
	for i, h := range header {
		headerRow[i+1] = h
	}
	colValues = append(colValues, headerRow)

	for i, row := range rows {
		rowStr := make([]string, numCols)
		rowStr[0] = fmt.Sprintf("%d", opts.ChunkStart+i)
		for j, val := range row {
			rowStr[j+1] = cellString(val)
		}
		colValues = append(colValues, rowStr)
	}

	colWidths := make([]int, numCols)
	for _, row := range colValues {
		for i, val := range row {
			w := utf8.RuneCountInString(val)
			if w > colWidths[i] {
				colWidths[i] = w
			}
		}
	}

	var buf bytes.Buffer
	for ri, row := range colValues {
		for ci, val := range row {
			if ci > 0 {
				buf.WriteString(" │ ")
			}
			buf.WriteString(val)
			pad := colWidths[ci] - utf8.RuneCountInString(val)
			if pad > 0 {
				buf.WriteString(strings.Repeat(" ", pad))
			}
		}
		buf.WriteString("\n")

		if ri == 0 {
			for ci := range colWidths {
				if ci > 0 {
					buf.WriteString("─┼─")
				}
				buf.WriteString(strings.Repeat("─", colWidths[ci]))
			}
			buf.WriteString("\n")
		}
	}

	return buf.Bytes(), nil
}
