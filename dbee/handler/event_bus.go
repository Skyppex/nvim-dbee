package handler

import (
	"fmt"

	"github.com/neovim/go-client/nvim"

	"github.com/kndndrj/nvim-dbee/dbee/core"
	"github.com/kndndrj/nvim-dbee/dbee/plugin"
)

type eventBus struct {
	vim *nvim.Nvim
	log *plugin.Logger
}

func (eb *eventBus) callLua(event string, data string) {
	err := eb.vim.ExecLua(fmt.Sprintf(`require("dbee.handler.__events").trigger(%q, %s)`, event, data), nil)
	if err != nil {
		eb.log.Infof("eb.vim.ExecLua: %s", err)
	}
}

func (eb *eventBus) CallStateChanged(call *core.Call) {
	errMsg := "nil"
	if err := call.Err(); err != nil {
		errMsg = fmt.Sprintf("[[%s]]", err.Error())
	}

	data := fmt.Sprintf(`{
		call = {
			id = %q,
			query = %q,
			state = %q,
			time_taken_us = %d,
			timestamp_us = %d,
			error = %s,
		},
	}`, call.GetID(),
		call.GetQuery(),
		call.GetState().String(),
		call.GetTimeTaken().Microseconds(),
		call.GetTimestamp().UnixMicro(),
		errMsg)

	eb.callLua("call_state_changed", data)
}

func (eb *eventBus) CurrentConnectionChanged(id core.ConnectionID) {
	data := fmt.Sprintf(`{
		conn_id = %q,
	}`, id)

	eb.callLua("current_connection_changed", data)
}

// DatabaseSelected is called when the selected database of a connection is changed.
// Sends the new database name along with affected connection ID to the lua event handler.
func (eb *eventBus) DatabaseSelected(id core.ConnectionID, dbname string) {
	data := fmt.Sprintf(`{
		conn_id = %q,
		database_name = %q,
	}`, id, dbname)

	eb.callLua("database_selected", data)
}

// StructureLoaded is called when the async structure fetch completes.
func (eb *eventBus) StructureLoaded(id core.ConnectionID, hasError bool) {
	data := fmt.Sprintf(`{
		conn_id = %q,
		has_error = %t,
	}`, id, hasError)

	eb.callLua("structure_loaded", data)
}

// DatabasesListed is called when the async database list fetch completes.
func (eb *eventBus) DatabasesListed(id core.ConnectionID, hasError bool) {
	data := fmt.Sprintf(`{
		conn_id = %q,
		has_error = %t,
	}`, id, hasError)

	eb.callLua("databases_listed", data)
}

// DatabaseSelectFailed is called when an async database switch fails.
func (eb *eventBus) DatabaseSelectFailed(id core.ConnectionID, err error) {
	data := fmt.Sprintf(`{
		conn_id = %q,
		error = %q,
	}`, id, err.Error())

	eb.callLua("database_select_failed", data)
}
