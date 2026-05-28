local install = require("dbee.install")
local api = require("dbee.api")
local config = require("dbee.config")

---@toc dbee.ref.contents

---@mod dbee.ref Dbee Reference
---@brief [[
---Database Client for NeoVim.
---@brief ]]

local dbee = {
  api = {
    core = api.core,
    ui = api.ui,
  },
}

---Setup function.
---Needs to be called before calling any other function.
---@param cfg? Config
function dbee.setup(cfg)
  -- merge with defaults
  local merged = config.merge_with_default(cfg)

  -- validate config
  config.validate(merged)

  api.setup(merged)
end

---Toggle dbee UI.
function dbee.toggle()
  if api.current_config().window_layout:is_open() then
    dbee.close()
  else
    dbee.open()
  end
end

---Open dbee UI. If already opened, reset window layout.
function dbee.open()
  if api.current_config().window_layout:is_open() then
    return api.current_config().window_layout:reset()
  end
  api.current_config().window_layout:open()
end

---Close dbee UI.
function dbee.close()
  if not api.current_config().window_layout:is_open() then
    return
  end
  api.current_config().window_layout:close()
end

---Check if dbee UI is open or not.
---@return boolean
function dbee.is_open()
  return api.current_config().window_layout:is_open()
end

---Execute a query on current connection.
---Convenience wrapper around some api functions that executes a query on
---current connection and pipes the output to result UI.
---@param query string
function dbee.execute(query)
  local conn = api.core.get_current_connection()
  if not conn then
    error("no connection currently selected")
  end

  local call = api.core.connection_execute(conn.id, query)
  api.ui.result_set_call(call)

  dbee.open()
end

---Store currently displayed result.
---Convenience wrapper around some api functions.
---@param format string format of the output -> "csv"|"json"|"table"
---@param output string where to pipe the results -> "file"|"yank"|"buffer"
---@param opts { from: integer, to: integer, extra_arg: any }
---Get a single cell value from the displayed result.
---If opts is not provided, uses the current cursor position.
---
---@param opts? { position: integer[]? } optional position as {row, col} (same as nvim_win_get_cursor), defaults to cursor
---@return any cell value. <nil> resolves to nil. <blob> resolves to actual binary data.
function dbee.get(opts)
  local call = api.ui.result_get_call()
  if not call then
    error("no current call to get value from")
  end

  opts = opts or {}

  local winid = vim.api.nvim_get_current_win()
  local position = opts.position or vim.api.nvim_win_get_cursor(winid)
  local row, col = position[1], position[2]

  local bufnr = vim.api.nvim_win_get_buf(winid)
  local line = vim.api.nvim_buf_get_lines(bufnr, row - 1, row, true)[1]
  if not line then
    error("no line at row " .. row)
  end

  -- Split the line by " │ " separator (space-pipe-space)
  -- line format: "# │ col1 │ col2 │ ..."
  local segments = {}
  local idx = 1
  while idx <= #line do
    -- find the next separator " │ "
    local _, sep_end = line:find(" │ ", idx)
    if not sep_end then
      -- last segment
      table.insert(segments, line:sub(idx))
      break
    end
    table.insert(segments, line:sub(idx, sep_end - 5))
    idx = sep_end + 1
  end

  if #segments < 2 then
    error("no columns found in line")
  end

  -- Determine which segment the cursor column is in
  -- Build the column start positions
  local col_starts = {}
  local pos = 1
  for i, seg in ipairs(segments) do
    table.insert(col_starts, pos)
    pos = pos + #seg
    if i < #segments then
      pos = pos + 5  -- skip " │ "
    end
  end

  -- Find the segment at cursor column (1-indexed in Lua)
  local seg_idx = nil
  for i, start in ipairs(col_starts) do
    local seg = segments[i]
    local seg_end = start + #seg - 1
    if col + 1 >= start and col + 1 <= seg_end then
      seg_idx = i
      break
    end
  end

  if not seg_idx then
    error("could not determine column at cursor position")
  end

  -- First segment = row number column, subtract 1 for data column index
  local col_index = seg_idx - 2
  if col_index < 0 then
    error("cursor is on the row number column")
  end

  local result_index = api.ui.result_get_result_set_index()
  local row_num_str = segments[1]:match("%d+")
  if not row_num_str then
    error("could not parse row number")
  end
  local row_index = tonumber(row_num_str)

  return api.core.call_get_cell_value(call.id, result_index, row_index, col_index)
end

function dbee.store(format, output, opts)
  local call = api.ui.result_get_call()
  if not call then
    error("no current call to store")
  end

  opts = opts or {}
  opts.result_index = api.ui.result_get_result_set_index()

  api.core.call_store_result(call.id, format, output, opts)
end

---Supported install commands.
---@alias install_command
---| '"wget"'
---| '"curl"'
---| '"bitsadmin"'
---| '"go"'
---| '"cgo"'

---Install dbee backend binary.
---@param command? install_command Preffered install command
---@see install_command
function dbee.install(command)
  install.exec(command)
end

return dbee
