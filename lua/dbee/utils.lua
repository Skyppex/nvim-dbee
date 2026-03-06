local M = {}

-- private variable with registered onces
---@type table<string, boolean>
local used_onces = {}

---@param id string unique id of this singleton bool
---@return boolean
function M.once(id)
  id = id or ""

  if used_onces[id] then
    return false
  end

  used_onces[id] = true

  return true
end

-- Get cursor range of current selection
---@return integer start row
---@return integer start column
---@return integer end row
---@return integer end column
function M.visual_selection()
  -- return to normal mode ('< and '> become available only after you exit visual mode)
  local key = vim.api.nvim_replace_termcodes("<esc>", true, false, true)
  vim.api.nvim_feedkeys(key, "x", false)

  local _, srow, scol, _ = unpack(vim.fn.getpos("'<"))
  local _, erow, ecol, _ = unpack(vim.fn.getpos("'>"))
  if ecol > 200000 then
    ecol = 20000
  end
  if srow < erow or (srow == erow and scol <= ecol) then
    return srow - 1, scol - 1, erow - 1, ecol
  else
    return erow - 1, ecol - 1, srow - 1, scol
  end
end

---@param level "info"|"warn"|"error"
---@param message string
---@param subtitle? string
function M.log(level, message, subtitle)
  -- log level
  local l = vim.log.levels.OFF
  if level == "info" then
    l = vim.log.levels.INFO
  elseif level == "warn" then
    l = vim.log.levels.WARN
  elseif level == "error" then
    l = vim.log.levels.ERROR
  end

  -- subtitle
  if subtitle then
    subtitle = "[" .. subtitle .. "]:"
  else
    subtitle = ""
  end
  vim.notify(subtitle .. " " .. message, l, { title = "nvim-dbee" })
end

-- Gets keys of a map and sorts them by name
---@param obj table<string, any> map-like table
---@return string[]
function M.sorted_keys(obj)
  local keys = {}
  for k, _ in pairs(obj) do
    table.insert(keys, k)
  end
  table.sort(keys)
  return keys
end

-- create an autocmd that is associated with a window rather than a buffer.
---@param events string[]
---@param winid integer
---@param opts table<string, any>
local function create_window_autocmd(events, winid, opts)
  opts = opts or {}
  if not events or not winid or not opts.callback then
    return
  end

  local cb = opts.callback

  opts.callback = function(event)
    -- remove autocmd if window is closed
    if not vim.api.nvim_win_is_valid(winid) then
      vim.api.nvim_del_autocmd(event.id)
      return
    end

    local wid = vim.fn.bufwinid(event.buf or -1)
    if wid ~= winid then
      return
    end
    cb(event)
  end

  vim.api.nvim_create_autocmd(events, opts)
end

-- create an autocmd just once in a single place in code.
-- If opts hold a "window" key, autocmd is defined per window rather than a buffer.
-- If window and buffer are provided, this results in an error.
---@param events string[] events list as defined in nvim api
---@param opts table<string, any> options as in api
function M.create_singleton_autocmd(events, opts)
  if opts.window and opts.buffer then
    error("cannot register autocmd for buffer and window at the same time")
  end

  local caller_info = debug.getinfo(2)
  if not caller_info or not caller_info.name or not caller_info.currentline then
    error("could not determine function caller")
  end

  if
    not M.once(
      "autocmd_singleton_"
        .. caller_info.name
        .. caller_info.currentline
        .. tostring(opts.window)
        .. tostring(opts.buffer)
    )
  then
    -- already configured
    return
  end

  if opts.window then
    local window = opts.window
    opts.window = nil
    create_window_autocmd(events, window, opts)
    return
  end

  vim.api.nvim_create_autocmd(events, opts)
end

local random_charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

--- Generate a random string
---@return string _ random string of 10 characters
function M.random_string()
  local function r(length)
    if length < 1 then
      return ""
    end

    local i = math.random(1, #random_charset)
    return r(length - 1) .. random_charset:sub(i, i)
  end

  return r(10)
end

--- Get the SQL statement under the cursor and its range (using treesitter).
--- Potential returns are 1. the SQL query, 2. empty string, 3. nil if filetype isn't SQL.
---@param bufnr integer buffer containing the SQL queries.
---@return nil|string query, nil|integer start_row, nil|integer end_row
function M.query_under_cursor(bufnr)
  bufnr = bufnr or vim.api.nvim_get_current_buf()
  local ft = vim.bo[bufnr].filetype
  if ft ~= "sql" then
    return
  end

  local lines = vim.api.nvim_buf_get_lines(bufnr, 0, -1, false)
  local cursor_row = vim.api.nvim_win_get_cursor(0)[1] - 1
  local query = ""
  local start_row, end_row = 0, 0

  -- tmp_buf is a temporary buffer for treesitter to parse the SQL statements
  local tmp_buf = vim.api.nvim_create_buf(false, true)

  -- replace empty lines with semicolons to make sure treesitter parses them
  -- as statement separators (still supports newlines between CTEs)
  local content = vim.tbl_map(function(line)
    return line ~= "" and line or ";"
  end, lines)

  vim.api.nvim_buf_set_lines(tmp_buf, 0, -1, false, content)

  local parser = vim.treesitter.get_parser(tmp_buf, "sql", {})
  if not parser then
    vim.api.nvim_buf_delete(tmp_buf, { force = true })
    return query, start_row, end_row
  end

  local root = parser:parse()[1]:root()

  for node in root:iter_children() do
    local node_type = node:type()
    local node_start_row, _, node_end_row, _ = node:range()

    if cursor_row >= node_start_row and cursor_row <= node_end_row then
      if node_type == "statement" then
        query = vim.treesitter.get_node_text(node, tmp_buf)
        start_row, end_row = node_start_row, node_end_row
        break
      elseif node_type == "transaction" then
        -- Handle transaction blocks: find statement or transaction keyword under cursor
        -- Statements may be direct children or nested inside ERROR nodes
        local function find_in_children(parent)
          for child in parent:iter_children() do
            local child_type = child:type()
            local child_start_row, _, child_end_row, _ = child:range()

            if cursor_row >= child_start_row and cursor_row <= child_end_row then
              if child_type == "statement" then
                query = vim.treesitter.get_node_text(child, tmp_buf)
                start_row, end_row = child_start_row, child_end_row
                return true
              elseif child_type == "keyword_begin" then
                query = "BEGIN TRANSACTION"
                start_row, end_row = child_start_row, child_end_row
                return true
              elseif child_type == "keyword_commit" then
                query = "COMMIT"
                start_row, end_row = child_start_row, child_end_row
                return true
              elseif child_type == "keyword_rollback" then
                query = "ROLLBACK"
                start_row, end_row = child_start_row, child_end_row
                return true
              elseif child_type == "ERROR" then
                -- Statements may be nested inside ERROR nodes, search recursively
                if find_in_children(child) then
                  return true
                end
              end
            end
          end
          return false
        end

        find_in_children(node)
        if query ~= "" then break end
      end
    end
  end

  -- clean up the tmp_buf
  vim.api.nvim_buf_delete(tmp_buf, { force = true })
  return query:gsub(";", ""), start_row, end_row
end

return M
