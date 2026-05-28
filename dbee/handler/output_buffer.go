package handler

import (
	"github.com/neovim/go-client/nvim"
)

func newBuffer(vim *nvim.Nvim, buffer nvim.Buffer) *Buffer {
	return &Buffer{
		buffer: buffer,
		vim:    vim,
	}
}

type Buffer struct {
	buffer nvim.Buffer
	vim    *nvim.Nvim
}

func (b *Buffer) Write(p []byte) (int, error) {
	err := b.vim.ExecLua(
		"local buf = select(1, ...); local text = select(2, ...); vim.api.nvim_buf_set_option(buf, 'modifiable', true); vim.api.nvim_buf_set_lines(buf, 0, -1, false, vim.split(text, '\\n')); vim.api.nvim_buf_set_option(buf, 'modifiable', false)",
		nil,
		b.buffer, string(p),
	)
	if err != nil {
		return 0, err
	}

	return len(p), nil
}
