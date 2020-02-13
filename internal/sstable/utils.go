package sstable

import (
	"fmt"
	"io"
)

func write(out io.Writer, bytes []byte) error {
	if n, err := out.Write(bytes); n != len(bytes) {
		return fmt.Errorf("failed to write all bytes to disk. n=%d, expected=%d", n, len(bytes))
	} else if err != nil {
		return fmt.Errorf("failure writing level 0 sstable: %w", err)
	}

	return nil
}
