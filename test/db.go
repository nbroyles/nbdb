package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func MakeDB(t *testing.T, dbFilePath string) {
	err := os.MkdirAll(dbFilePath, 0755)
	assert.NoError(t, err)
}

func CleanupDB(dbPath string) {
	os.RemoveAll(dbPath)
}

func FileExists(t *testing.T, path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	} else if err == nil {
		return true
	}

	assert.FailNow(t, fmt.Sprintf("failed attempting to check if %s exists", path))

	return false
}
