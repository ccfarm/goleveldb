package leveldb

import (
	"testing"

	"github.com/ccfarm/goleveldb/leveldb/testutil"
)

func TestLevelDB(t *testing.T) {
	testutil.RunSuite(t, "LevelDB Suite")
}
