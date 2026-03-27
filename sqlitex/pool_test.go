// Copyright (c) 2018 David Crawshaw <david@zentus.com>
//
// Permission to use, copy, modify, and distribute this software for any
// purpose with or without fee is hereby granted, provided that the above
// copyright notice and this permission notice appear in all copies.
//
// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
// WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
// ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
// WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
// ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
// OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

package sqlitex_test

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	sqlite "github.com/go-llsqlite/crawshaw"
	"github.com/go-llsqlite/crawshaw/sqlitex"
)

const (
	poolSize  = 20
	poolURI   = "file::memory:?mode=memory&cache=shared"
	poolFlags = sqlite.SQLITE_OPEN_READWRITE | sqlite.SQLITE_OPEN_CREATE | sqlite.SQLITE_OPEN_URI | sqlite.SQLITE_OPEN_NOMUTEX | sqlite.SQLITE_OPEN_SHAREDCACHE
)

// newMemPool returns new sqlitex.Pool attached to new database opened in memory.
//
// the pool is initialized with size=poolSize.
// any error is t.Fatal.
func newMemPool(t *testing.T) *sqlitex.Pool {
	t.Helper()
	dbpool, err := sqlitex.Open(poolURI, poolFlags, poolSize)
	if err != nil {
		t.Fatal(err)
	}
	return dbpool
}

func TestPool(t *testing.T) {
	dbpool := newMemPool(t)
	defer func() {
		if err := dbpool.Close(t.Context()); err != nil {
			t.Error(err)
		}
	}()

	c, err := dbpool.Get(nil)
	if err != nil {
		t.Fatal(err)
	}
	c.Prep("DROP TABLE IF EXISTS footable;").Step()
	if hasRow, err := c.Prep("CREATE TABLE footable (col1 integer);").Step(); err != nil {
		t.Fatal(err)
	} else if hasRow {
		t.Errorf("CREATE TABLE reports having a row")
	}
	dbpool.Put(c)
	c = nil

	var wg sync.WaitGroup
	for i := range poolSize {
		wg.Add(1)
		go func(i int) {
			for j := range 10 {
				testInsert(t, fmt.Sprintf("%d-%d", i, j), dbpool)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	c, err = dbpool.Get(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dbpool.Put(c)
	stmt := c.Prep("SELECT COUNT(*) FROM footable;")
	if hasRow, err := stmt.Step(); err != nil {
		t.Fatal(err)
	} else if hasRow {
		count := int(stmt.ColumnInt64(0))
		if want := poolSize * 10 * insertCount; count != want {
			t.Errorf("SELECT COUNT(*) = %d, want %d", count, want)
		}
	} else {
		t.Errorf("SELECT COUNT(*) reports not having a row")
	}
	stmt.Reset()
}

const insertCount = 120

func testInsert(t *testing.T, id string, dbpool *sqlitex.Pool) {
	c, err := dbpool.Get(nil)
	if err != nil {
		t.Fatalf("id=%s: dbpool.Get: %v", id, err)
	}
	defer dbpool.Put(c)

	begin := c.Prep("BEGIN;")
	commit := c.Prep("COMMIT;")
	stmt := c.Prep("INSERT INTO footable (col1) VALUES (?);")

	if _, err := begin.Step(); err != nil {
		t.Errorf("id=%s: BEGIN step: %v", id, err)
	}
	for i := range int64(insertCount) {
		if err := stmt.Reset(); err != nil {
			t.Errorf("id=%s: reset: %v", id, err)
		}
		stmt.BindInt64(1, i)
		if _, err := stmt.Step(); err != nil {
			t.Errorf("id=%s: step: %v", id, err)
		}
	}
	if _, err := commit.Step(); err != nil {
		t.Errorf("id=%s: COMMIT step: %v", id, err)
	}
}

func TestPoolAfterClose(t *testing.T) {
	// verify that Get after close never try to initialize a Conn and segfault
	dbpool := newMemPool(t)

	err := dbpool.Close(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	for range 10 * poolSize {
		conn, err := dbpool.Get(nil)
		if !errors.Is(err, sqlitex.ErrPoolClosed) {
			t.Fatalf("dbpool: Get after Close -> unexpected error: %v", err)
		}
		if conn != nil {
			t.Fatal("dbpool: Get after Close -> !nil conn")
		}
	}
}

func TestSharedCacheLock(t *testing.T) {
	dir, err := ioutil.TempDir("", "sqlite-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	dbFile := filepath.Join(dir, "awal.db")

	c0, err := sqlite.OpenConn(dbFile, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c0.Close(); err != nil {
			t.Error(err)
		}
	}()

	err = sqlitex.ExecScript(c0, `
		DROP TABLE IF EXISTS t;
		CREATE TABLE t (c, content BLOB);
		DROP TABLE IF EXISTS t2;
		CREATE TABLE t2 (c);
		INSERT INTO t2 (c) VALUES ('hello');
		`)
	if err != nil {
		t.Fatal(err)
	}

	c1, err := sqlite.OpenConn(dbFile, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c1.Close(); err != nil {
			t.Error(err)
		}
	}()
	c1.SetBusyTimeout(10 * time.Second)

	c0Lock := func() {
		if _, err := c0.Prep("BEGIN;").Step(); err != nil {
			t.Fatal(err)
		}
		if _, err := c0.Prep("INSERT INTO t (c, content) VALUES (0, 'hi');").Step(); err != nil {
			t.Fatal(err)
		}
	}
	c0Unlock := func() {
		if err := sqlitex.Exec(c0, "COMMIT;", nil); err != nil {
			t.Fatal(err)
		}
	}

	c0Lock()

	stmt := c1.Prep("INSERT INTO t (c) VALUES (1);")

	done := make(chan struct{})
	go func() {
		if _, err := stmt.Step(); err != nil {
			t.Fatal(err)
		}
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)
	select {
	case <-done:
		t.Error("insert done while transaction was held")
	default:
	}

	c0Unlock()

	// End the initial transaction, allowing the goroutine to complete
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Error("second connection insert not completing")
	}

	// TODO: It is possible for stmt.Reset to return SQLITE_LOCKED.
	//       Work out why and find a way to test it.
}

func TestPoolPutMatch(t *testing.T) {
	dbpool0 := newMemPool(t)
	dbpool1 := newMemPool(t)
	defer func() {
		if err := dbpool0.Close(t.Context()); err != nil {
			t.Error(err)
		}
		if err := dbpool1.Close(t.Context()); err != nil {
			t.Error(err)
		}
	}()

	func() {
		c, err := dbpool0.Get(nil)
		if err != nil {
			t.Fatal(err)
		}
		err = dbpool1.Put(c)
		if err == nil {
			t.Error("expect put mismatch error, got none")
		}
		err = dbpool0.Put(c)
		if err != nil {
			t.Fatalf("expect put match error, got %v", err)
		}
	}()
}

func TestPoolOpenInit(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		ctx := context.Background()
		initScript := `
                CREATE TABLE IF NOT EXISTS t(a INT, b INT);
                CREATE TEMP VIEW v AS SELECT a FROM t;
`
		dbpool, err := sqlitex.OpenInit(ctx, poolURI, poolFlags, poolSize, initScript)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := dbpool.Close(t.Context()); err != nil {
				t.Error(err)
			}
		}()

		for range poolSize {
			conn, err := dbpool.Get(nil)
			if err != nil {
				t.Fatalf("dbpool.Get: %v", err)
			}
			defer dbpool.Put(conn)
			if err := sqlitex.ExecScript(conn, `SELECT * FROM v;`); err != nil {
				t.Fatalf("initScript not run on connection: %s", err)
			}
		}
	})
	t.Run("invalid initScript", func(t *testing.T) {
		ctx := context.Background()
		initScript := `invalid script`
		dbpool, err := sqlitex.OpenInit(ctx, poolURI, poolFlags, poolSize, initScript)
		if err != nil {
			return
		}
		if err := dbpool.Close(t.Context()); err != nil {
			t.Error(err)
		}

		_, err = dbpool.Get(ctx)
		if err == nil {
			t.Fatal("an invalid script must fail initialization")
		}
	})
}

func TestPoolConnMaxLifetime(t *testing.T) {
	t.Run("expired connection is closed on Put", func(t *testing.T) {
		dbpool, err := sqlitex.OpenConfig(context.Background(), sqlitex.PoolConfig{
			URI:             poolURI,
			Flags:           poolFlags,
			MaxOpenConns:    5,
			MaxIdleConns:    5,
			ConnMaxLifetime: 50 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer dbpool.Close(t.Context())

		// Get a connection and note its identity.
		conn1, err := dbpool.Get(nil)
		if err != nil {
			t.Fatal(err)
		}
		conn1Ptr := fmt.Sprintf("%p", conn1)

		// Wait past the lifetime.
		time.Sleep(100 * time.Millisecond)

		// Put it back — should be closed, not returned to idle.
		if err := dbpool.Put(conn1); err != nil {
			t.Fatal(err)
		}

		// Get another connection — should be a new one since the old one expired.
		conn2, err := dbpool.Get(nil)
		if err != nil {
			t.Fatal(err)
		}
		conn2Ptr := fmt.Sprintf("%p", conn2)
		dbpool.Put(conn2)

		if conn1Ptr == conn2Ptr {
			t.Error("expected a new connection after lifetime expiry, got the same one")
		}
	})

	t.Run("connection within lifetime is reused", func(t *testing.T) {
		dbpool, err := sqlitex.OpenConfig(context.Background(), sqlitex.PoolConfig{
			URI:             poolURI,
			Flags:           poolFlags,
			MaxOpenConns:    5,
			MaxIdleConns:    5,
			ConnMaxLifetime: 10 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer dbpool.Close(t.Context())

		conn1, err := dbpool.Get(nil)
		if err != nil {
			t.Fatal(err)
		}
		conn1Ptr := fmt.Sprintf("%p", conn1)
		if err := dbpool.Put(conn1); err != nil {
			t.Fatal(err)
		}

		// Get again immediately — should be the same connection.
		conn2, err := dbpool.Get(nil)
		if err != nil {
			t.Fatal(err)
		}
		conn2Ptr := fmt.Sprintf("%p", conn2)
		dbpool.Put(conn2)

		if conn1Ptr != conn2Ptr {
			t.Error("expected the same connection to be reused within lifetime")
		}
	})
}

func TestPoolInitScriptPragma(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	dbpool, err := sqlitex.OpenConfig(ctx, sqlitex.PoolConfig{
		URI:          "file:" + dbPath,
		Flags:        sqlite.SQLITE_OPEN_READWRITE | sqlite.SQLITE_OPEN_CREATE | sqlite.SQLITE_OPEN_URI,
		MaxOpenConns: 2,
		InitScript:   "PRAGMA journal_mode=wal; PRAGMA synchronous=NORMAL;",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer dbpool.Close(ctx)

	conn, err := dbpool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer dbpool.Put(conn)

	// Verify PRAGMA synchronous took effect (NORMAL = 1).
	stmt := conn.Prep("PRAGMA synchronous;")
	if hasRow, err := stmt.Step(); err != nil {
		t.Fatal(err)
	} else if !hasRow {
		t.Fatal("expected a row from PRAGMA synchronous")
	}
	got := stmt.ColumnInt(0)
	stmt.Reset()
	if got != 1 {
		t.Fatalf("PRAGMA synchronous: got %d, want 1 (NORMAL)", got)
	}
}

func TestPoolInitScriptEdgeCases(t *testing.T) {
	tests := []struct {
		name       string
		initScript string
	}{
		{"empty", ""},
		{"whitespace", "   \n\t  "},
		{"single statement", "PRAGMA cache_size=1000;"},
		{"multiple statements", "PRAGMA cache_size=1000; PRAGMA temp_store=MEMORY;"},
		{"trailing whitespace", "PRAGMA cache_size=1000;   \n  "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dbpool, err := sqlitex.OpenConfig(ctx, sqlitex.PoolConfig{
				URI:          poolURI,
				Flags:        poolFlags,
				MaxOpenConns: 1,
				InitScript:   tt.initScript,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer dbpool.Close(ctx)

			conn, err := dbpool.Get(ctx)
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}
			dbpool.Put(conn)
		})
	}
}
