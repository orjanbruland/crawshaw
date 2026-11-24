package sqlite_test

import (
	"path/filepath"
	"testing"

	"github.com/go-llsqlite/crawshaw"
	"github.com/go-llsqlite/crawshaw/sqlitex"
)

func TestSetAuthorizer(t *testing.T) {
	c, err := sqlite.OpenConn(":memory:", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	}()

	authResult := sqlite.AuthResult(0)
	var lastAction sqlite.OpType
	auth := sqlite.AuthorizeFunc(func(info sqlite.ActionInfo) sqlite.AuthResult {
		lastAction = info.Action
		return authResult
	})
	c.SetAuthorizer(auth)

	t.Run("Allowed", func(t *testing.T) {
		authResult = 0
		stmt, _, err := c.PrepareTransient("SELECT 1;")
		if err != nil {
			t.Fatal(err)
		}
		stmt.Finalize()
		if lastAction != sqlite.SQLITE_SELECT {
			t.Errorf("action = %q; want SQLITE_SELECT", lastAction)
		}
	})

	t.Run("Denied", func(t *testing.T) {
		authResult = sqlite.SQLITE_DENY
		stmt, _, err := c.PrepareTransient("SELECT 1;")
		if err == nil {
			stmt.Finalize()
			t.Fatal("PrepareTransient did not return an error")
		}
		if got, want := sqlite.ErrCode(err), sqlite.SQLITE_AUTH; got != want {
			t.Errorf("sqlite.ErrCode(err) = %v; want %v", got, want)
		}
		if lastAction != sqlite.SQLITE_SELECT {
			t.Errorf("action = %q; want SQLITE_SELECT", lastAction)
		}
	})
}

// Close deletes the authorizer's handle even when SQLite keeps the connection
// open for an unfinalized statement, so SQLite must not call the authorizer
// after Close. It calls it only while compiling a statement, and it refuses to
// compile on a connection that is waiting to close. If that changes, this test
// crashes with "misuse of an invalid Handle".
func TestAuthorizerNotCalledAfterClose(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	conn, err := sqlite.OpenConn(dbPath, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := sqlitex.ExecScript(conn, "CREATE TABLE t (a); INSERT INTO t (a) VALUES (1);"); err != nil {
		t.Fatal(err)
	}
	allowAll := sqlite.AuthorizeFunc(func(sqlite.ActionInfo) sqlite.AuthResult { return 0 })
	if err := conn.SetAuthorizer(allowAll); err != nil {
		t.Fatal(err)
	}
	stmt, _, err := conn.PrepareTransient("SELECT a FROM t;")
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); sqlite.ErrCode(err) != sqlite.SQLITE_BUSY {
		t.Fatalf("Close with an unfinalized statement: got %v, want SQLITE_BUSY", err)
	}

	// The statement still runs as prepared.
	if hasRow, err := stmt.Step(); err != nil || !hasRow {
		t.Fatalf("Step after Close: got hasRow=%v, err=%v; want a row", hasRow, err)
	}
	if err := stmt.Reset(); err != nil {
		t.Fatal(err)
	}

	// After a schema change SQLite has to compile it again before it can run.
	other, err := sqlite.OpenConn(dbPath, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := sqlitex.ExecScript(other, "ALTER TABLE t ADD COLUMN b;"); err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.Step(); sqlite.ErrCode(err) != sqlite.SQLITE_MISUSE {
		t.Errorf("Step after Close and a schema change: got %v, want SQLITE_MISUSE", err)
	}
	if err := stmt.Finalize(); err != nil {
		t.Fatal(err)
	}
}
