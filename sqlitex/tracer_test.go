package sqlitex_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sqlite "github.com/go-llsqlite/crawshaw"
	"github.com/go-llsqlite/crawshaw/sqlitex"
)

// span stands in for an OpenTelemetry span: it finds its parent in a context
// and records how it ended.
type span struct {
	name   string
	parent *span
	steps  int
	ended  int
	err    error
}

type spanKey struct{}

// spans stands in for an OpenTelemetry tracer and exporter.
type spans struct{ all []*span }

func (s *spans) start(ctx context.Context, name string) (context.Context, *span) {
	parent, _ := ctx.Value(spanKey{}).(*span)
	sp := &span{name: name, parent: parent}
	s.all = append(s.all, sp)
	return context.WithValue(ctx, spanKey{}, sp), sp
}

func (s *spans) named(t *testing.T, prefix string) []*span {
	t.Helper()
	var found []*span
	for _, sp := range s.all {
		if strings.HasPrefix(sp.name, prefix) {
			found = append(found, sp)
		}
	}
	return found
}

// spanTracer is a sqlite.Tracer in the shape an OpenTelemetry adapter would
// take: one per Get, holding the Get context and a stack of savepoint spans.
type spanTracer struct {
	spans *spans
	ctxs  []context.Context // ctxs[0] is the Get context
}

func (t *spanTracer) top() context.Context { return t.ctxs[len(t.ctxs)-1] }

func (t *spanTracer) NewTask(query string) sqlite.TracerTask {
	_, sp := t.spans.start(t.top(), query)
	return spanTask{sp}
}

func (t *spanTracer) Push(name string) {
	ctx, _ := t.spans.start(t.top(), name)
	t.ctxs = append(t.ctxs, ctx)
}

func (t *spanTracer) Pop() { t.PopWithError(nil) }

func (t *spanTracer) PopWithError(err error) {
	sp := t.top().Value(spanKey{}).(*span)
	sp.ended++
	sp.err = err
	t.ctxs = t.ctxs[:len(t.ctxs)-1]
}

type spanTask struct{ sp *span }

func (t spanTask) StartRegion(string) {}
func (t spanTask) EndRegion()         { t.sp.steps++ }
func (t spanTask) End()               { t.EndWithError(nil) }
func (t spanTask) EndWithError(err error) {
	t.sp.ended++
	t.sp.err = err
}

func TestPoolTracer(t *testing.T) {
	rec := &spans{}
	var tracerCtxs []context.Context
	disabled := false
	dbpool, err := sqlitex.OpenConfig(t.Context(), sqlitex.PoolConfig{
		URI:          filepath.Join(t.TempDir(), "tracer.db"),
		MaxOpenConns: 1,
		Tracer: func(ctx context.Context) sqlite.Tracer {
			tracerCtxs = append(tracerCtxs, ctx)
			if disabled {
				return nil
			}
			return &spanTracer{spans: rec, ctxs: []context.Context{ctx}}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// Bounded, so a failure with a connection still out does not hang.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		dbpool.Close(ctx)
	}()

	get := func(t *testing.T, ctx context.Context) *sqlite.Conn {
		t.Helper()
		conn, err := dbpool.Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return conn
	}
	exec := func(t *testing.T, conn *sqlite.Conn, query string) {
		t.Helper()
		if err := sqlitex.ExecuteTransient(conn, query, nil); err != nil {
			t.Fatal(err)
		}
	}
	checkParent := func(t *testing.T, sp *span, parent *span) {
		t.Helper()
		if sp.parent != parent {
			t.Errorf("span %q has parent %v, want %v", sp.name, sp.parent, parent)
		}
	}

	reqCtx, req := rec.start(t.Context(), "request")
	conn := get(t, reqCtx)
	if got := tracerCtxs[len(tracerCtxs)-1]; got.Value(spanKey{}) != req {
		t.Fatal("Tracer was not called with the Get context")
	}
	exec(t, conn, "CREATE TABLE t (a INTEGER PRIMARY KEY);")

	boom := errors.New("boom")
	for _, fail := range []bool{false, true} {
		func() (err error) {
			defer sqlitex.Save(conn)(&err)
			exec(t, conn, "INSERT INTO t (a) VALUES (NULL);")
			if fail {
				return boom
			}
			return nil
		}()
	}
	if err := dbpool.Put(conn); err != nil {
		t.Fatal(err)
	}

	t.Run("statements", func(t *testing.T) {
		checkParent(t, rec.named(t, "CREATE")[0], req)
		if n := len(rec.named(t, "INSERT")); n != 2 {
			t.Errorf("%d INSERT spans, want 2", n)
		}
	})

	t.Run("savepoints", func(t *testing.T) {
		txs := rec.named(t, "TX ")
		if len(txs) != 2 {
			t.Fatalf("%d TX spans, want 2", len(txs))
		}
		for i, wantErr := range []error{nil, boom} {
			tx := txs[i]
			checkParent(t, tx, req)
			if tx.err != wantErr {
				t.Errorf("TX span %d ended with %v, want %v", i, tx.err, wantErr)
			}
			var children []string
			for _, sp := range rec.all {
				if sp.parent == tx {
					children = append(children, strings.Fields(sp.name)[0])
				}
			}
			want := "SAVEPOINT INSERT RELEASE"
			if wantErr != nil {
				want = "SAVEPOINT INSERT ROLLBACK RELEASE"
			}
			if got := strings.Join(children, " "); got != want {
				t.Errorf("TX span %d children: %s, want %s", i, got, want)
			}
		}
	})

	t.Run("new context per Get", func(t *testing.T) {
		reqCtx2, req2 := rec.start(t.Context(), "request 2")
		conn := get(t, reqCtx2)
		exec(t, conn, "SELECT 1;")
		dbpool.Put(conn)
		checkParent(t, rec.all[len(rec.all)-1], req2)
	})

	t.Run("nil context", func(t *testing.T) {
		conn := get(t, nil)
		if tracerCtxs[len(tracerCtxs)-1] == nil {
			t.Error("Tracer was called with a nil context")
		}
		exec(t, conn, "SELECT 2;")
		dbpool.Put(conn)
		checkParent(t, rec.all[len(rec.all)-1], nil)
	})

	t.Run("dirty Put", func(t *testing.T) {
		conn := get(t, reqCtx)
		exec(t, conn, "BEGIN;")
		stmt := conn.Prep("SELECT a FROM t;")
		if _, err := stmt.Step(); err != nil {
			t.Fatal(err)
		}
		if err := dbpool.Put(conn); !errors.Is(err, sqlitex.ErrDirtyConn) {
			t.Fatalf("Put: got %v, want ErrDirtyConn", err)
		}
		// Put's cleanup reports to the Get context too.
		rollbacks := rec.named(t, "ROLLBACK;")
		if len(rollbacks) != 1 {
			t.Fatalf("%d ROLLBACK spans, want 1", len(rollbacks))
		}
		checkParent(t, rollbacks[0], req)
	})

	t.Run("disabled", func(t *testing.T) {
		disabled = true
		defer func() { disabled = false }()
		n := len(rec.all)
		conn := get(t, reqCtx)
		exec(t, conn, "SELECT 3;")
		func() (err error) {
			defer sqlitex.Save(conn)(&err)
			return nil
		}()
		dbpool.Put(conn)
		if len(rec.all) != n {
			t.Errorf("%d spans with tracing disabled", len(rec.all)-n)
		}
	})

	for _, sp := range rec.all {
		want := 1
		if strings.HasPrefix(sp.name, "request") {
			want = 0 // the test's own spans
		}
		if sp.ended != want {
			t.Errorf("span %q ended %d times, want %d", sp.name, sp.ended, want)
		}
	}
}

func TestPoolDefaultTracer(t *testing.T) {
	dbpool := newMemPool(t)
	defer dbpool.Close(context.Background())

	conn, err := dbpool.Get(nil)
	if err != nil {
		t.Fatal(err)
	}
	if conn.Tracer() != nil {
		t.Error("Get(nil) set a tracer")
	}
	dbpool.Put(conn)

	conn, err = dbpool.Get(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if conn.Tracer() == nil {
		t.Error("Get(ctx) did not set the runtime/trace tracer")
	}
	dbpool.Put(conn)
	if conn.Tracer() != nil {
		t.Error("Put left the tracer on an idle connection")
	}
}
