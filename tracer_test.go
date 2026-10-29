package sqlite_test

import (
	"testing"

	sqlite "github.com/go-llsqlite/crawshaw"
)

// recTracer records the tasks a Conn reports.
type recTracer struct {
	tasks  []*recTask
	groups []string
	plain  bool // return tasks that do not implement TracerTaskWithError
}

type recTask struct {
	name     string
	group    string
	steps    int
	inRegion bool
	ended    int
	err      error
}

// plainTask is a TracerTask without EndWithError.
type plainTask struct{ t *recTask }

func (p plainTask) StartRegion(r string) { p.t.StartRegion(r) }
func (p plainTask) EndRegion()           { p.t.EndRegion() }
func (p plainTask) End()                 { p.t.ended++ }

func (tr *recTracer) NewTask(name string) sqlite.TracerTask {
	task := &recTask{name: name}
	if len(tr.groups) > 0 {
		task.group = tr.groups[len(tr.groups)-1]
	}
	tr.tasks = append(tr.tasks, task)
	if tr.plain {
		return plainTask{task}
	}
	return task
}

func (tr *recTracer) Push(name string) { tr.groups = append(tr.groups, name) }
func (tr *recTracer) Pop()             { tr.groups = tr.groups[:len(tr.groups)-1] }

func (t *recTask) StartRegion(regionType string) {
	if t.inRegion {
		panic("StartRegion: already in region")
	}
	if regionType != "Step" {
		panic("StartRegion: unexpected region " + regionType)
	}
	t.inRegion = true
}

func (t *recTask) EndRegion() {
	t.inRegion = false
	t.steps++
}

func (t *recTask) End() { panic("End called on a TracerTaskWithError") }

func (t *recTask) EndWithError(err error) {
	t.ended++
	t.err = err
}

// checkTasks checks that every task ended once, outside a region.
func checkTasks(t *testing.T, tr *recTracer) {
	t.Helper()
	for _, task := range tr.tasks {
		if task.ended != 1 || task.inRegion {
			t.Errorf("task %q: ended %d times, inRegion=%v; want ended once outside a region", task.name, task.ended, task.inRegion)
		}
	}
}

func step(t *testing.T, stmt *sqlite.Stmt) bool {
	t.Helper()
	hasRow, err := stmt.Step()
	if err != nil {
		t.Fatal(err)
	}
	return hasRow
}

func TestTracer(t *testing.T) {
	conn, err := sqlite.OpenConn(":memory:", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tr := &recTracer{}
	conn.SetTracer(tr)

	// lastTask returns the only task created since the previous call.
	seen := 0
	lastTask := func(t *testing.T, query string) *recTask {
		t.Helper()
		if n := len(tr.tasks) - seen; n != 1 {
			t.Fatalf("%d new tasks, want 1", n)
		}
		seen = len(tr.tasks)
		task := tr.tasks[seen-1]
		if task.name != query {
			t.Fatalf("task name %q, want %q", task.name, query)
		}
		return task
	}
	noTask := func(t *testing.T) {
		t.Helper()
		if n := len(tr.tasks) - seen; n != 0 {
			t.Fatalf("%d new tasks, want 0", n)
		}
	}

	const create = "CREATE TABLE t (a INTEGER PRIMARY KEY);"
	stmt := conn.Prep(create)
	noTask(t) // preparing alone is not an execution
	step(t, stmt)
	if task := lastTask(t, create); task.steps != 1 || task.err != nil {
		t.Errorf("CREATE: steps %d, err %v; want 1, nil", task.steps, task.err)
	}

	// Each execution of a cached statement is its own task, whether it is
	// prepared again or only reset.
	const insert = "INSERT INTO t (a) VALUES ($a);"
	for i := 1; i <= 2; i++ {
		stmt := conn.Prep(insert)
		stmt.SetInt64("$a", int64(i))
		step(t, stmt)
		lastTask(t, insert)
	}
	stmt.Reset()
	stmt = conn.Prep(insert)
	stmt.SetInt64("$a", 3)
	step(t, stmt)
	lastTask(t, insert)
	stmt.Reset()
	stmt.SetInt64("$a", 4)
	step(t, stmt)
	lastTask(t, insert)

	t.Run("error", func(t *testing.T) {
		stmt := conn.Prep(insert)
		stmt.SetInt64("$a", 1)
		if _, err := stmt.Step(); sqlite.ErrCode(err) != sqlite.SQLITE_CONSTRAINT_PRIMARYKEY {
			t.Fatalf("duplicate insert: got %v, want SQLITE_CONSTRAINT_PRIMARYKEY", err)
		}
		if task := lastTask(t, insert); task.err == nil {
			t.Error("task of a failed statement ended without an error")
		}
	})

	t.Run("bind error", func(t *testing.T) {
		stmt := conn.Prep(insert)
		stmt.SetInt64("$nope", 1)
		if _, err := stmt.Step(); err == nil {
			t.Fatal("Step with a bind error succeeded")
		}
		if task := lastTask(t, insert); task.err == nil || task.steps != 0 {
			t.Errorf("bind error: steps %d, err %v; want 0 steps and an error", task.steps, task.err)
		}
	})

	const query = "SELECT a FROM t ORDER BY a;"
	t.Run("reset early", func(t *testing.T) {
		stmt := conn.Prep(query)
		step(t, stmt)
		step(t, stmt)
		task := lastTask(t, query)
		if task.ended != 0 {
			t.Fatal("task ended while the statement still has rows")
		}
		stmt.Reset()
		if task.ended != 1 || task.err != nil || task.steps != 2 {
			t.Errorf("after Reset: ended %d, steps %d, err %v; want 1, 2, nil", task.ended, task.steps, task.err)
		}
	})

	t.Run("prepare again early", func(t *testing.T) {
		stmt := conn.Prep(query)
		step(t, stmt)
		task := lastTask(t, query)
		conn.Prep(query) // resets the cached statement
		if task.ended != 1 {
			t.Errorf("task ended %d times after Prep, want 1", task.ended)
		}
	})

	t.Run("transient", func(t *testing.T) {
		stmt, _, err := conn.PrepareTransient(query)
		if err != nil {
			t.Fatal(err)
		}
		for step(t, stmt) {
		}
		if task := lastTask(t, query); task.steps != 5 || task.ended != 1 {
			t.Errorf("transient: steps %d, ended %d; want 5, 1", task.steps, task.ended)
		}
		stmt.Finalize()
	})

	t.Run("finalize early", func(t *testing.T) {
		stmt, _, err := conn.PrepareTransient(query)
		if err != nil {
			t.Fatal(err)
		}
		step(t, stmt)
		task := lastTask(t, query)
		stmt.Finalize()
		if task.ended != 1 {
			t.Errorf("task ended %d times after Finalize, want 1", task.ended)
		}
	})

	t.Run("group", func(t *testing.T) {
		tr.Push("TX")
		stepAll(t, conn.Prep("SELECT 1;"))
		tr.Pop()
		if task := lastTask(t, "SELECT 1;"); task.group != "TX" {
			t.Errorf("task group %q, want TX", task.group)
		}
	})

	checkTasks(t, tr)

	t.Run("plain task", func(t *testing.T) {
		plain := &recTracer{plain: true}
		conn.SetTracer(plain)
		stepAll(t, conn.Prep("SELECT 1;"))
		stmt := conn.Prep(query)
		step(t, stmt)
		stmt.Reset()
		if len(plain.tasks) != 2 {
			t.Fatalf("%d tasks, want 2", len(plain.tasks))
		}
		checkTasks(t, plain)
	})

	t.Run("no tracer", func(t *testing.T) {
		conn.SetTracer(nil)
		stepAll(t, conn.Prep("SELECT 1;"))
		noTask(t)
	})
}

func stepAll(t *testing.T, stmt *sqlite.Stmt) {
	t.Helper()
	for step(t, stmt) {
	}
}
