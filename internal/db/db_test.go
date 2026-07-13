package db

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

func useTempWorkingDirectory(t *testing.T) {
	t.Helper()

	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd error: %v", err)
	}

	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatalf("Chdir temp directory failed: %v", err)
	}

	t.Cleanup(func() {
		if err := os.Chdir(oldDir); err != nil {
			t.Errorf("restore working directory failed: %v", err)
		}
	})
}

func TestFlushDeletesWALAndRestartReadsFromSSTable(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(2, 10)
	if err != nil {
		t.Fatalf("new db: %v", err)
	}

	if err := database.Put("apple", []byte("red")); err != nil {
		t.Fatalf("put apple: %v", err)
	}
	if err := database.Put("banana", []byte("yellow")); err != nil {
		t.Fatalf("put banana: %v", err)
	}

	if err := database.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// wal gets flushed to sstable
	if _, err := os.Stat("wal-0.log"); !os.IsNotExist(err) {
		t.Fatalf("wal-0.log should be deleted after flush, got err=%v", err)
	}

	// wal-1.log replaces wal-0.log after flush
	if _, err := os.Stat("wal-1.log"); err != nil {
		t.Fatalf("expected active wal-1.log to exist: %v", err)
	}

	// sstable created after flush
	if _, err := os.Stat("data-0.sst"); err != nil {
		t.Fatalf("expected data-0.sst to exist: %v", err)
	}

	// reopeen the database and check saved values
	reopened, err := New(2, 10)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()

	got, err := reopened.Get("apple")
	if err != nil {
		t.Fatalf("get apple after restart: %v", err)
	}
	if string(got) != "red" {
		t.Fatalf("get apple = %q, want red", got)
	}

	got, err = reopened.Get("banana")
	if err != nil {
		t.Fatalf("get banana after restart: %v", err)
	}
	if string(got) != "yellow" {
		t.Fatalf("get banana = %q, want yellow", got)
	}
}

func TestConcurrentReadersOfActiveMemtable(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(10_000, 100)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer database.Close()

	if err := database.Put("stable", []byte("value")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	var wg sync.WaitGroup

	for i := 0; i < 16; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < 500; j++ {
				got, err := database.Get("stable")
				if err != nil {
					t.Errorf("Get error: %v", err)
					return
				}

				if string(got) != "value" {
					t.Errorf("value = %q, want value", got)
					return
				}
			}
		}()
	}

	wg.Wait()
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(10_000, 100)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer database.Close()

	if err := database.Put("stable", []byte("value")); err != nil {
		t.Fatalf("initial Put: %v", err)
	}

	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < 250; j++ {
				got, err := database.Get("stable")
				if err != nil {
					t.Errorf("Get stable: %v", err)
					return
				}

				if string(got) != "value" {
					t.Errorf("stable = %q, want value", got)
					return
				}
			}
		}()
	}

	for writer := 0; writer < 4; writer++ {
		wg.Add(1)

		go func(writer int) {
			defer wg.Done()

			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("writer-%d-%d", writer, j)

				if err := database.Put(key, []byte("x")); err != nil {
					t.Errorf("Put %q: %v", key, err)
					return
				}
			}
		}(writer)
	}

	wg.Wait()
}

func TestConcurrentFirstReadsAfterReopen(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(2, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := database.Put("apple", []byte("red")); err != nil {
		t.Fatalf("Put apple: %v", err)
	}
	if err := database.Put("banana", []byte("yellow")); err != nil {
		t.Fatalf("Put banana: %v", err)
	}

	if err := database.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	reopened, err := New(2, 100)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	var wg sync.WaitGroup

	for i := 0; i < 16; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			got, err := reopened.Get("apple")
			if err != nil {
				t.Errorf("Get apple: %v", err)
				return
			}

			if string(got) != "red" {
				t.Errorf("a = %q, want red", got)
			}
		}()
	}

	wg.Wait()
}

func TestConcurrentGetDuringBackgroundFlush(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(4, 100)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer database.Close()

	if err := database.Put("stable", []byte("value")); err != nil {
		t.Fatalf("Put stable: %v", err)
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 200; i++ {
			got, err := database.Get("stable")
			if err != nil {
				t.Errorf("Get stable: %v", err)
				return
			}

			if string(got) != "value" {
				t.Errorf("stable = %q, want value", got)
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key-%d", i)

			if err := database.Put(key, []byte("x")); err != nil {
				t.Errorf("Put %q: %v", key, err)
				return
			}
		}
	}()

	wg.Wait()
}
