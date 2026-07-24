package ifacecheck

import (
	"path/filepath"
	"runtime"
	"testing"
)

const gocqlImportPath = "github.com/apache/cassandra-gocql-driver/v2"

func repoRoot(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
}

func TestFindImplementers_HostSelectionPolicy(t *testing.T) {
	implementers, err := FindImplementers(FindImplementersOptions{
		Dir:              repoRoot(t),
		ImportPath:       gocqlImportPath,
		InterfaceName:    "HostSelectionPolicy",
		IncludeTestFiles: true,
	})
	if err != nil {
		t.Fatalf("FindImplementers() error = %v", err)
	}

	want := []string{
		"*dcAwareRR",
		"*rackAwareRR",
		"*roundRobinHostPolicy",
		"*singleHostReadyPolicy",
		"*tokenAwareHostPolicy",
	}
	for _, name := range want {
		found := false
		for _, implementer := range implementers {
			if implementer == name {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("FindImplementers() = %v, missing %q", implementers, name)
		}
	}
}

func TestFindImplementers_HostStateNotifier(t *testing.T) {
	implementers, err := FindImplementers(FindImplementersOptions{
		Dir:              repoRoot(t),
		ImportPath:       gocqlImportPath,
		InterfaceName:    "HostStateNotifier",
		IncludeTestFiles: true,
	})
	if err != nil {
		t.Fatalf("FindImplementers() error = %v", err)
	}

	if len(implementers) < 4 {
		t.Fatalf("expected at least 4 HostStateNotifier implementers, got %v", implementers)
	}
}

func TestFindImplementers_ExcludesTestFilesByDefault(t *testing.T) {
	withTests, err := FindImplementers(FindImplementersOptions{
		Dir:              repoRoot(t),
		ImportPath:       gocqlImportPath,
		InterfaceName:    "HostSelectionPolicy",
		IncludeTestFiles: true,
	})
	if err != nil {
		t.Fatalf("FindImplementers() error = %v", err)
	}

	withoutTests, err := FindImplementers(FindImplementersOptions{
		Dir:           repoRoot(t),
		ImportPath:    gocqlImportPath,
		InterfaceName: "HostSelectionPolicy",
	})
	if err != nil {
		t.Fatalf("FindImplementers() error = %v", err)
	}

	if len(withoutTests) > len(withTests) {
		t.Fatalf("excluding test files should never add implementers: with=%v without=%v", withTests, withoutTests)
	}
}

func TestFindImplementers_MissingInterface(t *testing.T) {
	_, err := FindImplementers(FindImplementersOptions{
		Dir:              repoRoot(t),
		ImportPath:       gocqlImportPath,
		InterfaceName:    "DoesNotExist",
		IncludeTestFiles: true,
	})
	if err == nil {
		t.Fatal("expected error for missing interface")
	}
}

func TestFindImplementers_MissingInterfaceName(t *testing.T) {
	_, err := FindImplementers(FindImplementersOptions{
		Dir:        repoRoot(t),
		ImportPath: gocqlImportPath,
	})
	if err == nil {
		t.Fatal("expected error for empty interface name")
	}
}

func TestFindImplementers_MissingImportPath(t *testing.T) {
	_, err := FindImplementers(FindImplementersOptions{
		Dir:           repoRoot(t),
		InterfaceName: "HostSelectionPolicy",
	})
	if err == nil {
		t.Fatal("expected error for empty import path")
	}
}

func TestFindImplementers_Example(t *testing.T) {
	t.Log(repoRoot(t))
	implementers, err := FindImplementers(FindImplementersOptions{
		Dir:           repoRoot(t),
		ImportPath:    gocqlImportPath + "/hostpool",
		InterfaceName: "HostSelectionPolicy",
	})
	if err != nil {
		t.Fatalf("FindImplementers() error = %v", err)
	}

	t.Fatal(implementers)
}
