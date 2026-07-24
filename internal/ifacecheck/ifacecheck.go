// Package ifacecheck discovers named types in a Go package that implement a
// given interface. Interface satisfaction is determined with go/types
// (types.Implements), so embedding, promoted methods, and pointer-vs-value
// receiver rules are handled by the standard library's type checker instead
// of being reimplemented here.
//
// Example:
//
//	implementers, err := ifacecheck.FindImplementers(ifacecheck.FindImplementersOptions{
//	    Dir:           ".",
//	    ImportPath:    "github.com/apache/cassandra-gocql-driver/v2",
//	    InterfaceName: "HostSelectionPolicy",
//	})
//
// Package files are type-checked using the "source" compiler importer
// (go/importer), which resolves imports from source rather than from
// precompiled export data. This package is internal-only: it can only be
// imported from within this module, so it only needs to work with the Go
// toolchain versions this module's own CI uses.
//
// Source files are considered only if they'd participate in the build that
// produced the currently running process (see currentBuildTags), so files
// gated behind a build tag that isn't active (for example, integration
// test files behind "//go:build ccm") are ignored the same way `go build`
// would ignore them.
package ifacecheck

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
)

// FindImplementersOptions configures FindImplementers.
type FindImplementersOptions struct {
	// Dir is the directory containing the package's source files. Defaults to ".".
	Dir string
	// ImportPath is the import path of the package to analyze, for example
	// "github.com/apache/cassandra-gocql-driver/v2". It is used both to
	// type-check the package and to label it in error messages. Required.
	ImportPath string
	// InterfaceName is the name of the interface, declared in the target
	// package, to find implementers of. Required.
	InterfaceName string
	// IncludeTestFiles additionally considers *_test.go files that declare
	// the target package itself (internal test files), as opposed to files
	// declaring a separate "<package>_test" package. Defaults to false.
	IncludeTestFiles bool
}

// FindImplementers returns the sorted names of every named type declared in
// the target package that implements InterfaceName. A type that implements
// the interface only through a pointer receiver is prefixed with "*", for
// example "*tokenAwareHostPolicy". Generic (type-parameterized) types are
// skipped, since determining implementers of an uninstantiated generic type
// is not well-defined.
func FindImplementers(opts FindImplementersOptions) ([]string, error) {
	if opts.ImportPath == "" {
		return nil, fmt.Errorf("ifacecheck: ImportPath is required")
	}
	if opts.InterfaceName == "" {
		return nil, fmt.Errorf("ifacecheck: InterfaceName is required")
	}

	dir := opts.Dir
	if dir == "" {
		dir = "."
	}

	fset := token.NewFileSet()
	files, err := parseDir(fset, dir, opts.IncludeTestFiles)
	if err != nil {
		return nil, err
	}

	pkg, err := checkPackage(fset, opts.ImportPath, files)
	if err != nil {
		return nil, err
	}

	iface, err := lookupInterface(pkg, opts.InterfaceName)
	if err != nil {
		return nil, err
	}

	return collectImplementers(pkg, iface), nil
}

// parseDir parses the *.go files directly inside dir that would participate
// in the current build and returns the ones belonging to the package under
// analysis. "Current build" means: honoring //go:build constraints using
// the same build tags (if any) that this process itself was compiled with
// (see currentBuildTags), so e.g. a file gated behind an opt-in tag that
// isn't active in the running test binary is ignored, just as `go build`
// would ignore it. Among the remaining files, those declaring "main" or an
// external "<package>_test" package are never part of the package under
// analysis; *_test.go files declaring the package itself are additionally
// excluded unless includeTestFiles is true.
func parseDir(fset *token.FileSet, dir string, includeTestFiles bool) ([]*ast.File, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return nil, fmt.Errorf("ifacecheck: list source files in %q: %w", dir, err)
	}

	buildCtxt := build.Default
	buildCtxt.BuildTags = currentBuildTags()

	filesByPackage := make(map[string][]*ast.File)
	for _, path := range paths {
		baseName := filepath.Base(path)

		match, err := buildCtxt.MatchFile(dir, baseName)
		if err != nil {
			return nil, fmt.Errorf("ifacecheck: evaluate build constraints for %s: %w", path, err)
		}
		if !match {
			continue
		}

		isTestFile := strings.HasSuffix(baseName, "_test.go")

		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("ifacecheck: parse %s: %w", path, err)
		}

		name := file.Name.Name
		if name == "main" || strings.HasSuffix(name, "_test") {
			continue
		}
		if isTestFile && !includeTestFiles {
			continue
		}

		filesByPackage[name] = append(filesByPackage[name], file)
	}

	switch len(filesByPackage) {
	case 0:
		return nil, fmt.Errorf("ifacecheck: no package found in %q", dir)
	case 1:
		for _, files := range filesByPackage {
			return files, nil
		}
	}
	return nil, fmt.Errorf("ifacecheck: multiple candidate packages found in %q", dir)
}

// currentBuildTags returns the -tags this process itself was built with
// (e.g. []string{"unit"} when the test binary was built via
// `go test -tags unit`), read from the embedded build info. It returns nil
// if no custom tags were set or build info isn't available.
func currentBuildTags() []string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return nil
	}
	for _, setting := range info.Settings {
		if setting.Key == "-tags" && setting.Value != "" {
			return strings.Split(setting.Value, ",")
		}
	}
	return nil
}

// checkPackage type-checks files as the package named importPath, resolving
// imports from source via go/importer. All interface-satisfaction logic is
// delegated to the returned *types.Package rather than reimplemented.
func checkPackage(fset *token.FileSet, importPath string, files []*ast.File) (*types.Package, error) {
	cfg := &types.Config{Importer: importer.ForCompiler(fset, "source", nil)}
	pkg, err := cfg.Check(importPath, fset, files, nil)
	if err != nil {
		return nil, fmt.Errorf("ifacecheck: type-check %q: %w", importPath, err)
	}
	return pkg, nil
}

// lookupInterface finds the interface type declared as name in pkg.
func lookupInterface(pkg *types.Package, name string) (*types.Interface, error) {
	obj := pkg.Scope().Lookup(name)
	if obj == nil {
		return nil, fmt.Errorf("ifacecheck: %q not found in package %q", name, pkg.Path())
	}

	typeName, ok := obj.(*types.TypeName)
	if !ok {
		return nil, fmt.Errorf("ifacecheck: %q is not a type", name)
	}

	iface, ok := typeName.Type().Underlying().(*types.Interface)
	if !ok {
		return nil, fmt.Errorf("ifacecheck: %q is not an interface", name)
	}
	return iface, nil
}

// collectImplementers returns the sorted names of the named, non-generic
// types declared in pkg whose value or pointer type implements iface.
func collectImplementers(pkg *types.Package, iface *types.Interface) []string {
	scope := pkg.Scope()

	implementers := make([]string, 0, len(scope.Names()))
	for _, name := range scope.Names() {
		typeName, ok := scope.Lookup(name).(*types.TypeName)
		if !ok || typeName.IsAlias() {
			continue
		}

		named, ok := typeName.Type().(*types.Named)
		if !ok || named.TypeParams() != nil {
			continue
		}
		if _, isInterface := named.Underlying().(*types.Interface); isInterface {
			continue
		}

		switch {
		case types.Implements(named, iface):
			implementers = append(implementers, name)
		case types.Implements(types.NewPointer(named), iface):
			implementers = append(implementers, "*"+name)
		}
	}

	sort.Strings(implementers)
	return implementers
}
