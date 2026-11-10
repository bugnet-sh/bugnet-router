package provider_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// optional are the optional interfaces of a provider, which only provider.As may assert to
var optional = map[string]bool{
	"Pather": true, "StartCoster": true, "Sizer": true, "EgressSourcer": true, "Gater": true,
	"NativeSSHer": true, "HookVerifier": true, "LimitsReader": true, "AccountChecker": true,
	"UsageReader": true, "ForeignLister": true, "Parker": true, "PoolClaimer": true,
	"ImageDeleter": true, "ImageSaver": true,
}

// TestNoDirectOptionalAssertions walks the module for a type assertion or a type switch case that
// names an optional provider interface, which a traced wrapper would hide
func TestNoDirectOptionalAssertions(t *testing.T) {
	root := filepath.Join("..", "..")
	var bad []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") ||
			strings.HasSuffix(path, "_test.go") {
			return err
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		inProvider := f.Name.Name == "provider" && filepath.Base(filepath.Dir(path)) == "provider"
		check := func(e ast.Expr, pos token.Pos) {
			switch x := e.(type) {
			case *ast.SelectorExpr:
				if id, ok := x.X.(*ast.Ident); ok && id.Name == "provider" && optional[x.Sel.Name] {
					bad = append(bad, fset.Position(pos).String())
				}
			case *ast.Ident:
				if inProvider && optional[x.Name] {
					bad = append(bad, fset.Position(pos).String())
				}
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.TypeAssertExpr:
				if x.Type != nil {
					check(x.Type, x.Pos())
				}
			case *ast.TypeSwitchStmt:
				for _, c := range x.Body.List {
					for _, e := range c.(*ast.CaseClause).List {
						check(e, e.Pos())
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(bad) > 0 {
		t.Fatalf("type assertions to an optional provider interface, use provider.As: %v", bad)
	}
}

// TestAdaptersImportNoTracing pins that no adapter can add a trace header to a provider call,
// since none imports OpenTelemetry (DP 20.1)
func TestAdaptersImportNoTracing(t *testing.T) {
	for _, name := range []string{"boat", "boxd", "e2b", "koyeb", "local"} {
		files, err := filepath.Glob(filepath.Join(name, "*.go"))
		if err != nil || len(files) == 0 {
			t.Fatalf("no files for %s: %v", name, err)
		}
		for _, path := range files {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatal(err)
			}
			for _, im := range f.Imports {
				if strings.Contains(im.Path.Value, "opentelemetry") ||
					strings.Contains(im.Path.Value, "/telemetry") {
					t.Errorf("%s imports %s", path, im.Path.Value)
				}
			}
		}
	}
}
