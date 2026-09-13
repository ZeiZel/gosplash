package archcheck

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// parseSource парсит Go-исходник из строки в *file — без единого файла на
// диске. Используется точечными тестами правил (маркер, allowlist), где
// нужен контроль над относительным путём файла (f.relPath выставляется
// вызывающим тестом отдельно), а не полноценная фикстура в testdata/.
func parseSource(t *testing.T, src string) *file {
	t.Helper()
	fset := token.NewFileSet()
	astFile, err := parser.ParseFile(fset, "virtual.go", src, parser.ParseComments)
	require.NoError(t, err)
	return &file{relPath: "virtual.go", fset: fset, ast: astFile}
}

// findPublishCall находит единственный вызов метода Publish в файле —
// вспомогательная функция для тестов маркера-исключения, которым нужна
// строка вызова, а не сам факт нарушения.
func findPublishCall(t *testing.T, f *file) *ast.CallExpr {
	t.Helper()
	var found *ast.CallExpr
	ast.Inspect(f.ast, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if ok && sel.Sel.Name == "Publish" {
			found = call
		}
		return true
	})
	require.NotNil(t, found, "в исходнике теста нет вызова Publish")
	return found
}

func mkdirAll(fullFilePath string) error {
	return os.MkdirAll(filepath.Dir(fullFilePath), 0o755)
}

func writeFile(fullFilePath, content string) error {
	return os.WriteFile(fullFilePath, []byte(content), 0o644)
}
