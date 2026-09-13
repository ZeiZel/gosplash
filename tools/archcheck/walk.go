package archcheck

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// file — один распарсенный .go файл вместе со всем, что нужно правилам:
// путь относительно корня сканирования (для сравнения с allowlist и для
// сообщений) и AST с комментариями (комментарии нужны direct-produce —
// маркер //archcheck:allow живёт именно в них).
type file struct {
	relPath string // всегда с "/", независимо от ОС
	fset    *token.FileSet
	ast     *ast.File
}

// skipDirs — каталоги, не несущие архитектурных решений проекта и потому
// исключённые из обхода:
//   - .git, vendor, node_modules — служебные, не Go-код проекта;
//   - gen — сгенерированный из .proto код: его форму задаёт buf, а не
//     эти правила, и ни domain, ни app, ни отдельный сервис в него не
//     импортируется по чужим правилам, только по прямому пути gen/go;
//   - tools/archcheck — сам анализатор. Критично исключить: в
//     tools/archcheck/testdata лежат ЗАВЕДОМО нарушающие правила примеры
//     для тестов самого archcheck, и без этого исключения `archcheck`,
//     запущенный по всему репозиторию, жаловался бы сам на себя.
var skipDirNames = map[string]bool{
	".git":         true,
	"vendor":       true,
	"node_modules": true,
	"gen":          true,
}

// skipRelPaths — то же самое, но по полному пути от корня сканирования,
// а не по имени каталога (иначе пришлось бы запретить директорию с именем
// "archcheck" вообще везде).
var skipRelPaths = map[string]bool{
	"tools/archcheck": true,
}

// collectFiles обходит root и парсит каждый .go файл. Парсинг — это
// go/parser.ParseFile: он строит AST из ТЕКСТА файла и не резолвит ни
// одного импорта, поэтому не требует ни go.mod, ни module cache, ни сети —
// в том числе для тестовых фикстур в testdata/, которые сами по себе
// невалидный (нерезолвящийся) Go-модуль.
func collectFiles(root string) ([]*file, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}

	var files []*file
	walkErr := filepath.WalkDir(absRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, rerr := filepath.Rel(absRoot, path)
		if rerr != nil {
			return rerr
		}
		rel = filepath.ToSlash(rel)

		if d.IsDir() {
			if rel == "." {
				return nil
			}
			if skipDirNames[d.Name()] || skipRelPaths[rel] {
				return filepath.SkipDir
			}
			return nil
		}

		if !strings.HasSuffix(rel, ".go") {
			return nil
		}

		fset := token.NewFileSet()
		astFile, perr := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if perr != nil {
			// Файл, который не парсится, — забота `go build`/`go vet`,
			// не архитектурных правил. Не роняем весь обход из-за одного
			// битого файла (например, временно недописанного во время
			// правки кем-то ещё).
			return nil
		}
		files = append(files, &file{relPath: rel, fset: fset, ast: astFile})
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}
	return files, nil
}
