package archcheck

import (
	"fmt"
	"go/ast"
	"strings"
)

const ruleDirectProduce = "direct-produce"

// produceMethods — имена методов, которые кладут сообщение в Kafka:
//   - Publish — kafkax.Producer.Publish и любой узкий интерфейс со своим
//     методом Publish поверх него (см. пакет doc про эвристику по имени);
//   - Produce/ProduceSync — методы *kgo.Client, которыми пользуется сам
//     pkg/kafkax (relay, retry/DLQ) и которыми в обход него мог бы
//     воспользоваться кто угодно, у кого в руках оказался kgo.Client —
//     а это само по себе уже подозрительно за пределами pkg/kafkax
//     и pkg/outbox.
var produceMethods = map[string]bool{
	"Publish":     true,
	"Produce":     true,
	"ProduceSync": true,
}

// directProduceHomeDirs — каталоги, где вызывать эти методы можно без
// исключений: это сам механизм outbox/kafka, и защищать его от самого
// себя бессмысленно.
var directProduceHomeDirs = []string{"pkg/outbox/", "pkg/kafkax/"}

// directProduceScopeDirs — где правило вообще действует. tools/** сюда
// намеренно не входит: STYLE.md прямо оговаривает утилиты в tools/ как
// исключение ("Единственное исключение — утилиты в tools/") — это
// однократно запускаемые руками админ-команды (kafka-reprocess переливает
// DLQ обратно в топик байт-в-байт), а не бизнес-сценарии сервиса, для
// которых и написано правило «публикация только через outbox». В объём
// правила входят pkg/** (общие библиотеки) и services/** (рантайм
// сервисов) — именно там цена прямого Produce та же, что описана в ADR 0008.
var directProduceScopeDirs = []string{"pkg/", "services/"}

// checkDirectProduce ищет вызовы produceMethods вне разрешённых каталогов
// и без действующего исключения (маркер в комментарии или запись в
// directProduceAllowlist, см. allowlist.go).
func checkDirectProduce(files []*file, verbose bool) []Violation {
	var out []Violation
	for _, f := range files {
		if !hasPrefixAny(f.relPath, directProduceScopeDirs) {
			continue
		}
		if hasPrefixAny(f.relPath, directProduceHomeDirs) {
			continue
		}

		ast.Inspect(f.ast, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !produceMethods[sel.Sel.Name] {
				return true
			}

			line := f.fset.Position(call.Pos()).Line

			if reason, ok := allowedByMarker(f, line); ok {
				if verbose {
					fmt.Printf("archcheck: %s:%d — %s разрешён маркером archcheck:allow (%s)\n",
						f.relPath, line, sel.Sel.Name, reason)
				}
				return true
			}
			if reason, ok := directProduceAllowlist[f.relPath]; ok {
				if verbose {
					fmt.Printf("archcheck: %s:%d — %s разрешён allowlist'ом (%s)\n",
						f.relPath, line, sel.Sel.Name, reason)
				}
				return true
			}

			out = append(out, Violation{
				File: f.relPath,
				Line: line,
				Rule: ruleDirectProduce,
				Message: fmt.Sprintf(
					"прямой вызов %s в обход outbox (docs/adr/0008-*, pkg/outbox.Write): "+
						"публикация в Kafka разрешена только из pkg/outbox и pkg/kafkax, "+
						"либо явным исключением — комментарий "+
						"«//archcheck:allow direct-produce <причина>» на этой строке или "+
						"строкой выше, либо запись в tools/archcheck/allowlist.go",
					sel.Sel.Name,
				),
			})
			return true
		})
	}
	return out
}

// allowedByMarker ищет комментарий-маркер вида
// `//archcheck:allow direct-produce <причина>` на строке вызова или сразу
// над ней. Причина ОБЯЗАНА быть непустой: маркер без объяснения не лучше
// отсутствия проверки — тот самый принцип STYLE.md про ADR, «чем платим»
// применённый к исключению из линтера.
func allowedByMarker(f *file, callLine int) (reason string, ok bool) {
	const marker = "archcheck:allow direct-produce"
	for _, cg := range f.ast.Comments {
		for _, c := range cg.List {
			text := strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(c.Text, "//"), "/*"), "*/")
			idx := strings.Index(text, marker)
			if idx < 0 {
				continue
			}
			line := f.fset.Position(c.Pos()).Line
			if line != callLine && line != callLine-1 {
				continue
			}
			reason = strings.TrimSpace(text[idx+len(marker):])
			if reason == "" {
				continue
			}
			return reason, true
		}
	}
	return "", false
}

func hasPrefixAny(path string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(path, p) {
			return true
		}
	}
	return false
}
