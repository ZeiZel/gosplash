package archcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRun_Good проверяет, что фикстуры testdata/good — намеренно
// правильный код по всем трём правилам — не дают НИ ОДНОГО нарушения.
// В их числе прямая публикация в "домашних" каталогах (pkg/outbox) и
// прямая публикация с маркером-исключением в адаптере — оба случая
// должны молча пройти.
func TestRun_Good(t *testing.T) {
	violations, err := Run("testdata/good", false)
	require.NoError(t, err)
	assert.Empty(t, violations, "%v", violations)
}

// TestRun_Bad проверяет, что каждое нарушение во всех трёх фикстурах
// найдено РОВНО там, где ожидается, и больше нигде — свойство, а не
// конкретный текст сообщения (см. STYLE.md о тестах).
func TestRun_Bad(t *testing.T) {
	violations, err := Run("testdata/bad", false)
	require.NoError(t, err)

	type loc struct {
		file string
		line int
		rule string
	}
	want := []loc{
		{"pkg/views/batcher.go", 14, ruleDirectProduce},
		{"services/alpha/internal/app/service.go", 7, rulePortsAdapters},
		{"services/alpha/internal/app/service.go", 9, rulePortsAdapters},
		{"services/alpha/internal/domain/model.go", 7, rulePortsAdapters},
		{"services/alpha/internal/domain/model.go", 9, rulePortsAdapters},
		{"services/beta/internal/app/client.go", 5, ruleServiceIsolation},
	}

	require.Len(t, violations, len(want), "%v", violations)
	for i, v := range violations {
		assert.Equal(t, want[i].file, v.File, "нарушение %d: файл", i)
		assert.Equal(t, want[i].line, v.Line, "нарушение %d: строка", i)
		assert.Equal(t, want[i].rule, v.Rule, "нарушение %d: правило", i)
	}
}

// TestCollectFiles_SkipsSelfAndGenerated проверяет главное свойство
// обхода: archcheck не проверяет сам себя (иначе testdata с заведомо
// плохим кодом ловился бы при сканировании всего репозитория) и не лезет
// в сгенерированный код (gen/) и служебные каталоги (.git, vendor).
func TestCollectFiles_SkipsSelfAndGenerated(t *testing.T) {
	root := t.TempDir()
	write := func(rel, content string) {
		full := root + "/" + rel
		require.NoError(t, mkdirAll(full))
		require.NoError(t, writeFile(full, content))
	}

	// Настоящее нарушение — должно быть найдено.
	write("pkg/real/bad.go", `package real

import "context"

type p struct{}

func (x *p) Publish(ctx context.Context, topic string, payload []byte) error { return nil }

func Send(ctx context.Context) {
	(&p{}).Publish(ctx, "t", nil)
}
`)

	// То же самое нарушение, но внутри tools/archcheck/testdata — должно
	// быть проигнорировано целиком, каталог tools/archcheck исключён.
	write("tools/archcheck/testdata/whatever/bad.go", `package whatever

import "context"

type p struct{}

func (x *p) Publish(ctx context.Context, topic string, payload []byte) error { return nil }

func Send(ctx context.Context) {
	(&p{}).Publish(ctx, "t", nil)
}
`)

	// И то же самое в сгенерированном коде — тоже должно быть проигнорировано.
	write("gen/go/whatever/bad.go", `package whatever

import "context"

type p struct{}

func (x *p) Publish(ctx context.Context, topic string, payload []byte) error { return nil }

func Send(ctx context.Context) {
	(&p{}).Publish(ctx, "t", nil)
}
`)

	violations, err := Run(root, false)
	require.NoError(t, err)
	require.Len(t, violations, 1, "%v", violations)
	assert.Equal(t, "pkg/real/bad.go", violations[0].File)
}

// TestAllowedByMarker проверяет сам механизм исключения: маркер работает
// на строке вызова и строкой выше, ТРЕБУЕТ непустой причины и не путается
// с похожими, но другими маркерами.
func TestAllowedByMarker(t *testing.T) {
	tests := []struct {
		name       string
		src        string
		wantOK     bool
		wantReason string
	}{
		{
			name: "маркер строкой выше вызова",
			src: `package p
import "context"
type x struct{}
func (v *x) Publish(ctx context.Context, topic string, b []byte) error { return nil }
func f(ctx context.Context) error {
	v := &x{}
	// archcheck:allow direct-produce тестовая причина
	return v.Publish(ctx, "t", nil)
}
`,
			wantOK:     true,
			wantReason: "тестовая причина",
		},
		{
			name: "маркер на той же строке (trailing-комментарий)",
			src: `package p
import "context"
type x struct{}
func (v *x) Publish(ctx context.Context, topic string, b []byte) error { return nil }
func f(ctx context.Context) error {
	v := &x{}
	return v.Publish(ctx, "t", nil) // archcheck:allow direct-produce инлайн-причина
}
`,
			wantOK:     true,
			wantReason: "инлайн-причина",
		},
		{
			name: "маркер без причины не считается исключением",
			src: `package p
import "context"
type x struct{}
func (v *x) Publish(ctx context.Context, topic string, b []byte) error { return nil }
func f(ctx context.Context) error {
	v := &x{}
	// archcheck:allow direct-produce
	return v.Publish(ctx, "t", nil)
}
`,
			wantOK: false,
		},
		{
			name: "маркер слишком далеко от вызова не считается",
			src: `package p
import "context"
type x struct{}
func (v *x) Publish(ctx context.Context, topic string, b []byte) error { return nil }
// archcheck:allow direct-produce причина есть, но комментарий не рядом
func f(ctx context.Context) error {
	v := &x{}
	return v.Publish(ctx, "t", nil)
}
`,
			wantOK: false,
		},
		{
			name: "нет маркера вовсе",
			src: `package p
import "context"
type x struct{}
func (v *x) Publish(ctx context.Context, topic string, b []byte) error { return nil }
func f(ctx context.Context) error {
	v := &x{}
	return v.Publish(ctx, "t", nil)
}
`,
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := parseSource(t, tt.src)
			call := findPublishCall(t, f)
			line := f.fset.Position(call.Pos()).Line
			reason, ok := allowedByMarker(f, line)
			assert.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				assert.Equal(t, tt.wantReason, reason)
			}
		})
	}
}

// TestDirectProduceAllowlist_KnownException фиксирует ЕДИНСТВЕННОЕ
// известное исключение из direct-produce, которое нельзя оформить
// маркером в самом файле (catalog вне зоны правок archcheck). Если кто-то
// уберёт эту запись из allowlist.go, реальный прогон по репозиторию
// должен покраснеть на view_batcher.go — этот тест проверяет механизм
// в изоляции, не трогая настоящий файл catalog.
func TestDirectProduceAllowlist_KnownException(t *testing.T) {
	const known = "services/catalog/internal/adapters/kafka/view_batcher.go"
	reason, ok := directProduceAllowlist[known]
	require.True(t, ok, "ожидался известный exception для %s", known)
	assert.NotEmpty(t, reason)

	f := parseSource(t, `package kafka
import "context"
type EventPublisher interface {
	Publish(ctx context.Context, topic string, payload []byte) error
}
func send(ctx context.Context, p EventPublisher) error {
	return p.Publish(ctx, "catalog.photo.viewed", nil)
}
`)
	f.relPath = known

	violations := checkDirectProduce([]*file{f}, false)
	assert.Empty(t, violations, "запись в allowlist должна подавлять нарушение для этого файла")
}

// TestCheckPortsAdapters_KafkaxAllowedInApp фиксирует важный НЕ-негативный
// случай: gosplash/pkg/kafkax и gosplash/pkg/outbox — не инфраструктура
// в смысле этого правила (см. комментарий forbiddenInApp), и реальный
// internal/app всех сервисов их использует. Если бы правило запрещало
// kafkax, checkPortsAdapters ловил бы половину существующего кода.
func TestCheckPortsAdapters_KafkaxAllowedInApp(t *testing.T) {
	f := parseSource(t, `package app
import (
	"gosplash/pkg/kafkax"
	"gosplash/pkg/outbox"
)
var _ = kafkax.TopicPhotoUploaded
var _ = outbox.Write
`)
	f.relPath = "services/alpha/internal/app/service.go"

	assert.Empty(t, checkPortsAdapters([]*file{f}))
}
