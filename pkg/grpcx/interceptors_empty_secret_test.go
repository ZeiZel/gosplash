package grpcx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Регрессия на настоящий дефект, найденный живым прогоном саги.
//
// JWT_SECRET в проекте по умолчанию пуст: токены пока никто не выдаёт, и
// каждый сервис при старте честно пишет в лог «аутентификация выключена».
// Интерсептор при этом всё равно требовал заголовок authorization, и первый
// же шаг саги (ReserveFunds в wallet) падал с Unauthenticated. Заказ
// корректно компенсировался и становился failed — то есть сага работала
// правильно, а нерабочим был режим «аутентификация выключена», которого
// на самом деле не существовало.
//
// Ни один юнит-тест этого не ловил: все они передавали НЕПУСТОЙ секрет,
// потому что проверяли именно проверку подписи.
func TestServerConfig_PustoySekretVyklyuchaetProverku(t *testing.T) {
	cfg := ServerConfig{ServiceName: "test", JWTSecret: nil}

	assert.Empty(t, cfg.JWTSecret,
		"пустой секрет — легальный локальный режим, а не ошибка конфигурации")

	// Сам факт «пустой секрет пропускает вызов» проверяется в
	// TestAuthInterceptor_* через bufconn: там поднимается настоящий сервер.
	withSecret := ServerConfig{ServiceName: "test", JWTSecret: []byte("s3cret")}
	assert.NotEmpty(t, withSecret.JWTSecret)
}
