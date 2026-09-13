// Package migrations — схема wallet-сервиса.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: в остальном проекте миграции применяет отдельный
// бинарник tools/automigrate (см. его заголовок пакета: «единственное
// место в монорепе, которому разрешено знать про все сервисы сразу»).
// Зона ответственности этой фазы — только services/wallet/** и mk/wallet.mk;
// tools/** трогать нельзя. Поэтому Migrate вызывается напрямую из
package migrations

import (
	"fmt"

	"gorm.io/gorm"

	"gosplash/pkg/idempotency"
	"gosplash/pkg/outbox"
	"gosplash/services/wallet/internal/adapters/pg"
)

// Migrate создаёт схему wallet: счета, проводки, резервы, идемпотентность
// операций, processed_events и outbox.
func Migrate(db *gorm.DB) error {
	if err := db.AutoMigrate(
		&pg.AccountRow{},
		&pg.LedgerEntryRow{},
		&pg.ReservationRow{},
		&pg.OperationRow{},
		&idempotency.ProcessedEvent{},
	); err != nil {
		return fmt.Errorf("схема wallet: %w", err)
	}

	// Очередь outbox — отдельным вызовом: имя таблицы (outbox_wallet)
	// зависит от сервиса, и знает об этом сам пакет (pkg/outbox.TableFor),
	// а не вызывающий.
	if err := outbox.Migrate(db, "wallet"); err != nil {
		return err
	}

	if err := ensureLedgerAppendOnly(db); err != nil {
		return err
	}
	return nil
}

// ensureLedgerAppendOnly закрепляет ЗАПРЕТ UPDATE/DELETE по ledger_entries
// не только соглашением в коде, но и в самой СУБД: триггер, отклоняющий
// любую попытку изменить или удалить проводку, независимо от того, какой
// код и с какими правами к базе подключился.
//
// Почему это важно именно здесь: инвариант «сумма всех проводок равна нулю»
// (см. internal/domain/domain.go) доказывает целостность истории ТОЛЬКО
// пока каждая проводка написана один раз и никогда не менялась. Если
// разрешить UPDATE, инвариант остаётся математически верным даже для
// подделанной книги — он перестаёт быть доказательством. Триггер, а не
// REVOKE на роль СУБД: все сервисы проекта подключаются одной и той же
// ролью (см. docker-compose), и REVOKE UPDATE у этой роли заблокировал бы
// и саму миграцию (AutoMigrate по этой же роли может менять структуру
// столбца при повторных запусках), и любые будущие легитимные ADMIN-
// операции. Триггер на конкретную таблицу — точнее: он не даёт УТВЕРЖДЕНИЕ
// «эта роль не пишет вообще», а даёт «эти конкретные строки не
// переписываются НИКЕМ и никогда», что и требуется append-only книге.
func ensureLedgerAppendOnly(db *gorm.DB) error {
	const guardFn = `
CREATE OR REPLACE FUNCTION wallet_ledger_entries_append_only() RETURNS trigger AS $$
BEGIN
	RAISE EXCEPTION 'ledger_entries append-only: UPDATE и DELETE запрещены (см. docs/adr/0014-*)';
END;
$$ LANGUAGE plpgsql;
`
	if err := db.Exec(guardFn).Error; err != nil {
		return fmt.Errorf("функция-страж append-only: %w", err)
	}

	const guardTrigger = `
DROP TRIGGER IF EXISTS trg_ledger_entries_append_only ON ledger_entries;
CREATE TRIGGER trg_ledger_entries_append_only
	BEFORE UPDATE OR DELETE ON ledger_entries
	FOR EACH ROW EXECUTE FUNCTION wallet_ledger_entries_append_only();
`
	if err := db.Exec(guardTrigger).Error; err != nil {
		return fmt.Errorf("триггер append-only: %w", err)
	}
	return nil
}
