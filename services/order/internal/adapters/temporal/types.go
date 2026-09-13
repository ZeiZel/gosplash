// Package temporal — сага PlaceOrderWorkflow: воркфлоу (workflow.go),
// activities (activities.go) и клиентский запуск воркфлоу (starter.go).
//
// Всё, что реально ходит в сеть или в базу, — в activities.go и
// starter.go. workflow.go — чистая оркестрация: см. его package doc про
// требование детерминизма (docs/adr/0004-saga-srazu-na-temporal.md,
// раздел «риск, за которым надо следить»).
//
// Входы и выходы воркфлоу и каждой activity — плоские структуры с
// экспортированными полями: Temporal сериализует их через свой
// DataConverter (по умолчанию — encoding/json) и кладёт в историю
// воркфлоу. Тип должен быть стабильным: переименование поля здесь —
// breaking change для уже запущенных, но ещё не завершённых воркфлоу.
package temporal

// PlaceOrderInput — вход PlaceOrderWorkflow.
//
// PayeeAccountID (счёт автора карточки) вычисляется ОДИН РАЗ до старта
// саги, в app.OrderService.PlaceOrder (там, где известна карточка), и
// дальше живёт только здесь — во входе воркфлоу, то есть в истории
// Temporal. В таблице orders этого поля нет (см. adapters/pg/model.go) —
// ровно демонстрация того, что ADR 0004 называет «данными саги»: они не
// нужны никому, кроме самой саги, и Temporal — их единственное хранилище.
type PlaceOrderInput struct {
	OrderID        string
	BuyerID        int64
	ListingID      string
	PriceCents     int64
	Currency       string
	PayeeAccountID int64
}

type reserveFundsInput struct {
	OrderID    string
	BuyerID    int64
	PriceCents int64
	Currency   string
}

type reserveFundsOutput struct {
	ReservationID string
}

type releaseFundsInput struct {
	OrderID       string
	ReservationID string
	Reason        string
}

type grantLicenseInput struct {
	OrderID   string
	ListingID string
	BuyerID   int64
}

type grantLicenseOutput struct {
	LicenseID string
}

type revokeLicenseInput struct {
	OrderID string
	Reason  string
}

type commitFundsInput struct {
	OrderID        string
	ReservationID  string
	PayeeAccountID int64
}

type commitFundsOutput struct {
	LedgerEntryIDs []string
}

type confirmOrderInput struct {
	OrderID    string
	LicenseID  string
	BuyerID    int64
	AuthorID   int64
	ListingID  string
	PriceCents int64
	Currency   string
}

type updateStatusInput struct {
	OrderID string
	Status  string
}

type failOrderInput struct {
	OrderID string
	Reason  string
}
