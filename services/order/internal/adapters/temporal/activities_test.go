package temporal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

// fakeWalletSaga воспроизводит идемпотентный контракт wallet: повтор
// ReserveFunds/CommitFunds с тем же order_id возвращает ТОТ ЖЕ результат
// и не создаёт вторую запись — см. proto/gosplash/wallet/v1/wallet.proto.
type fakeWalletSaga struct {
	reservations   map[string]string // orderID -> reservationID
	reserveCalls   int               // сколько раз РЕАЛЬНО создан новый резерв
	commitCalls    int
	releaseReasons []string
}

func newFakeWalletSaga() *fakeWalletSaga {
	return &fakeWalletSaga{reservations: map[string]string{}}
}

func (w *fakeWalletSaga) ReserveFunds(_ context.Context, orderID string, _, _ int64, _ string) (string, error) {
	if id, ok := w.reservations[orderID]; ok {
		return id, nil
	}
	w.reserveCalls++
	id := "res-" + orderID
	w.reservations[orderID] = id
	return id, nil
}

func (w *fakeWalletSaga) CommitFunds(_ context.Context, _, _ string, _ int64) ([]string, error) {
	w.commitCalls++
	return []string{"ledger-1"}, nil
}

func (w *fakeWalletSaga) ReleaseFunds(_ context.Context, _, _, reason string) error {
	w.releaseReasons = append(w.releaseReasons, reason)
	return nil
}

// fakeCatalogSaga — аналогично, идемпотентность GrantLicense по order_id.
type fakeCatalogSaga struct {
	licenses    map[string]string // orderID -> licenseID
	grantCalls  int
	revokeCalls int
}

func newFakeCatalogSaga() *fakeCatalogSaga {
	return &fakeCatalogSaga{licenses: map[string]string{}}
}

func (c *fakeCatalogSaga) GrantLicense(_ context.Context, orderID, _ string, _ int64) (string, error) {
	if id, ok := c.licenses[orderID]; ok {
		return id, nil
	}
	c.grantCalls++
	id := "license-" + orderID
	c.licenses[orderID] = id
	return id, nil
}

func (c *fakeCatalogSaga) RevokeLicense(_ context.Context, orderID, _ string) (bool, error) {
	c.revokeCalls++
	_, existed := c.licenses[orderID]
	delete(c.licenses, orderID)
	return existed, nil
}

// fakeSagaOrderRepo — подделка ports.SagaOrderRepository.
type fakeSagaOrderRepo struct {
	statuses      map[string]string
	completed     map[string]string // orderID -> licenseID
	completeCalls int
	failed        map[string]string
}

func newFakeSagaOrderRepo() *fakeSagaOrderRepo {
	return &fakeSagaOrderRepo{
		statuses:  map[string]string{},
		completed: map[string]string{},
		failed:    map[string]string{},
	}
}

func (r *fakeSagaOrderRepo) UpdateStatus(_ context.Context, id, status string) error {
	r.statuses[id] = status
	return nil
}

func (r *fakeSagaOrderRepo) Complete(_ context.Context, id, licenseID string, _ *eventsv1.Envelope) error {
	r.completeCalls++
	r.completed[id] = licenseID
	return nil
}

func (r *fakeSagaOrderRepo) Fail(_ context.Context, id, reason string) error {
	r.failed[id] = reason
	return nil
}

// TestActivities_ReserveFunds_IdempotentnoPoOrderID — вызов ReserveFunds
// дважды с одним order_id не создаёт второй резерв (docs/adr/0004:
// "activity вызывается дважды с одним idempotency_key, второй вызов не
// должен изменить состояние").
func TestActivities_ReserveFunds_IdempotentnoPoOrderID(t *testing.T) {
	wallet := newFakeWalletSaga()
	acts := NewActivities(wallet, newFakeCatalogSaga(), newFakeSagaOrderRepo())

	first, err := acts.ReserveFunds(context.Background(), reserveFundsInput{
		OrderID: "order-1", BuyerID: 1, PriceCents: 1000, Currency: "RUB",
	})
	require.NoError(t, err)

	second, err := acts.ReserveFunds(context.Background(), reserveFundsInput{
		OrderID: "order-1", BuyerID: 1, PriceCents: 1000, Currency: "RUB",
	})
	require.NoError(t, err)

	assert.Equal(t, first.ReservationID, second.ReservationID, "повтор обязан вернуть тот же reservation_id")
	assert.Equal(t, 1, wallet.reserveCalls, "второй вызов не должен создавать новый резерв")
}

// TestActivities_GrantLicense_IdempotentnoPoOrderID — то же для catalog.
func TestActivities_GrantLicense_IdempotentnoPoOrderID(t *testing.T) {
	catalog := newFakeCatalogSaga()
	acts := NewActivities(newFakeWalletSaga(), catalog, newFakeSagaOrderRepo())

	first, err := acts.GrantLicense(context.Background(), grantLicenseInput{
		OrderID: "order-1", ListingID: "listing-1", BuyerID: 1,
	})
	require.NoError(t, err)

	second, err := acts.GrantLicense(context.Background(), grantLicenseInput{
		OrderID: "order-1", ListingID: "listing-1", BuyerID: 1,
	})
	require.NoError(t, err)

	assert.Equal(t, first.LicenseID, second.LicenseID, "повтор обязан вернуть ту же лицензию")
	assert.Equal(t, 1, catalog.grantCalls, "второй вызов не должен выдавать вторую лицензию")
}

// TestActivities_RevokeLicense_BezopasnaBezPredshestvuyushegoGrant —
// компенсация не падает, даже если GrantLicense никогда не вызывался
// (см. package doc workflow.go про то, зачем RevokeLicense кладётся
// в стек компенсаций заранее).
func TestActivities_RevokeLicense_BezopasnaBezPredshestvuyushegoGrant(t *testing.T) {
	acts := NewActivities(newFakeWalletSaga(), newFakeCatalogSaga(), newFakeSagaOrderRepo())

	err := acts.RevokeLicense(context.Background(), revokeLicenseInput{OrderID: "order-1", Reason: "тест"})

	require.NoError(t, err)
}

func TestActivities_ConfirmOrder_ZapisyvaetZaversheniyeIOutbox(t *testing.T) {
	repo := newFakeSagaOrderRepo()
	acts := NewActivities(newFakeWalletSaga(), newFakeCatalogSaga(), repo)

	err := acts.ConfirmOrder(context.Background(), confirmOrderInput{
		OrderID: "order-1", LicenseID: "license-1", BuyerID: 1, AuthorID: 7,
		ListingID: "listing-1", PriceCents: 1500, Currency: "RUB",
	})

	require.NoError(t, err)
	assert.Equal(t, "license-1", repo.completed["order-1"])
	assert.Equal(t, 1, repo.completeCalls)
}

func TestActivities_FailOrder_ZapisyvaetPrichinu(t *testing.T) {
	repo := newFakeSagaOrderRepo()
	acts := NewActivities(newFakeWalletSaga(), newFakeCatalogSaga(), repo)

	err := acts.FailOrder(context.Background(), failOrderInput{OrderID: "order-1", Reason: "wallet недоступен"})

	require.NoError(t, err)
	assert.Equal(t, "wallet недоступен", repo.failed["order-1"])
}
