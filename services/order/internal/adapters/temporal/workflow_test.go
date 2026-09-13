package temporal

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"gosplash/services/order/internal/domain"
)

func placeOrderInput() PlaceOrderInput {
	return PlaceOrderInput{
		OrderID:        "order-1",
		BuyerID:        1,
		ListingID:      "listing-1",
		PriceCents:     1500,
		Currency:       "RUB",
		PayeeAccountID: 7,
	}
}

// TestPlaceOrderWorkflow_UspeshnyPut — все шаги проходят, компенсации не
// вызываются ни разу (AssertExpectations ниже проверит это неявно: моков
// на RevokeLicense/ReleaseFunds нет вовсе, и вызов незамоканной activity
// провалил бы тест).
func TestPlaceOrderWorkflow_UspeshnyPut(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.OnActivity(a.ReserveFunds, mock.Anything, reserveFundsInput{
		OrderID: "order-1", BuyerID: 1, PriceCents: 1500, Currency: "RUB",
	}).Return(reserveFundsOutput{ReservationID: "res-1"}, nil).Once()

	env.OnActivity(a.UpdateStatus, mock.Anything, updateStatusInput{
		OrderID: "order-1", Status: domain.StatusFundsReserved,
	}).Return(nil).Once()

	env.OnActivity(a.GrantLicense, mock.Anything, grantLicenseInput{
		OrderID: "order-1", ListingID: "listing-1", BuyerID: 1,
	}).Return(grantLicenseOutput{LicenseID: "license-1"}, nil).Once()

	env.OnActivity(a.UpdateStatus, mock.Anything, updateStatusInput{
		OrderID: "order-1", Status: domain.StatusLicenseGranted,
	}).Return(nil).Once()

	env.OnActivity(a.CommitFunds, mock.Anything, commitFundsInput{
		OrderID: "order-1", ReservationID: "res-1", PayeeAccountID: 7,
	}).Return(commitFundsOutput{LedgerEntryIDs: []string{"le-1", "le-2"}}, nil).Once()

	env.OnActivity(a.ConfirmOrder, mock.Anything, confirmOrderInput{
		OrderID: "order-1", LicenseID: "license-1", BuyerID: 1, AuthorID: 7,
		ListingID: "listing-1", PriceCents: 1500, Currency: "RUB",
	}).Return(nil).Once()

	env.ExecuteWorkflow(PlaceOrderWorkflow, placeOrderInput())

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

// TestPlaceOrderWorkflow_PadenieGrantLicense — GrantLicense проваливается
// ПОСЛЕ успешного ReserveFunds. Ожидание: выполняются ОБЕ компенсации,
// в обратном порядке (RevokeLicense — она относится к шагу, который как
// раз провалился, — затем ReleaseFunds), и заказ помечается failed.
// RevokeLicense вызывается несмотря на то, что GrantLicense не удался: это
// её контракт (безопасна даже когда отзывать нечего, см. workflow.go).
func TestPlaceOrderWorkflow_PadenieGrantLicense(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var order []string

	env.OnActivity(a.ReserveFunds, mock.Anything, mock.Anything).
		Return(reserveFundsOutput{ReservationID: "res-1"}, nil).Once()
	env.OnActivity(a.UpdateStatus, mock.Anything, updateStatusInput{
		OrderID: "order-1", Status: domain.StatusFundsReserved,
	}).Return(nil).Once()

	env.OnActivity(a.GrantLicense, mock.Anything, mock.Anything).
		Return(grantLicenseOutput{}, errors.New("catalog недоступен")).Once()

	env.OnActivity(a.UpdateStatus, mock.Anything, updateStatusInput{
		OrderID: "order-1", Status: domain.StatusCompensating,
	}).Run(func(mock.Arguments) { order = append(order, "compensating") }).Return(nil).Once()

	env.OnActivity(a.RevokeLicense, mock.Anything, revokeLicenseInput{
		OrderID: "order-1", Reason: "саговая компенсация: сага заказа не завершилась успехом",
	}).Run(func(mock.Arguments) { order = append(order, "revoke") }).Return(nil).Once()

	env.OnActivity(a.ReleaseFunds, mock.Anything, releaseFundsInput{
		OrderID: "order-1", ReservationID: "res-1", Reason: "саговая компенсация: сага заказа не завершилась успехом",
	}).Run(func(mock.Arguments) { order = append(order, "release") }).Return(nil).Once()

	env.OnActivity(a.FailOrder, mock.Anything, mock.MatchedBy(func(in failOrderInput) bool {
		return in.OrderID == "order-1" && in.Reason != ""
	})).Return(nil).Once()

	env.ExecuteWorkflow(PlaceOrderWorkflow, placeOrderInput())

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError(), "провал шага саги обязан завершить воркфлоу ошибкой")
	require.Equal(t, []string{"compensating", "revoke", "release"}, order,
		"компенсации обязаны идти в ОБРАТНОМ порядке: RevokeLicense (последний успешный/попытанный шаг) раньше ReleaseFunds")
	env.AssertExpectations(t)
}

// TestPlaceOrderWorkflow_PadenieCommitFunds — CommitFunds проваливается
// ПОСЛЕ успешных ReserveFunds и GrantLicense. Обе компенсации отрабатывают
// в том же обратном порядке, заказ помечается failed.
func TestPlaceOrderWorkflow_PadenieCommitFunds(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var order []string

	env.OnActivity(a.ReserveFunds, mock.Anything, mock.Anything).
		Return(reserveFundsOutput{ReservationID: "res-1"}, nil).Once()
	env.OnActivity(a.UpdateStatus, mock.Anything, updateStatusInput{
		OrderID: "order-1", Status: domain.StatusFundsReserved,
	}).Return(nil).Once()

	env.OnActivity(a.GrantLicense, mock.Anything, mock.Anything).
		Return(grantLicenseOutput{LicenseID: "license-1"}, nil).Once()
	env.OnActivity(a.UpdateStatus, mock.Anything, updateStatusInput{
		OrderID: "order-1", Status: domain.StatusLicenseGranted,
	}).Return(nil).Once()

	env.OnActivity(a.CommitFunds, mock.Anything, mock.Anything).
		Return(commitFundsOutput{}, errors.New("wallet недоступен")).Once()

	env.OnActivity(a.UpdateStatus, mock.Anything, updateStatusInput{
		OrderID: "order-1", Status: domain.StatusCompensating,
	}).Return(nil).Once()

	env.OnActivity(a.RevokeLicense, mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { order = append(order, "revoke") }).Return(nil).Once()
	env.OnActivity(a.ReleaseFunds, mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { order = append(order, "release") }).Return(nil).Once()

	env.OnActivity(a.FailOrder, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(PlaceOrderWorkflow, placeOrderInput())

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Equal(t, []string{"revoke", "release"}, order,
		"после успешных ReserveFunds и GrantLicense компенсации обязаны пройти в обратном порядке")
	env.AssertExpectations(t)
}
