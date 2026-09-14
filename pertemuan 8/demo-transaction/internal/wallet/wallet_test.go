package wallet

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// --- Mocking (stuntman untuk database) ---

type mockTx struct{ mock.Mock }

func (m *mockTx) GetBalance(ctx context.Context, accountID int64) (int64, error) {
	args := m.Called(ctx, accountID)
	return args.Get(0).(int64), args.Error(1)
}

func (m *mockTx) AddBalance(ctx context.Context, accountID int64, delta int64) error {
	args := m.Called(ctx, accountID, delta)
	return args.Error(0)
}

func (m *mockTx) GetVotesLocked(ctx context.Context, contestID int64) (int64, error) {
	args := m.Called(ctx, contestID)
	return args.Get(0).(int64), args.Error(1)
}

func (m *mockTx) SetVotes(ctx context.Context, contestID int64, value int64) error {
	args := m.Called(ctx, contestID, value)
	return args.Error(0)
}

func (m *mockTx) MarkKey(ctx context.Context, key string, amount int64) (bool, error) {
	args := m.Called(ctx, key, amount)
	return args.Bool(0), args.Error(1)
}

func (m *mockTx) Commit(ctx context.Context) error   { return m.Called(ctx).Error(0) }
func (m *mockTx) Rollback(ctx context.Context) error { return m.Called(ctx).Error(0) }

type mockStore struct{ mock.Mock }

func (m *mockStore) Begin(ctx context.Context) (Tx, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(Tx), args.Error(1)
}

func (m *mockStore) GetBalance(ctx context.Context, accountID int64) (int64, error) {
	args := m.Called(ctx, accountID)
	return args.Get(0).(int64), args.Error(1)
}

func (m *mockStore) SetBalance(ctx context.Context, accountID int64, value int64) error {
	return m.Called(ctx, accountID, value).Error(0)
}

func (m *mockStore) GetVotes(ctx context.Context, contestID int64) (int64, error) {
	args := m.Called(ctx, contestID)
	return args.Get(0).(int64), args.Error(1)
}

func (m *mockStore) SetVotes(ctx context.Context, contestID int64, value int64) error {
	return m.Called(ctx, contestID, value).Error(0)
}

// --- Positive test: sukses -> Commit jalan, Rollback TIDAK ---

func TestTransferSafe_HappyPath_Commits(t *testing.T) {
	// Arrange
	tx := new(mockTx)
	store := new(mockStore)
	store.On("Begin", mock.Anything).Return(tx, nil)

	tx.On("GetBalance", mock.Anything, int64(1)).Return(int64(1000), nil) // pengirim
	tx.On("GetBalance", mock.Anything, int64(2)).Return(int64(500), nil)  // penerima
	tx.On("AddBalance", mock.Anything, int64(1), int64(-100)).Return(nil)
	tx.On("AddBalance", mock.Anything, int64(2), int64(100)).Return(nil)
	tx.On("Commit", mock.Anything).Return(nil)
	// sengaja TIDAK ada .On("Rollback", ...) — kalau dipanggil, mock akan panik.

	svc := New(store)

	// Act
	err := svc.TransferSafe(context.Background(), 1, 2, 100)

	// Assert
	assert.NoError(t, err)
	tx.AssertCalled(t, "Commit", mock.Anything)
	tx.AssertNotCalled(t, "Rollback", mock.Anything)
	tx.AssertExpectations(t)
}

// --- Negative test: gagal -> Rollback jalan, Commit TIDAK ---

func TestTransferSafe_InsufficientFunds_RollsBack(t *testing.T) {
	tx := new(mockTx)
	store := new(mockStore)
	store.On("Begin", mock.Anything).Return(tx, nil)

	tx.On("GetBalance", mock.Anything, int64(1)).Return(int64(50), nil) // saldo < amount
	tx.On("Rollback", mock.Anything).Return(nil)
	// sengaja TIDAK ada .On("Commit", ...)

	svc := New(store)

	err := svc.TransferSafe(context.Background(), 1, 2, 100)

	assert.ErrorIs(t, err, ErrInsufficientFunds)
	tx.AssertCalled(t, "Rollback", mock.Anything)
	tx.AssertNotCalled(t, "Commit", mock.Anything)
	tx.AssertNotCalled(t, "AddBalance", mock.Anything, mock.Anything, mock.Anything)
	tx.AssertExpectations(t)
}

// --- Negative test: gagal di tengah -> Rollback, tidak ada side effect ---

func TestTransferSafe_AddFails_RollsBack(t *testing.T) {
	tx := new(mockTx)
	store := new(mockStore)
	store.On("Begin", mock.Anything).Return(tx, nil)

	tx.On("GetBalance", mock.Anything, int64(1)).Return(int64(1000), nil)
	tx.On("AddBalance", mock.Anything, int64(1), int64(-100)).Return(nil)

	// Kegagalan di langkah penerima (mis. melanggar CHECK limit / koneksi putus).
	tx.On("GetBalance", mock.Anything, int64(2)).Return(int64(500), nil)
	tx.On("AddBalance", mock.Anything, int64(2), int64(100)).Return(errors.New("db down"))
	tx.On("Rollback", mock.Anything).Return(nil)

	svc := New(store)

	err := svc.TransferSafe(context.Background(), 1, 2, 100)

	assert.Error(t, err)
	tx.AssertCalled(t, "Rollback", mock.Anything)
	tx.AssertNotCalled(t, "Commit", mock.Anything)
}

// --- Idempotency: key duplikat -> ErrDuplicate, tidak potong saldo ---

func TestPaySafe_DuplicateKey_Rejected(t *testing.T) {
	tx := new(mockTx)
	store := new(mockStore)
	store.On("Begin", mock.Anything).Return(tx, nil)

	tx.On("MarkKey", mock.Anything, "order-12345", int64(30000)).Return(false, nil)
	tx.On("Rollback", mock.Anything).Return(nil)

	svc := New(store)

	err := svc.PaySafe(context.Background(), 1, 30000, "order-12345")

	assert.ErrorIs(t, err, ErrDuplicate)
	tx.AssertNotCalled(t, "AddBalance", mock.Anything, mock.Anything, mock.Anything)
	tx.AssertNotCalled(t, "Commit", mock.Anything)
	tx.AssertExpectations(t)
}

func TestPaySafe_FirstRequest_Commits(t *testing.T) {
	tx := new(mockTx)
	store := new(mockStore)
	store.On("Begin", mock.Anything).Return(tx, nil)

	tx.On("MarkKey", mock.Anything, "order-12345", int64(30000)).Return(true, nil)
	tx.On("GetBalance", mock.Anything, int64(1)).Return(int64(100000), nil)
	tx.On("AddBalance", mock.Anything, int64(1), int64(-30000)).Return(nil)
	tx.On("Commit", mock.Anything).Return(nil)

	svc := New(store)

	err := svc.PaySafe(context.Background(), 1, 30000, "order-12345")

	assert.NoError(t, err)
	tx.AssertCalled(t, "Commit", mock.Anything)
	tx.AssertNotCalled(t, "Rollback", mock.Anything)
}

// --- Jalur unsafe: membuktikan tidak ada rollback & side effect tetap jalan ---

func TestTransferUnsafe_PartialWrite_NoRollback(t *testing.T) {
	store := new(mockStore)

	// langkah 1 (potong pengirim) sukses & langsung tersimpan
	store.On("GetBalance", mock.Anything, int64(1)).Return(int64(1000), nil)
	store.On("SetBalance", mock.Anything, int64(1), int64(900)).Return(nil)
	// langkah 2 (tambah penerima) gagal, tapi langkah 1 sudah kadung commit
	store.On("GetBalance", mock.Anything, int64(2)).Return(int64(500), nil)
	store.On("SetBalance", mock.Anything, int64(2), int64(600)).Return(errors.New("db down"))

	svc := New(store)

	err := svc.TransferUnsafe(context.Background(), 1, 2, 100)

	assert.Error(t, err)
	// Bukti bahaya: potongan saldo pengirim tetap dieksekusi walau transfer gagal.
	store.AssertCalled(t, "SetBalance", mock.Anything, int64(1), int64(900))
	store.AssertExpectations(t)
}

func TestTransferUnsafe_InsufficientFunds(t *testing.T) {
	store := new(mockStore)
	store.On("GetBalance", mock.Anything, int64(1)).Return(int64(50), nil)

	svc := New(store)
	err := svc.TransferUnsafe(context.Background(), 1, 2, 100)

	assert.ErrorIs(t, err, ErrInsufficientFunds)
	store.AssertNotCalled(t, "SetBalance", mock.Anything, mock.Anything, mock.Anything)
}

func TestVoteUnsafe_ReadModifyWrite(t *testing.T) {
	store := new(mockStore)
	store.On("GetVotes", mock.Anything, int64(1)).Return(int64(7), nil)
	store.On("SetVotes", mock.Anything, int64(1), int64(8)).Return(nil)

	svc := New(store)
	err := svc.VoteUnsafe(context.Background(), 1)

	assert.NoError(t, err)
	store.AssertCalled(t, "SetVotes", mock.Anything, int64(1), int64(8))
}

func TestPayUnsafe_AlwaysCharges(t *testing.T) {
	tx := new(mockTx)
	store := new(mockStore)
	store.On("Begin", mock.Anything).Return(tx, nil)

	tx.On("GetBalance", mock.Anything, int64(1)).Return(int64(100000), nil)
	tx.On("AddBalance", mock.Anything, int64(1), int64(-30000)).Return(nil)
	tx.On("Commit", mock.Anything).Return(nil)

	svc := New(store)
	// Dua request dengan key yang sama: tanpa idempotency keduanya tetap ditagih.
	assert.NoError(t, svc.PayUnsafe(context.Background(), 1, 30000))
	assert.NoError(t, svc.PayUnsafe(context.Background(), 1, 30000))
	tx.AssertNumberOfCalls(t, "AddBalance", 2)
}

func TestTransferSafe_AmountNotPositive(t *testing.T) {
	svc := New(new(mockStore))
	assert.Error(t, svc.TransferSafe(context.Background(), 1, 2, 0))
}

func TestPaySafe_RequiresKey(t *testing.T) {
	svc := New(new(mockStore))
	assert.Error(t, svc.PaySafe(context.Background(), 1, 30000, ""))
}

// --- Voting: jalur aman mengunci baris sebelum menulis ---

func TestVoteSafe_LocksThenIncrements(t *testing.T) {
	tx := new(mockTx)
	store := new(mockStore)
	store.On("Begin", mock.Anything).Return(tx, nil)

	tx.On("GetVotesLocked", mock.Anything, int64(1)).Return(int64(10), nil)
	tx.On("SetVotes", mock.Anything, int64(1), int64(11)).Return(nil)
	tx.On("Commit", mock.Anything).Return(nil)

	svc := New(store)

	err := svc.VoteSafe(context.Background(), 1)

	assert.NoError(t, err)
	tx.AssertCalled(t, "GetVotesLocked", mock.Anything, int64(1))
	tx.AssertCalled(t, "SetVotes", mock.Anything, int64(1), int64(11))
	tx.AssertExpectations(t)
}