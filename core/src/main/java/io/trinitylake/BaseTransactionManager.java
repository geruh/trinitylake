package io.trinitylake;

import java.util.Optional;

public class BaseTransactionManager implements TransactionManager {

    private static final ThreadLocal<Transaction> activeTransaction = ThreadLocal.withInitial(() -> null);

    @Override
    public Optional<Transaction> currentTransaction() {
        return Optional.ofNullable(activeTransaction.get());
    }
    
    @Override
    public void beginTransaction() {
        if (activeTransaction.get() != null) {
            throw new IllegalStateException("Transaction already in progress");
        }
        Transaction transaction = new BaseTransaction();
        activeTransaction.set(transaction);
    }

    @Override
    public void commitTransaction() {
        Transaction transaction = activeTransaction.get();
        if (transaction == null) {
            throw new IllegalStateException("No active transaction to commit");
        }
        transaction.commitTransaction();
        activeTransaction.remove();
    }

    @Override
    public void rollbackTransaction() {
        Transaction transaction = activeTransaction.get();
        if (transaction == null) {
            throw new IllegalStateException("No active transaction to rollback");
        }
        activeTransaction.remove();
    }   
}
