package io.trinitylake;

import java.util.Optional;

public interface TransactionManager {
    Optional<Transaction> currentTransaction();
    void beginTransaction();
    void commitTransaction();
    void rollbackTransaction();
}
