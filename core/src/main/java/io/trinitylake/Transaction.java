package io.trinitylake;

public interface Transaction {
    String transactionId();
    void commitTransaction();
}   
