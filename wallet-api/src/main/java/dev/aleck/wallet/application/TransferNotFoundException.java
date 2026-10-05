package dev.aleck.wallet.application;

import java.util.UUID;

public class TransferNotFoundException extends RuntimeException {

    public TransferNotFoundException(UUID transferId) {
        super("transfer not found: " + transferId);
    }
}
