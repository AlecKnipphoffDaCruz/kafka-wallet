package dev.aleck.wallet.application.service;

import dev.aleck.wallet.application.TransferNotFoundException;
import dev.aleck.wallet.application.port.in.TransferQueries;
import dev.aleck.wallet.application.port.out.TransferRepository;
import dev.aleck.wallet.domain.Transfer;
import java.util.UUID;

public class TransferQueryService implements TransferQueries {

    private final TransferRepository transfers;

    public TransferQueryService(TransferRepository transfers) {
        this.transfers = transfers;
    }

    @Override
    public Transfer getTransfer(UUID transferId) {
        return transfers.findById(transferId).orElseThrow(() -> new TransferNotFoundException(transferId));
    }
}
