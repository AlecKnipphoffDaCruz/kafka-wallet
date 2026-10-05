package dev.aleck.wallet.application.port.in;

import dev.aleck.wallet.domain.Transfer;
import java.util.UUID;

public interface TransferQueries {

    Transfer getTransfer(UUID transferId);
}
