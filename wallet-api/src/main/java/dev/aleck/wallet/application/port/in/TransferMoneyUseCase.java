package dev.aleck.wallet.application.port.in;

import dev.aleck.wallet.domain.Transfer;
import java.math.BigDecimal;
import java.util.UUID;

public interface TransferMoneyUseCase {

    TransferOutcome transfer(TransferCommand command);

    record TransferCommand(
            String idempotencyKey,
            UUID fromAccountId,
            UUID toAccountId,
            BigDecimal amount,
            String currency) {
    }

    /**
     * @param transfer the transfer as stored
     * @param replayed {@code true} when this call matched an existing {@code Idempotency-Key} and
     *                 no new money moved — the controller answers 200 instead of 201
     */
    record TransferOutcome(Transfer transfer, boolean replayed) {
    }
}
