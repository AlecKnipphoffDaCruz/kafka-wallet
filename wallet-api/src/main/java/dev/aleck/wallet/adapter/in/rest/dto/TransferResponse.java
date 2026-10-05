package dev.aleck.wallet.adapter.in.rest.dto;

import dev.aleck.wallet.domain.Transfer;
import com.fasterxml.jackson.annotation.JsonFormat;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record TransferResponse(
        UUID transferId,
        UUID fromAccountId,
        UUID toAccountId,
        /*
         * A JSON string, not a number — the same reason as in the event contract. A JSON number is
         * an IEEE-754 double to most clients, and serialising as one also strips the trailing
         * zeros that make 850.0000 recognisable as money.
         */
        @JsonFormat(shape = JsonFormat.Shape.STRING) BigDecimal amount,
        String currency,
        String status,
        Instant createdAt) {

    public static TransferResponse from(Transfer transfer) {
        return new TransferResponse(
                transfer.id(),
                transfer.fromAccountId(),
                transfer.toAccountId(),
                transfer.amount().amount(),
                transfer.amount().currency(),
                transfer.status().name(),
                transfer.createdAt());
    }
}
