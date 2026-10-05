package dev.aleck.wallet.adapter.in.rest.dto;

import dev.aleck.wallet.domain.LedgerEntry;
import com.fasterxml.jackson.annotation.JsonFormat;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record LedgerEntryResponse(
        UUID id,
        UUID transferId,
        UUID accountId,
        String type,
        /*
         * A JSON string, not a number — the same reason as in the event contract. A JSON number is
         * an IEEE-754 double to most clients, and serialising as one also strips the trailing
         * zeros that make 850.0000 recognisable as money.
         */
        @JsonFormat(shape = JsonFormat.Shape.STRING) BigDecimal amount,
        String currency,
        Instant createdAt) {

    public static LedgerEntryResponse from(LedgerEntry entry) {
        return new LedgerEntryResponse(
                entry.id(),
                entry.transferId(),
                entry.accountId(),
                entry.type().name(),
                entry.amount().amount(),
                entry.amount().currency(),
                entry.createdAt());
    }
}
