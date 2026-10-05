package dev.aleck.wallet.adapter.in.rest.dto;

import dev.aleck.wallet.domain.Account;
import com.fasterxml.jackson.annotation.JsonFormat;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record AccountResponse(
        UUID id,
        String ownerName,
        /*
         * A JSON string, not a number — the same reason as in the event contract. A JSON number is
         * an IEEE-754 double to most clients, and serialising as one also strips the trailing
         * zeros that make 850.0000 recognisable as money.
         */
        @JsonFormat(shape = JsonFormat.Shape.STRING) BigDecimal balance,
        String currency,
        long version,
        Instant createdAt) {

    public static AccountResponse from(Account account) {
        return new AccountResponse(
                account.id(),
                account.ownerName(),
                account.balance().amount(),
                account.balance().currency(),
                account.version(),
                account.createdAt());
    }
}
