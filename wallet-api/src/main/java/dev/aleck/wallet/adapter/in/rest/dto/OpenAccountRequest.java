package dev.aleck.wallet.adapter.in.rest.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;

public record OpenAccountRequest(
        @NotBlank @Size(max = 120) String ownerName,

        @NotNull
        @DecimalMin(value = "0.0000", message = "initial balance cannot be negative")
        // Matches NUMERIC(19,4). Rejecting over-precise input here is what lets the domain refuse
        // to round money silently.
        @Digits(integer = 15, fraction = 4)
        BigDecimal initialBalance,

        @NotBlank
        @Pattern(regexp = "^[A-Za-z]{3}$", message = "currency must be a 3-letter ISO code")
        String currency) {
}
