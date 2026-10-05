package dev.aleck.wallet.application.port.in;

import dev.aleck.wallet.domain.Account;
import java.math.BigDecimal;

public interface OpenAccountUseCase {

    Account open(OpenAccountCommand command);

    record OpenAccountCommand(String ownerName, BigDecimal initialBalance, String currency) {
    }
}
