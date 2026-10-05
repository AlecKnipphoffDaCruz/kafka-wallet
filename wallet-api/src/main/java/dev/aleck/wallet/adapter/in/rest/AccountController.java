package dev.aleck.wallet.adapter.in.rest;

import dev.aleck.wallet.adapter.in.rest.dto.AccountResponse;
import dev.aleck.wallet.adapter.in.rest.dto.LedgerEntryResponse;
import dev.aleck.wallet.adapter.in.rest.dto.OpenAccountRequest;
import dev.aleck.wallet.application.port.in.AccountQueries;
import dev.aleck.wallet.application.port.in.OpenAccountUseCase;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/accounts")
class AccountController {

    private final OpenAccountUseCase openAccount;
    private final AccountQueries accountQueries;

    AccountController(OpenAccountUseCase openAccount, AccountQueries accountQueries) {
        this.openAccount = openAccount;
        this.accountQueries = accountQueries;
    }

    @PostMapping
    ResponseEntity<AccountResponse> open(@Valid @RequestBody OpenAccountRequest request) {
        AccountResponse created = AccountResponse.from(openAccount.open(
                new OpenAccountUseCase.OpenAccountCommand(
                        request.ownerName(), request.initialBalance(), request.currency())));

        return ResponseEntity.created(URI.create("/accounts/" + created.id())).body(created);
    }

    @GetMapping("/{accountId}")
    AccountResponse get(@PathVariable UUID accountId) {
        return AccountResponse.from(accountQueries.getAccount(accountId));
    }

    /**
     * The double-entry ledger for one account. Useful as a demo: every transfer shows up here as a
     * DEBIT on one side and a CREDIT on the other, and the two never exist apart.
     */
    @GetMapping("/{accountId}/ledger")
    List<LedgerEntryResponse> ledger(
            @PathVariable UUID accountId,
            @RequestParam(defaultValue = "50") @Min(1) @Max(500) int limit) {

        return accountQueries.getLedger(accountId, limit).stream()
                .map(LedgerEntryResponse::from)
                .toList();
    }
}
