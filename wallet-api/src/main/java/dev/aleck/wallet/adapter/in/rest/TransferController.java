package dev.aleck.wallet.adapter.in.rest;

import dev.aleck.wallet.adapter.in.rest.dto.TransferRequest;
import dev.aleck.wallet.adapter.in.rest.dto.TransferResponse;
import dev.aleck.wallet.application.port.in.TransferMoneyUseCase;
import dev.aleck.wallet.application.port.in.TransferQueries;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.validation.annotation.Validated;

@RestController
@RequestMapping("/transfers")
@Validated
class TransferController {

    private final TransferMoneyUseCase transferMoney;
    private final TransferQueries transferQueries;
    private final Counter createdCounter;
    private final Counter replayedCounter;

    TransferController(
            TransferMoneyUseCase transferMoney,
            TransferQueries transferQueries,
            MeterRegistry meterRegistry) {
        this.transferMoney = transferMoney;
        this.transferQueries = transferQueries;
        this.createdCounter = Counter.builder("wallet.transfers")
                .tag("outcome", "created")
                .description("Transfers that moved money")
                .register(meterRegistry);
        this.replayedCounter = Counter.builder("wallet.transfers")
                .tag("outcome", "replayed")
                .description("Requests answered from an existing Idempotency-Key")
                .register(meterRegistry);
    }

    /**
     * {@code Idempotency-Key} is required, not optional.
     *
     * <p>A transfer is not safe to retry blindly, and a client that times out cannot tell a lost
     * request from a lost response. Making the header mandatory means every caller is retry-safe by
     * construction instead of by discipline.
     *
     * <p>Replaying a key returns {@code 200} with the original transfer; a first call returns
     * {@code 201}. Same body either way, so a client that ignores the distinction still behaves.
     */
    @PostMapping
    ResponseEntity<TransferResponse> transfer(
            @RequestHeader("Idempotency-Key") @NotBlank @Size(max = 120) String idempotencyKey,
            @Valid @RequestBody TransferRequest request) {

        TransferMoneyUseCase.TransferOutcome outcome = transferMoney.transfer(
                new TransferMoneyUseCase.TransferCommand(
                        idempotencyKey,
                        request.fromAccountId(),
                        request.toAccountId(),
                        request.amount(),
                        request.currency()));

        TransferResponse body = TransferResponse.from(outcome.transfer());

        if (outcome.replayed()) {
            replayedCounter.increment();
            return ResponseEntity.ok(body);
        }
        createdCounter.increment();
        return ResponseEntity.created(URI.create("/transfers/" + body.transferId())).body(body);
    }

    @GetMapping("/{transferId}")
    TransferResponse get(@PathVariable UUID transferId) {
        return TransferResponse.from(transferQueries.getTransfer(transferId));
    }
}
