package dev.aleck.wallet.adapter.in.rest;

import dev.aleck.wallet.application.AccountNotFoundException;
import dev.aleck.wallet.application.ConcurrencyConflictException;
import dev.aleck.wallet.application.IdempotencyKeyReusedException;
import dev.aleck.wallet.application.TransferNotFoundException;
import dev.aleck.wallet.domain.CurrencyMismatchException;
import dev.aleck.wallet.domain.InsufficientFundsException;
import dev.aleck.wallet.domain.SameAccountTransferException;
import java.net.URI;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Maps domain and application failures to RFC 7807 problem documents.
 *
 * <p>This class is the reason the inner layers throw meaningful exceptions instead of returning
 * status codes: HTTP is a detail of this adapter, and the same use case could be driven by a Kafka
 * listener or a CLI without rewriting any of it.
 *
 * <p>The status codes are chosen deliberately:
 * <ul>
 *   <li>{@code 422} for a request that is well-formed but cannot be satisfied (no funds) — retrying
 *       it unchanged will not help.
 *   <li>{@code 409} for a conflict the client may be able to resolve: lock contention that
 *       outlasted our retries, or an {@code Idempotency-Key} reused for a different body.
 * </ul>
 */
@RestControllerAdvice
class ApiExceptionHandler {

    private static final String PROBLEM_BASE = "https://github.com/aleck/kafka-wallet/problems/";

    @ExceptionHandler(AccountNotFoundException.class)
    ProblemDetail handleAccountNotFound(AccountNotFoundException failure) {
        return problem(HttpStatus.NOT_FOUND, "account-not-found", "Account not found", failure.getMessage());
    }

    @ExceptionHandler(TransferNotFoundException.class)
    ProblemDetail handleTransferNotFound(TransferNotFoundException failure) {
        return problem(HttpStatus.NOT_FOUND, "transfer-not-found", "Transfer not found", failure.getMessage());
    }

    @ExceptionHandler(InsufficientFundsException.class)
    ProblemDetail handleInsufficientFunds(InsufficientFundsException failure) {
        ProblemDetail problem = problem(HttpStatus.UNPROCESSABLE_CONTENT, "insufficient-funds",
                "Insufficient funds", failure.getMessage());
        problem.setProperty("accountId", failure.accountId().toString());
        return problem;
    }

    @ExceptionHandler(SameAccountTransferException.class)
    ProblemDetail handleSameAccount(SameAccountTransferException failure) {
        return problem(HttpStatus.UNPROCESSABLE_CONTENT, "same-account-transfer",
                "Source and destination must differ", failure.getMessage());
    }

    @ExceptionHandler(CurrencyMismatchException.class)
    ProblemDetail handleCurrencyMismatch(CurrencyMismatchException failure) {
        ProblemDetail problem = problem(HttpStatus.UNPROCESSABLE_CONTENT, "currency-mismatch",
                "Currency mismatch", failure.getMessage());
        problem.setProperty("expected", failure.expected());
        problem.setProperty("actual", failure.actual());
        return problem;
    }

    @ExceptionHandler(IdempotencyKeyReusedException.class)
    ProblemDetail handleKeyReused(IdempotencyKeyReusedException failure) {
        return problem(HttpStatus.CONFLICT, "idempotency-key-reused",
                "Idempotency-Key reused for a different request", failure.getMessage());
    }

    @ExceptionHandler(ConcurrencyConflictException.class)
    ProblemDetail handleConcurrencyConflict(ConcurrencyConflictException failure) {
        ProblemDetail problem = problem(HttpStatus.CONFLICT, "concurrency-conflict",
                "Too much contention on these accounts", failure.getMessage());
        // Tells an honest client that retrying is the right move, unlike every other 4xx here.
        problem.setProperty("retryable", true);
        return problem;
    }

    /**
     * Over-precise money reaches the domain as an {@link ArithmeticException} from
     * {@code setScale(4, UNNECESSARY)}. Validation normally catches it first, so this is a
     * backstop for paths that bypass bean validation.
     */
    @ExceptionHandler(ArithmeticException.class)
    ProblemDetail handleArithmetic(ArithmeticException failure) {
        return problem(HttpStatus.UNPROCESSABLE_CONTENT, "amount-too-precise",
                "Amount has more than 4 decimal places", failure.getMessage());
    }

    private static ProblemDetail problem(HttpStatus status, String slug, String title, String detail) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setTitle(title);
        problem.setType(URI.create(PROBLEM_BASE + slug));
        return problem;
    }
}
