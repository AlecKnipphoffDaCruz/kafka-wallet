package dev.aleck.wallet.domain;

/** Raised when two {@link Money} values of different currencies are combined or compared. */
public class CurrencyMismatchException extends RuntimeException {

    private final String expected;
    private final String actual;

    public CurrencyMismatchException(String expected, String actual) {
        super("currency mismatch: expected " + expected + " but got " + actual);
        this.expected = expected;
        this.actual = actual;
    }

    public String expected() {
        return expected;
    }

    public String actual() {
        return actual;
    }
}
