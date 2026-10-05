package dev.aleck.wallet.domain;

/**
 * Side of a double-entry posting.
 *
 * <p>{@code DEBIT} takes money out of an account, {@code CREDIT} puts money in. Both are stored
 * with a positive amount; the sign lives in the type. The schema derives a {@code signed_amount}
 * column from it so the "every transfer sums to zero" invariant is one SQL query away.
 */
public enum LedgerEntryType {
    DEBIT,
    CREDIT
}
