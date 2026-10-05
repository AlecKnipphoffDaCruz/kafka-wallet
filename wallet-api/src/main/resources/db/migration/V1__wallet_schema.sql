-- Wallet schema: accounts, transfers, a double-entry ledger and the transactional outbox.
--
-- Money is NUMERIC(19,4) everywhere. Never float: 0.1 + 0.2 must be 0.3 in a ledger.

create table accounts (
    id          uuid           primary key,
    owner_name  varchar(120)   not null,
    balance     numeric(19, 4) not null,
    currency    varchar(3)     not null,
    -- Optimistic lock. JPA bumps it on every update and refuses a write whose version is stale,
    -- which is what stops two concurrent transfers from losing each other's balance update.
    version     bigint         not null default 0,
    created_at  timestamptz    not null default now(),

    constraint accounts_balance_non_negative check (balance >= 0)
);

create table transfers (
    id                   uuid           primary key,
    -- The client's Idempotency-Key. UNIQUE is the actual guarantee: the lookup in the service is
    -- only a fast path, this constraint is what makes a retried POST impossible to double-apply.
    idempotency_key      varchar(120)   not null,
    -- SHA-256 of (from, to, amount, currency). Lets us tell "same request replayed" (return the
    -- original) apart from "same key, different body" (client bug -> 409).
    request_fingerprint  varchar(64)    not null,
    from_account         uuid           not null references accounts (id),
    to_account           uuid           not null references accounts (id),
    amount               numeric(19, 4) not null,
    currency             varchar(3)     not null,
    status               varchar(20)    not null,
    created_at           timestamptz    not null default now(),

    constraint transfers_idempotency_key_unique unique (idempotency_key),
    constraint transfers_amount_positive check (amount > 0),
    constraint transfers_distinct_accounts check (from_account <> to_account)
);

create index transfers_from_account_idx on transfers (from_account, created_at desc);

create table ledger_entries (
    id            uuid           primary key,
    transfer_id   uuid           not null references transfers (id),
    account_id    uuid           not null references accounts (id),
    entry_type    varchar(6)     not null,
    amount        numeric(19, 4) not null,
    currency      varchar(3)     not null,
    created_at    timestamptz    not null default now(),
    -- Derived, so the "a transfer nets to zero" invariant is one SUM() away and cannot drift out
    -- of sync with entry_type.
    signed_amount numeric(19, 4) generated always as
        (case when entry_type = 'DEBIT' then -amount else amount end) stored,

    constraint ledger_entry_type_valid check (entry_type in ('DEBIT', 'CREDIT')),
    constraint ledger_amount_positive check (amount > 0),
    -- Exactly one debit and one credit per transfer, enforced by the database rather than by hope.
    constraint ledger_one_entry_per_side unique (transfer_id, entry_type)
);

create index ledger_entries_account_idx on ledger_entries (account_id, created_at desc);

create table outbox_events (
    id             uuid         primary key,  -- doubles as the event's deduplication key
    aggregate_type varchar(40)  not null,
    aggregate_id   uuid         not null,
    event_type     varchar(60)  not null,
    partition_key  varchar(80)  not null,     -- becomes the Kafka message key
    payload        jsonb        not null,
    created_at     timestamptz  not null default now(),
    published_at   timestamptz,               -- null = still owed to Kafka
    attempts       integer      not null default 0,
    last_error     text
);

-- Partial index: the relay only ever asks for unpublished rows, and this keeps that query O(backlog)
-- instead of O(all events ever). Published rows drop out of the index as they are marked.
create index outbox_unpublished_idx on outbox_events (created_at) where published_at is null;
