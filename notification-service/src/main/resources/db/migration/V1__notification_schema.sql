-- notification-service owns its own schema. Nothing here references the wallet tables: the only
-- contract between the two services is the event on the topic.

-- The idempotency ledger. One row per (event, consumer) that has been fully processed.
--
-- Why it exists: the outbox relay delivers at-least-once, and a consumer that crashes after
-- sending a notification but before committing its offset will see the same event again. Without
-- this table that means a second notification for one transfer.
--
-- Why 'consumer' is part of the key: several logical consumers may share this database, and each
-- must get its own shot at every event. Keying on event_id alone would let the first consumer to
-- process an event silently suppress it for the others.
create table processed_events (
    event_id     uuid        not null,
    consumer     varchar(80) not null,
    processed_at timestamptz not null default now(),

    constraint processed_events_pkey primary key (event_id, consumer)
);

create table notifications (
    id          uuid         primary key,
    event_id    uuid         not null,
    transfer_id uuid         not null,
    account_id  uuid         not null,
    channel     varchar(20)  not null,
    message     varchar(500) not null,
    created_at  timestamptz  not null default now()
);

create index notifications_transfer_idx on notifications (transfer_id);

-- A notification is the side effect we are protecting. One per event, enforced by the database so
-- a bug in the dedup logic surfaces as a constraint violation rather than as a duplicate SMS.
create unique index notifications_event_unique on notifications (event_id);
