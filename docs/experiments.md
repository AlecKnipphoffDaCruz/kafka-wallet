# Kafka experiments (Phase 1)

Hands-on exercises against the running broker, using the CLI inside the Kafka container. Nothing
here involves the application — the point is to build intuition about partitions, groups, offsets
and ordering *before* building on top of them.

Every command assumes `docker compose up -d` is running. A shortcut, since the full path gets old:

```bash
k() { docker compose exec kafka /opt/kafka/bin/"$@"; }
```

> The broker listens on `kafka:19092` from inside the Docker network and `localhost:9092` from your
> machine. Commands run with `docker compose exec` are inside, so they use `kafka:19092`.

---

## Setup: a playground topic

```bash
k kafka-topics.sh --bootstrap-server kafka:19092 \
  --create --topic playground --partitions 3 --replication-factor 1

k kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic playground
```

The `describe` output shows one line per partition, each with a leader. Three partitions means at
most three consumers in one group can do useful work.

---

## Experiment 1 — partitions are split across a group

Two terminals, same `--group`:

```bash
# terminal A
k kafka-console-consumer.sh --bootstrap-server kafka:19092 \
  --topic playground --group demo-group --property print.partition=true

# terminal B — identical command
k kafka-console-consumer.sh --bootstrap-server kafka:19092 \
  --topic playground --group demo-group --property print.partition=true
```

Then produce:

```bash
k kafka-console-producer.sh --bootstrap-server kafka:19092 --topic playground
> one
> two
> three
> four
> five
> six
```

**What to look for:** each message appears in exactly *one* terminal, and `print.partition` shows
which partition it came from. The group's partitions were divided between the two members — one gets
two partitions, the other gets one.

**Why:** a partition is assigned to at most one consumer within a group. That is the unit of
consumer parallelism, and it is why a fourth consumer here would start, join, and then sit idle.

Confirm the assignment directly:

```bash
k kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group demo-group
```

The `CONSUMER-ID` column shows which member owns which partition.

---

## Experiment 2 — rebalancing

With both consumers still running, `Ctrl-C` terminal B and watch terminal A.

**What to look for:** a short pause, then terminal A starts receiving everything. Re-describe the
group and all three partitions now belong to one member.

**Why:** leaving the group triggers a rebalance — the coordinator reassigns the orphaned partitions
to the surviving members. Start terminal B again and it happens in reverse.

**The lesson that matters later:** during a rebalance, consumption stops. A consumer that takes too
long between polls (`max.poll.interval.ms`) looks dead to the coordinator and gets kicked out,
which triggers a rebalance, which makes everything slower — a feedback loop worth recognising. It
is also the reason `notification-service` caps `max.poll.records`.

---

## Experiment 3 — the key decides the partition, and ordering follows

```bash
k kafka-console-producer.sh --bootstrap-server kafka:19092 --topic playground \
  --property parse.key=true --property key.separator=:
> alice:first
> alice:second
> alice:third
> bob:first
> bob:second
```

Read it back with keys and partitions visible:

```bash
k kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic playground \
  --from-beginning --property print.key=true --property print.partition=true
```

**What to look for:** every `alice` message is on the same partition, in the order it was sent.
Same for `bob`, probably on a different one.

**Why:** the default partitioner hashes the key. Same key → same partition → guaranteed order.
Kafka makes **no** ordering promise across partitions, so with a null key (round-robin) the three
`alice` messages could be read in any order relative to each other.

**This is the whole reason** `transfer.completed` is keyed on the source account. One account's
transfers stay on one partition and therefore stay ordered.

---

## Experiment 4 — two groups each get everything

```bash
# terminal A
k kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic playground --group group-one

# terminal B
k kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic playground --group group-two
```

Produce one message.

**What to look for:** both terminals print it.

**Why:** offsets are tracked per group. A topic is a log, not a queue — reading does not consume.
That is what lets `notification-service` and `fraud-service` both process every transfer without
knowing the other exists, and what makes adding a fourth consumer a configuration change rather
than a redesign.

---

## Experiment 5 — resetting offsets to reprocess

Stop every consumer in the group first; the reset refuses to run while the group is active.

```bash
# Where is the group now?
k kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group demo-group

# Preview the change — no --execute, nothing is written
k kafka-consumer-groups.sh --bootstrap-server kafka:19092 \
  --group demo-group --topic playground --reset-offsets --to-earliest --dry-run

# Commit it
k kafka-consumer-groups.sh --bootstrap-server kafka:19092 \
  --group demo-group --topic playground --reset-offsets --to-earliest --execute
```

Start a consumer in `demo-group` again: it replays the topic from the beginning.

Other targets worth trying: `--to-latest` (skip everything), `--to-datetime
2026-10-05T12:00:00.000`, `--shift-by -10`.

**Why it matters:** this is the recovery tool for a consumer that processed events incorrectly —
fix the bug, reset, reprocess. It is only safe if processing is **idempotent**. Reset
`notification-service`'s group and nothing happens on the second pass, because `processed_events`
already holds every `eventId`. Without that table, every customer would be notified twice.

---

## Experiment 6 — the outbox holding the line

This one uses the application, and is the most convincing thing in the project.

```bash
# Make some accounts and note the ids
ALICE=$(curl -sX POST localhost:8080/accounts -H 'Content-Type: application/json' \
  -d '{"ownerName":"Alice","initialBalance":"1000.00","currency":"BRL"}' | jq -r .id)
BOB=$(curl -sX POST localhost:8080/accounts -H 'Content-Type: application/json' \
  -d '{"ownerName":"Bob","initialBalance":"0.00","currency":"BRL"}' | jq -r .id)

# Kill the broker
docker compose stop kafka

# Keep transferring — every one succeeds
for i in $(seq 1 5); do
  curl -sX POST localhost:8080/transfers \
    -H 'Content-Type: application/json' -H "Idempotency-Key: outage-$i" \
    -d "{\"fromAccountId\":\"$ALICE\",\"toAccountId\":\"$BOB\",\"amount\":\"10.00\",\"currency\":\"BRL\"}" \
    -o /dev/null -w '%{http_code}\n'
done

# The events are queued, not lost
curl -s localhost:8080/actuator/metrics/wallet.outbox.backlog | jq '.measurements[0].value'
# -> 5

docker compose start kafka
# Wait a few seconds, then check again
curl -s localhost:8080/actuator/metrics/wallet.outbox.backlog | jq '.measurements[0].value'
# -> 0
```

**What to look for:** the API returned `201` throughout. Balances were correct the whole time. The
relay logged failures and kept retrying, and the backlog drained once the broker came back.

**Why:** the event was committed to the database with the money. Kafka being down delays delivery;
it cannot lose it. Without the outbox, those five events would simply not exist — the transfers
would have succeeded and nothing downstream would ever know.

---

## Cleanup

```bash
k kafka-topics.sh --bootstrap-server kafka:19092 --delete --topic playground
k kafka-consumer-groups.sh --bootstrap-server kafka:19092 --delete --group demo-group
k kafka-consumer-groups.sh --bootstrap-server kafka:19092 --delete --group group-one
k kafka-consumer-groups.sh --bootstrap-server kafka:19092 --delete --group group-two
```

---

## Questions you should now be able to answer

1. What is a partition, and why is order only guaranteed *within* one?
2. What is the message key for?
3. Consumer vs. consumer group — what is the difference?
4. What happens when a consumer joins or leaves a group?
5. What is an offset, and when should it be committed?
6. At-most-once, at-least-once, exactly-once — which does this project use, and where?
7. Why does the outbox pattern exist? What breaks without it?
8. How do you make a consumer idempotent?
9. What is a DLQ for, and what should happen to what lands in it?
10. What is consumer lag, and what causes it to grow?
11. Why `acks=all` and `enable.idempotence=true`?
12. How many partitions should you pick, and what breaks if you increase it later?
