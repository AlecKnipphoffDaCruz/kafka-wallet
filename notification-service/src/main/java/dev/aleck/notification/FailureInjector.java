package dev.aleck.notification;

import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

/**
 * A switch for making the consumer fail on purpose.
 *
 * <p>Retry and dead-lettering are the parts of a consumer you most need to see working and least
 * want to find out about in production. This makes them demonstrable: flip the mode, send a
 * transfer, and watch the backoff in the logs and the message land in the DLQ.
 *
 * <p>Deliberately in the main source set, not in tests — the point is to exercise the real
 * container, the real error handler and the real broker. The endpoint that drives it is disabled
 * by default and should stay that way outside a demo.
 */
@Component
class FailureInjector {

    enum Mode {
        /** Normal operation. */
        NONE,
        /** Fail the next N deliveries, then succeed — shows retry recovering. */
        TRANSIENT,
        /** Fail every delivery — shows the retry budget running out and the DLQ catching it. */
        ALWAYS
    }

    private volatile Mode mode = Mode.NONE;
    private final AtomicInteger remainingFailures = new AtomicInteger();

    void arm(Mode mode, int times) {
        this.remainingFailures.set(times);
        this.mode = mode;
    }

    Mode mode() {
        return mode;
    }

    int remainingFailures() {
        return remainingFailures.get();
    }

    void failIfArmed(String eventId) {
        switch (mode) {
            case NONE -> {
                // nothing to do
            }
            case ALWAYS -> throw new IllegalStateException(
                    "injected permanent failure for event " + eventId);
            case TRANSIENT -> {
                if (remainingFailures.getAndDecrement() > 0) {
                    throw new IllegalStateException(
                            "injected transient failure for event " + eventId);
                }
                mode = Mode.NONE;
            }
        }
    }
}
