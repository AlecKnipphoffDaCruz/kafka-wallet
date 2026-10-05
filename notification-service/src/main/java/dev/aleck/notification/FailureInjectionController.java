package dev.aleck.notification;

import java.util.Map;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Drives {@link FailureInjector} over HTTP so retry and DLQ behaviour can be demonstrated against
 * a running broker.
 *
 * <p>Disabled unless {@code notification.failure-injection.enabled=true}. An endpoint that can make
 * a service fail on demand is a liability anywhere it is not deliberately wanted, so the default
 * is off rather than "remember to turn it off".
 */
@RestController
@RequestMapping("/test/failure-injection")
@ConditionalOnProperty(name = "notification.failure-injection.enabled", havingValue = "true")
class FailureInjectionController {

    private final FailureInjector failureInjector;

    FailureInjectionController(FailureInjector failureInjector) {
        this.failureInjector = failureInjector;
    }

    @PostMapping
    Map<String, Object> arm(
            @RequestParam FailureInjector.Mode mode,
            @RequestParam(defaultValue = "2") int times) {

        failureInjector.arm(mode, times);
        return Map.of("mode", failureInjector.mode(), "remainingFailures", failureInjector.remainingFailures());
    }
}
