package dev.aleck.notification;

import java.util.List;
import java.util.UUID;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read-only window into what the consumer has done. Exists so the idempotency claim can be checked
 * from outside: publish the same event twice, see one notification.
 */
@RestController
@RequestMapping("/notifications")
class NotificationQueryController {

    private final NotificationRepository notifications;

    NotificationQueryController(NotificationRepository notifications) {
        this.notifications = notifications;
    }

    @GetMapping("/count")
    long count() {
        return notifications.count();
    }

    @GetMapping("/by-transfer/{transferId}")
    List<Notification> byTransfer(@PathVariable UUID transferId) {
        return notifications.findByTransfer(transferId);
    }
}
