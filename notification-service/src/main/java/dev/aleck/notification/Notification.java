package dev.aleck.notification;

import java.util.UUID;

record Notification(
        UUID id,
        UUID eventId,
        UUID transferId,
        UUID accountId,
        String channel,
        String message) {
}
