<?php

declare(strict_types=1);

namespace LiteSocket\Messaging\PubSub;

/**
 * Pub/Sub abstraction contract.
 * Allows transparent scaling from single-process in-memory to multi-worker Redis/NATS pub-sub.
 */
interface PubSubInterface
{
    /**
     * Publish a message to a topic/channel.
     */
    public function publish(string $channel, mixed $message): void;

    /**
     * Subscribe to a topic/channel.
     *
     * @param string $channel
     * @param callable $handler fn(string $channel, mixed $message)
     * @return int Subscription identifier
     */
    public function subscribe(string $channel, callable $handler): int;

    /**
     * Unsubscribe by subscription ID.
     */
    public function unsubscribe(int $subscriptionId): void;
}
