<?php

declare(strict_types=1);

namespace LiteSocket\Messaging\PubSub;

use Throwable;

/**
 * In-memory single-process Pub/Sub driver.
 * Zero external dependencies.
 */
class LocalPubSub implements PubSubInterface
{
    /** @var array<string, array<int, callable>> channel => [id => callable] */
    private array $subscribers = [];

    /** @var array<int, string> id => channel */
    private array $subMap = [];

    private int $nextSubId = 1;

    public function publish(string $channel, mixed $message): void
    {
        if (empty($this->subscribers[$channel])) {
            return;
        }

        foreach ($this->subscribers[$channel] as $handler) {
            try {
                $handler($channel, $message);
            } catch (Throwable $e) {
                echo sprintf("[LiteSocket LocalPubSub Error] %s\n", $e->getMessage());
            }
        }
    }

    public function subscribe(string $channel, callable $handler): int
    {
        $id = $this->nextSubId++;
        $this->subscribers[$channel][$id] = $handler;
        $this->subMap[$id] = $channel;
        return $id;
    }

    public function unsubscribe(int $subscriptionId): void
    {
        if (isset($this->subMap[$subscriptionId])) {
            $channel = $this->subMap[$subscriptionId];
            unset($this->subscribers[$channel][$subscriptionId], $this->subMap[$subscriptionId]);

            if (empty($this->subscribers[$channel])) {
                unset($this->subscribers[$channel]);
            }
        }
    }
}
