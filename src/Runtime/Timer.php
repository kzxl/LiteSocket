<?php

declare(strict_types=1);

namespace LiteSocket\Runtime;

/**
 * Encapsulates an active timer instance within the event loop.
 */
class Timer
{
    private int $id;
    private float $executeAt;
    private ?float $interval;
    /** @var callable */
    private $callback;
    private bool $cancelled = false;

    public function __construct(int $id, float $executeAt, ?float $interval, callable $callback)
    {
        $this->id = $id;
        $this->executeAt = $executeAt;
        $this->interval = $interval;
        $this->callback = $callback;
    }

    public function getId(): int
    {
        return $this->id;
    }

    public function getExecuteAt(): float
    {
        return $this->executeAt;
    }

    public function setExecuteAt(float $time): void
    {
        $this->executeAt = $time;
    }

    public function getInterval(): ?float
    {
        return $this->interval;
    }

    public function isRecurring(): bool
    {
        return $this->interval !== null;
    }

    public function isCancelled(): bool
    {
        return $this->cancelled;
    }

    public function cancel(): void
    {
        $this->cancelled = true;
    }

    public function execute(): void
    {
        if (!$this->cancelled) {
            ($this->callback)();
        }
    }
}
