<?php

declare(strict_types=1);

namespace LiteSocket\Runtime;

/**
 * Contract for LiteSocket Event Loop.
 * Handles non-blocking stream I/O and independent timers.
 */
interface EventLoopInterface
{
    /**
     * Add a stream socket to the read notification set.
     *
     * @param resource $socket
     * @param callable $callback fn($socket)
     */
    public function addRead($socket, callable $callback): void;

    /**
     * Remove a stream socket from the read notification set.
     *
     * @param resource $socket
     */
    public function removeRead($socket): void;

    /**
     * Add a stream socket to the write notification set.
     *
     * @param resource $socket
     * @param callable $callback fn($socket)
     */
    public function addWrite($socket, callable $callback): void;

    /**
     * Remove a stream socket from the write notification set.
     *
     * @param resource $socket
     */
    public function removeWrite($socket): void;

    /**
     * Schedule a one-off timer to execute after specified seconds.
     *
     * @param float $delay Delay in seconds (e.g. 0.5, 1.0)
     * @param callable $callback fn()
     * @return int Timer identifier
     */
    public function after(float $delay, callable $callback): int;

    /**
     * Schedule a recurring timer to execute every specified interval.
     *
     * @param float $interval Interval in seconds (e.g. 0.05 for 20 TPS)
     * @param callable $callback fn()
     * @return int Timer identifier
     */
    public function every(float $interval, callable $callback): int;

    /**
     * Cancel an active timer by its ID.
     */
    public function cancelTimer(int $timerId): void;

    /**
     * Start the event loop (blocking until stopped).
     */
    public function run(): void;

    /**
     * Stop the event loop.
     */
    public function stop(): void;

    /**
     * Check if the event loop is currently running.
     */
    public function isRunning(): bool;
}
