<?php

declare(strict_types=1);

namespace LiteSocket\Runtime;

use Throwable;

/**
 * Sovereign Event Loop implementation using PHP native stream_select and microtime timer queue.
 * Zero external extensions (no Swoole, no libevent required).
 */
class StreamSelectLoop implements EventLoopInterface
{
    /** @var array<int, resource> socketId => resource */
    private array $readSockets = [];

    /** @var array<int, callable> socketId => callback */
    private array $readCallbacks = [];

    /** @var array<int, resource> socketId => resource */
    private array $writeSockets = [];

    /** @var array<int, callable> socketId => callback */
    private array $writeCallbacks = [];

    /** @var array<int, Timer> timerId => Timer */
    private array $timers = [];

    private int $nextTimerId = 1;
    private bool $running = false;

    public function addRead($socket, callable $callback): void
    {
        $id = (int)$socket;
        $this->readSockets[$id] = $socket;
        $this->readCallbacks[$id] = $callback;
    }

    public function removeRead($socket): void
    {
        $id = (int)$socket;
        unset($this->readSockets[$id], $this->readCallbacks[$id]);
    }

    public function addWrite($socket, callable $callback): void
    {
        $id = (int)$socket;
        $this->writeSockets[$id] = $socket;
        $this->writeCallbacks[$id] = $callback;
    }

    public function removeWrite($socket): void
    {
        $id = (int)$socket;
        unset($this->writeSockets[$id], $this->writeCallbacks[$id]);
    }

    public function after(float $delay, callable $callback): int
    {
        $id = $this->nextTimerId++;
        $executeAt = microtime(true) + max(0.0, $delay);
        $this->timers[$id] = new Timer($id, $executeAt, null, $callback);
        return $id;
    }

    public function every(float $interval, callable $callback): int
    {
        $id = $this->nextTimerId++;
        $interval = max(0.001, $interval);
        $executeAt = microtime(true) + $interval;
        $this->timers[$id] = new Timer($id, $executeAt, $interval, $callback);
        return $id;
    }

    public function cancelTimer(int $timerId): void
    {
        if (isset($this->timers[$timerId])) {
            $this->timers[$timerId]->cancel();
            unset($this->timers[$timerId]);
        }
    }

    public function isRunning(): bool
    {
        return $this->running;
    }

    public function stop(): void
    {
        $this->running = false;
    }

    public function run(): void
    {
        $this->running = true;

        while ($this->running) {
            $read = array_values($this->readSockets);
            $write = array_values($this->writeSockets);
            $except = null;

            // If there's nothing to monitor and no timers, loop is idle
            if (empty($read) && empty($write) && empty($this->timers)) {
                usleep(50000);
                continue;
            }

            // Calculate dynamic select timeout based on nearest timer
            $timeoutSec = null;
            $timeoutUsec = null;

            if (!empty($this->timers)) {
                $now = microtime(true);
                $minExecuteAt = null;
                foreach ($this->timers as $timer) {
                    if ($timer->isCancelled()) {
                        continue;
                    }
                    $executeAt = $timer->getExecuteAt();
                    if ($minExecuteAt === null || $executeAt < $minExecuteAt) {
                        $minExecuteAt = $executeAt;
                    }
                }

                if ($minExecuteAt !== null) {
                    $diff = max(0.0, $minExecuteAt - $now);
                    $timeoutSec = (int)$diff;
                    $timeoutUsec = (int)(($diff - $timeoutSec) * 1_000_000);
                }
            } else {
                // Default maximum sleep of 1 second when no timer is scheduled (allows signal handling)
                $timeoutSec = 1;
                $timeoutUsec = 0;
            }

            // Perform non-blocking multiplexed I/O select
            $numChanged = 0;
            if (!empty($read) || !empty($write)) {
                $numChanged = @stream_select($read, $write, $except, $timeoutSec, $timeoutUsec);
            } elseif ($timeoutSec !== null || $timeoutUsec !== null) {
                // Sockets empty but timers exist: sleep until nearest timer
                $sleepUsec = ($timeoutSec ?? 0) * 1_000_000 + ($timeoutUsec ?? 0);
                if ($sleepUsec > 0) {
                    usleep(min($sleepUsec, 1000000));
                }
            }

            if ($numChanged === false) {
                // Interrupted by system signal or error, continue loop
                $this->processTimers();
                continue;
            }

            // 1. Process readable sockets
            if (!empty($read)) {
                foreach ($read as $sock) {
                    $id = (int)$sock;
                    if (isset($this->readCallbacks[$id])) {
                        try {
                            ($this->readCallbacks[$id])($sock);
                        } catch (Throwable $e) {
                            $this->onUncaughtException($e);
                        }
                    }
                }
            }

            // 2. Process writable sockets
            if (!empty($write)) {
                foreach ($write as $sock) {
                    $id = (int)$sock;
                    if (isset($this->writeCallbacks[$id])) {
                        try {
                            ($this->writeCallbacks[$id])($sock);
                        } catch (Throwable $e) {
                            $this->onUncaughtException($e);
                        }
                    }
                }
            }

            // 3. Process expired timers
            $this->processTimers();
        }
    }

    private function processTimers(): void
    {
        if (empty($this->timers)) {
            return;
        }

        $now = microtime(true);
        $expired = [];

        foreach ($this->timers as $id => $timer) {
            if ($timer->isCancelled()) {
                unset($this->timers[$id]);
                continue;
            }
            if ($now >= $timer->getExecuteAt()) {
                $expired[] = $timer;
            }
        }

        foreach ($expired as $timer) {
            if ($timer->isCancelled()) {
                continue;
            }

            try {
                $timer->execute();
            } catch (Throwable $e) {
                $this->onUncaughtException($e);
            }

            if ($timer->isRecurring() && !$timer->isCancelled()) {
                $timer->setExecuteAt(microtime(true) + (float)$timer->getInterval());
            } else {
                unset($this->timers[$timer->getId()]);
            }
        }
    }

    private function onUncaughtException(Throwable $e): void
    {
        echo sprintf("[LiteSocket EventLoop Error] %s in %s:%d\n", $e->getMessage(), $e->getFile(), $e->getLine());
    }
}
