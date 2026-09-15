<?php

declare(strict_types=1);

namespace LiteSocket\Sse;

/**
 * Server-Sent Events (SSE) Streamer for HTTP/Shared Hosting environments.
 * Provides unidirectional real-time event streaming over standard HTTP.
 */
class SseStreamer
{
    private int $retryMs = 3000;

    public function __construct(int $retryMs = 3000)
    {
        $this->retryMs = $retryMs;
    }

    /**
     * Send initial SSE response headers.
     */
    public function initHeaders(): void
    {
        if (headers_sent()) {
            return;
        }

        header('Content-Type: text/event-stream');
        header('Cache-Control: no-cache, no-transform');
        header('Connection: keep-alive');
        header('X-Accel-Buffering: no'); // Disable Nginx proxy buffering

        // Tell client reconnection delay
        echo "retry: {$this->retryMs}\n\n";
        $this->flush();
    }

    /**
     * Emit an SSE event to the client.
     */
    public function send(mixed $data, ?string $event = null, ?string $id = null): void
    {
        if ($id !== null) {
            echo "id: {$id}\n";
        }

        if ($event !== null) {
            echo "event: {$event}\n";
        }

        $payload = is_string($data) ? $data : json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);

        // Multi-line data handling per SSE spec
        $lines = explode("\n", str_replace(["\r\n", "\r"], "\n", $payload));
        foreach ($lines as $line) {
            echo "data: {$line}\n";
        }

        echo "\n";
        $this->flush();
    }

    /**
     * Send a comment (used for keep-alive heartbeats).
     */
    public function ping(): void
    {
        echo ": keep-alive\n\n";
        $this->flush();
    }

    /**
     * Flush all PHP and server output buffers.
     */
    public function flush(): void
    {
        if (ob_get_level() > 0) {
            @ob_flush();
        }
        @flush();
    }

    /**
     * Run an SSE stream loop until client disconnects or timeout.
     *
     * @param callable $fetchDataFn Callback fn(): ?array returning payload or null
     * @param float $sleepSec Polling sleep interval in seconds
     * @param int $maxExecutionSec Maximum loop time before gracefully ending HTTP connection
     */
    public function stream(callable $fetchDataFn, float $sleepSec = 1.0, int $maxExecutionSec = 25): void
    {
        $this->initHeaders();

        $startTime = time();
        $usleepTime = (int)($sleepSec * 1000000);

        while (!connection_aborted() && (time() - $startTime) < $maxExecutionSec) {
            $data = $fetchDataFn();
            if ($data !== null) {
                $this->send($data);
            } else {
                $this->ping();
            }

            usleep($usleepTime);
        }
    }
}
