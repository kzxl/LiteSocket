<?php

declare(strict_types=1);

namespace LiteSocket;

use LiteSocket\Connection\ReadBuffer;
use LiteSocket\Connection\WriteQueue;

/**
 * Represents an active client WebSocket connection with memory-guarded buffers and metadata.
 */
class Connection
{
    public const STATE_CONNECTING = 0;
    public const STATE_OPEN       = 1;
    public const STATE_CLOSING    = 2;
    public const STATE_CLOSED     = 3;

    private string $id;
    /** @var resource */
    private $socket;
    private string $remoteAddress;
    private int $state = self::STATE_CONNECTING;

    private ReadBuffer $readBuffer;
    private WriteQueue $writeQueue;

    private array $headers = [];
    private array $queryParams = [];
    private array $attributes = [];
    /** @var array<string, bool> */
    private array $rooms = [];
    private float $lastActivityTime;

    /** @var callable|null fn(Connection $conn) */
    private $onWriteNeeded = null;

    /**
     * @param resource $socket
     */
    public function __construct(
        string $id,
        $socket,
        string $remoteAddress = '',
        int $maxReadBuffer = 2097152, // 2MB
        int $maxWriteBuffer = 4194304  // 4MB
    ) {
        $this->id = $id;
        $this->socket = $socket;
        $this->remoteAddress = $remoteAddress;
        $this->lastActivityTime = microtime(true);

        $this->readBuffer = new ReadBuffer($maxReadBuffer);
        $this->writeQueue = new WriteQueue($maxWriteBuffer);
    }

    public function getId(): string
    {
        return $this->id;
    }

    /**
     * Concise alias for getId()
     */
    public function id(): string
    {
        return $this->id;
    }

    /**
     * @return resource
     */
    public function getSocket()
    {
        return $this->socket;
    }

    public function getRemoteAddress(): string
    {
        return $this->remoteAddress;
    }

    /**
     * Concise alias for getRemoteAddress()
     */
    public function ip(): string
    {
        return $this->remoteAddress;
    }

    public function getState(): int
    {
        return $this->state;
    }

    public function setState(int $state): void
    {
        $this->state = $state;
    }

    public function isOpen(): bool
    {
        return $this->state === self::STATE_OPEN;
    }

    public function isHandshakeDone(): bool
    {
        return $this->state === self::STATE_OPEN;
    }

    public function setHandshakeDone(bool $done): void
    {
        $this->state = $done ? self::STATE_OPEN : self::STATE_CONNECTING;
    }

    // --- Read Buffer Operations ---

    public function appendBuffer(string $data): void
    {
        $this->readBuffer->append($data);
        $this->lastActivityTime = microtime(true);
    }

    public function getBuffer(): string
    {
        return $this->readBuffer->get();
    }

    public function consumeBuffer(int $bytes): void
    {
        $this->readBuffer->consume($bytes);
    }

    public function getReadBuffer(): ReadBuffer
    {
        return $this->readBuffer;
    }

    // --- Write Queue Operations ---

    public function getWriteQueue(): WriteQueue
    {
        return $this->writeQueue;
    }

    /**
     * Set callback triggered when connection needs write event monitoring.
     *
     * @param callable|null $callback fn(Connection $conn)
     */
    public function setOnWriteNeeded(?callable $callback): void
    {
        $this->onWriteNeeded = $callback;
    }

    /**
     * Flush pending bytes in WriteQueue to the stream socket.
     */
    public function flush(): int
    {
        return $this->writeQueue->flush($this->socket);
    }

    public function hasPendingWrites(): bool
    {
        return $this->writeQueue->hasPendingData();
    }

    // --- Headers & Query Parameters ---

    public function setHeaders(array $headers): void
    {
        $this->headers = $headers;
    }

    public function getHeaders(): array
    {
        return $this->headers;
    }

    public function getHeader(string $name): ?string
    {
        return $this->headers[strtolower($name)] ?? null;
    }

    public function setQueryParams(array $params): void
    {
        $this->queryParams = $params;
    }

    public function getQueryParams(): array
    {
        return $this->queryParams;
    }

    public function getQueryParam(string $name, ?string $default = null): ?string
    {
        return $this->queryParams[$name] ?? $default;
    }

    // --- Metadata / Attributes ---

    public function set(string $key, mixed $value): void
    {
        $this->attributes[$key] = $value;
    }

    public function get(string $key, mixed $default = null): mixed
    {
        return $this->attributes[$key] ?? $default;
    }

    public function has(string $key): bool
    {
        return array_key_exists($key, $this->attributes);
    }

    public function getAttributes(): array
    {
        return $this->attributes;
    }

    public function attributes(): array
    {
        return $this->attributes;
    }

    /**
     * Quick helper for userId attribute.
     */
    public function userId(?string $default = null): ?string
    {
        $val = $this->get('userId');
        return $val !== null ? (string)$val : $default;
    }

    // --- Room Membership ---

    public function join(string $room): void
    {
        $this->rooms[$room] = true;
    }

    public function leave(string $room): void
    {
        unset($this->rooms[$room]);
    }

    public function isInRoom(string $room): bool
    {
        return isset($this->rooms[$room]);
    }

    /**
     * @return string[]
     */
    public function getRooms(): array
    {
        return array_keys($this->rooms);
    }

    public function rooms(): array
    {
        return array_keys($this->rooms);
    }

    public function getLastActivityTime(): float
    {
        return $this->lastActivityTime;
    }

    public function touch(): void
    {
        $this->lastActivityTime = microtime(true);
    }

    // --- Messaging Methods ---

    /**
     * Send a raw text message (auto-encoded into RFC 6455 text frame).
     * Non-blocking with WriteQueue buffering and backpressure.
     */
    public function send(string $text, bool $droppable = false): bool
    {
        return $this->sendText($text, $droppable);
    }

    /**
     * Send an RFC 6455 Text Frame.
     */
    public function sendText(string $text, bool $droppable = false): bool
    {
        $frame = Frame::encodeText($text);
        return $this->writeRaw($frame, $droppable);
    }

    /**
     * Send an RFC 6455 Binary Frame.
     */
    public function sendBinary(string $data, bool $droppable = false): bool
    {
        $frame = Frame::encodeBinary($data);
        return $this->writeRaw($frame, $droppable);
    }

    /**
     * Send a JSON-serializable payload.
     */
    public function sendJson(mixed $data, bool $droppable = false): bool
    {
        $json = json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if ($json === false) {
            return false;
        }
        return $this->sendText($json, $droppable);
    }

    /**
     * Send an RFC 6455 Ping frame.
     */
    public function ping(string $payload = ''): bool
    {
        $frame = Frame::encodePing($payload);
        return $this->writeRaw($frame, false);
    }

    /**
     * Enqueue raw frame bytes to WriteQueue and trigger write notification.
     */
    public function writeRaw(string $frameBytes, bool $droppable = false): bool
    {
        if (!is_resource($this->socket) || $this->state === self::STATE_CLOSED) {
            return false;
        }

        // 1. If queue is empty, attempt immediate write optimization
        if (!$this->writeQueue->hasPendingData()) {
            $written = @fwrite($this->socket, $frameBytes);
            if ($written === strlen($frameBytes)) {
                return true; // Fast-path: 100% written synchronously
            }

            if ($written !== false && $written > 0) {
                // Partial write: enqueue remainder
                $frameBytes = substr($frameBytes, $written);
            }
        }

        // 2. Enqueue remaining bytes into non-blocking WriteQueue
        $ok = $this->writeQueue->enqueue($frameBytes, $droppable);
        if ($ok && $this->onWriteNeeded !== null) {
            ($this->onWriteNeeded)($this);
        }

        return $ok;
    }

    /**
     * Close connection with RFC 6455 status code.
     */
    public function close(int $code = 1000, string $reason = ''): void
    {
        if ($this->state === self::STATE_CLOSED) {
            return;
        }

        $this->state = self::STATE_CLOSING;

        if (is_resource($this->socket)) {
            $frame = Frame::encodeClose($code, $reason);
            @fwrite($this->socket, $frame);
            @fclose($this->socket);
        }

        $this->state = self::STATE_CLOSED;
        $this->readBuffer->clear();
        $this->writeQueue->clear();
    }
}
