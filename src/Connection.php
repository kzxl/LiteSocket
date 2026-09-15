<?php

declare(strict_types=1);

namespace LiteSocket;

/**
 * Represents an active client WebSocket connection.
 */
class Connection
{
    private string $id;
    /** @var resource */
    private $socket;
    private string $remoteAddress;
    private bool $handshakeDone = false;
    private string $readBuffer = '';
    private array $headers = [];
    private array $queryParams = [];
    private array $attributes = [];
    /** @var array<string, bool> */
    private array $rooms = [];
    private float $lastActivityTime;

    /**
     * @param resource $socket
     */
    public function __construct(string $id, $socket, string $remoteAddress = '')
    {
        $this->id = $id;
        $this->socket = $socket;
        $this->remoteAddress = $remoteAddress;
        $this->lastActivityTime = microtime(true);
    }

    public function getId(): string
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

    public function isHandshakeDone(): bool
    {
        return $this->handshakeDone;
    }

    public function setHandshakeDone(bool $done): void
    {
        $this->handshakeDone = $done;
    }

    public function appendBuffer(string $data): void
    {
        $this->readBuffer .= $data;
        $this->lastActivityTime = microtime(true);
    }

    public function getBuffer(): string
    {
        return $this->readBuffer;
    }

    public function consumeBuffer(int $bytes): void
    {
        $this->readBuffer = substr($this->readBuffer, $bytes);
    }

    public function setHeaders(array $headers): void
    {
        $this->headers = $headers;
    }

    public function getHeader(string $name): ?string
    {
        return $this->headers[strtolower($name)] ?? null;
    }

    public function setQueryParams(array $params): void
    {
        $this->queryParams = $params;
    }

    public function getQueryParam(string $name, ?string $default = null): ?string
    {
        return $this->queryParams[$name] ?? $default;
    }

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

    /**
     * Join a room.
     */
    public function join(string $room): void
    {
        $this->rooms[$room] = true;
    }

    /**
     * Leave a room.
     */
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

    public function getLastActivityTime(): float
    {
        return $this->lastActivityTime;
    }

    /**
     * Send a text message to this client.
     */
    public function send(string $text): bool
    {
        if (!is_resource($this->socket)) {
            return false;
        }

        $frame = Frame::encodeText($text);
        $length = strlen($frame);
        $written = @fwrite($this->socket, $frame);

        return $written === $length;
    }

    /**
     * Send a JSON-serializable message.
     */
    public function sendJson(mixed $data): bool
    {
        $json = json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if ($json === false) {
            return false;
        }
        return $this->send($json);
    }

    /**
     * Send a Ping to the client.
     */
    public function ping(): bool
    {
        if (!is_resource($this->socket)) {
            return false;
        }
        $frame = Frame::encodePing();
        return @fwrite($this->socket, $frame) !== false;
    }

    /**
     * Close connection with status code.
     */
    public function close(int $code = 1000, string $reason = ''): void
    {
        if (is_resource($this->socket)) {
            $frame = Frame::encodeClose($code, $reason);
            @fwrite($this->socket, $frame);
            @fclose($this->socket);
        }
    }
}
