<?php

declare(strict_types=1);

namespace LiteSocket;

use Throwable;

/**
 * Sovereign, Zero-Dependency RFC 6455 WebSocket Server for PHP 8.2+.
 * Uses non-blocking stream_socket_server & stream_select event loop.
 */
class WebSocketServer
{
    private string $host;
    private int $port;
    /** @var resource|null */
    private $masterSocket = null;
    private bool $running = false;

    /** @var array<string, Connection> connId => Connection */
    private array $connections = [];

    private RoomManager $rooms;

    /** @var array<string, callable[]> */
    private array $eventHandlers = [
        'connect' => [],
        'message' => [],
        'close'   => [],
        'error'   => [],
        'tick'    => [],
    ];

    private int $nextConnectionId = 1;
    private float $idleTimeout = 120.0; // 2 minutes heartbeat timeout

    public function __construct(string $host = '0.0.0.0', int $port = 8088)
    {
        $this->host = $host;
        $this->port = $port;
        $this->rooms = new RoomManager();
    }

    public function getRoomManager(): RoomManager
    {
        return $this->rooms;
    }

    /**
     * Register an event listener ('connect', 'message', 'close', 'error', 'tick').
     */
    public function on(string $event, callable $handler): self
    {
        if (isset($this->eventHandlers[$event])) {
            $this->eventHandlers[$event][] = $handler;
        }
        return $this;
    }

    /**
     * Start the server and enter the event loop.
     *
     * @param float|null $tickInterval Tick interval in seconds (e.g., 0.05 for 20 TPS).
     */
    public function run(?float $tickInterval = 0.05): void
    {
        $address = "tcp://{$this->host}:{$this->port}";
        $context = stream_context_create([
            'socket' => [
                'so_reuseport' => 1,
                'backlog'      => 1024,
            ],
        ]);

        $errno = 0;
        $errstr = '';
        $this->masterSocket = @stream_socket_server($address, $errno, $errstr, STREAM_SERVER_BIND | STREAM_SERVER_LISTEN, $context);

        if (!$this->masterSocket) {
            throw new \RuntimeException("LiteSocket failed to bind on {$address}: [{$errno}] {$errstr}");
        }

        stream_set_blocking($this->masterSocket, false);
        $this->running = true;

        echo sprintf("[LiteSocket] Listening on ws://%s:%d (PID: %d)\n", $this->host, $this->port, getmypid());

        $lastTick = microtime(true);

        while ($this->running) {
            $read = [$this->masterSocket];
            $write = null;
            $except = null;

            foreach ($this->connections as $conn) {
                $sock = $conn->getSocket();
                if (is_resource($sock)) {
                    $read[] = $sock;
                }
            }

            // Timeout in microseconds: 20ms (0.02s) to allow responsive ticks
            $timeoutSec = 0;
            $timeoutUsec = 20000;
            $numChanged = @stream_select($read, $write, $except, $timeoutSec, $timeoutUsec);

            if ($numChanged === false) {
                // Interrupted by signal or error
                continue;
            }

            // 1. Check for incoming new connections
            if (in_array($this->masterSocket, $read, true)) {
                $newSocket = @stream_socket_accept($this->masterSocket, 0, $peerName);
                if ($newSocket) {
                    stream_set_blocking($newSocket, false);
                    $connId = 'c_' . ($this->nextConnectionId++);
                    $conn = new Connection($connId, $newSocket, (string)$peerName);
                    $this->connections[$connId] = $conn;
                }
                // Remove master from read list
                $key = array_search($this->masterSocket, $read, true);
                if ($key !== false) {
                    unset($read[$key]);
                }
            }

            // 2. Process data from clients
            foreach ($read as $socket) {
                $conn = $this->findConnectionBySocket($socket);
                if (!$conn) {
                    continue;
                }

                $data = @fread($socket, 8192);

                if ($data === false || $data === '') {
                    // Socket closed by peer or empty read
                    $this->disconnect($conn);
                    continue;
                }

                $conn->appendBuffer($data);

                if (!$conn->isHandshakeDone()) {
                    $this->handleHandshake($conn);
                } else {
                    $this->handleFrames($conn);
                }
            }

            // 3. Tick Loop for authoritative physics/logic (e.g. 20 TPS)
            $now = microtime(true);
            if ($tickInterval !== null && ($now - $lastTick) >= $tickInterval) {
                $delta = $now - $lastTick;
                $lastTick = $now;
                $this->emit('tick', $delta);
            }
        }

        $this->shutdown();
    }

    public function stop(): void
    {
        $this->running = false;
    }

    public function broadcastAll(mixed $message, ?Connection $exclude = null): int
    {
        $payload = is_string($message) ? $message : json_encode($message, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        $excludeId = $exclude?->getId();
        $count = 0;

        foreach ($this->connections as $connId => $conn) {
            if ($excludeId !== null && $connId === $excludeId) {
                continue;
            }
            if ($conn->isHandshakeDone() && $conn->send($payload)) {
                $count++;
            }
        }

        return $count;
    }

    public function broadcastToRoom(string $room, mixed $message, ?Connection $exclude = null): int
    {
        return $this->rooms->broadcast($room, $message, $exclude);
    }

    public function getConnectionCount(): int
    {
        return count($this->connections);
    }

    /**
     * @return array<string, Connection>
     */
    public function getConnections(): array
    {
        return $this->connections;
    }

    // --- Internal Handlers ---

    private function handleHandshake(Connection $conn): void
    {
        $buffer = $conn->getBuffer();
        $headerEnd = strpos($buffer, "\r\n\r\n");

        if ($headerEnd === false) {
            // Incomplete HTTP request, wait for more data
            if (strlen($buffer) > 4096) {
                $conn->close(1002, 'Handshake headers too large');
                $this->disconnect($conn);
            }
            return;
        }

        $rawHeaders = substr($buffer, 0, $headerEnd);
        $conn->consumeBuffer($headerEnd + 4);

        $lines = explode("\r\n", $rawHeaders);
        $requestLine = array_shift($lines);
        $parts = explode(' ', $requestLine);

        if (count($parts) < 2 || strtoupper($parts[0]) !== 'GET') {
            $conn->close(1002, 'Invalid HTTP request');
            $this->disconnect($conn);
            return;
        }

        $urlParts = parse_url($parts[1]);
        $path = $urlParts['path'] ?? '/';
        $query = [];
        if (!empty($urlParts['query'])) {
            parse_str($urlParts['query'], $query);
        }
        $conn->setQueryParams($query);

        $headers = [];
        foreach ($lines as $line) {
            $colon = strpos($line, ':');
            if ($colon !== false) {
                $key = strtolower(trim(substr($line, 0, $colon)));
                $val = trim(substr($line, $colon + 1));
                $headers[$key] = $val;
            }
        }
        $conn->setHeaders($headers);

        $secKey = $headers['sec-websocket-key'] ?? null;
        if (!$secKey) {
            $conn->close(1002, 'Missing Sec-WebSocket-Key');
            $this->disconnect($conn);
            return;
        }

        // Calculate Sec-WebSocket-Accept token RFC 6455
        $magic = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
        $acceptKey = base64_encode(sha1($secKey . $magic, true));

        $response = "HTTP/1.1 101 Switching Protocols\r\n"
            . "Upgrade: websocket\r\n"
            . "Connection: Upgrade\r\n"
            . "Sec-WebSocket-Accept: {$acceptKey}\r\n"
            . "Server: LiteSocket/1.0\r\n\r\n";

        @fwrite($conn->getSocket(), $response);
        $conn->setHandshakeDone(true);

        // Auto-join room from query param if provided (e.g. ?room=CH-1_SanctuaryHaven)
        $autoRoom = $conn->getQueryParam('room');
        if ($autoRoom !== null && $autoRoom !== '') {
            $this->rooms->join($autoRoom, $conn);
        }

        $this->emit('connect', $conn);
    }

    private function handleFrames(Connection $conn): void
    {
        while (strlen($conn->getBuffer()) > 0) {
            $frame = Frame::decode($conn->getBuffer());
            if ($frame === null) {
                // Incomplete frame, wait for more bytes
                break;
            }

            $conn->consumeBuffer($frame['bytesConsumed']);

            $opcode = $frame['opcode'];
            $payload = $frame['payload'];

            switch ($opcode) {
                case Frame::OPCODE_TEXT:
                    $this->processTextMessage($conn, $payload);
                    break;

                case Frame::OPCODE_PING:
                    // Respond with Pong
                    $conn->send(Frame::encodePong($payload));
                    break;

                case Frame::OPCODE_PONG:
                    // Keepalive acknowledged
                    break;

                case Frame::OPCODE_CLOSE:
                    $this->disconnect($conn);
                    return;

                default:
                    // Ignore unsupported opcodes
                    break;
            }
        }
    }

    private function processTextMessage(Connection $conn, string $message): void
    {
        $json = null;
        if (str_starts_with($message, '{') || str_starts_with($message, '[')) {
            $json = json_decode($message, true);
        }

        // Handle native protocol events (subscribe, unsubscribe, ping)
        if (is_array($json) && isset($json['type'])) {
            $type = $json['type'];
            if ($type === 'subscribe' && !empty($json['room'])) {
                $this->rooms->join((string)$json['room'], $conn);
                $conn->sendJson(['type' => 'subscribed', 'room' => $json['room']]);
                return;
            } elseif ($type === 'unsubscribe' && !empty($json['room'])) {
                $this->rooms->leave((string)$json['room'], $conn);
                $conn->sendJson(['type' => 'unsubscribed', 'room' => $json['room']]);
                return;
            } elseif ($type === 'ping') {
                $conn->sendJson(['type' => 'pong', 'timestamp' => microtime(true)]);
                return;
            }
        }

        $this->emit('message', $conn, $message, $json);
    }

    private function disconnect(Connection $conn): void
    {
        $connId = $conn->getId();
        if (!isset($this->connections[$connId])) {
            return;
        }

        $this->rooms->leaveAll($conn);
        $this->emit('close', $conn);

        unset($this->connections[$connId]);
        $conn->close();
    }

    private function findConnectionBySocket($socket): ?Connection
    {
        foreach ($this->connections as $conn) {
            if ($conn->getSocket() === $socket) {
                return $conn;
            }
        }
        return null;
    }

    private function emit(string $event, ...$args): void
    {
        foreach ($this->eventHandlers[$event] ?? [] as $handler) {
            try {
                $handler(...$args);
            } catch (Throwable $e) {
                $this->handleError($e);
            }
        }
    }

    private function handleError(Throwable $e): void
    {
        foreach ($this->eventHandlers['error'] ?? [] as $handler) {
            try {
                $handler($e);
            } catch (Throwable) {
                // Prevent infinite loop
            }
        }
        echo sprintf("[LiteSocket Error] %s in %s:%d\n", $e->getMessage(), $e->getFile(), $e->getLine());
    }

    private function shutdown(): void
    {
        foreach ($this->connections as $conn) {
            $this->disconnect($conn);
        }
        if (is_resource($this->masterSocket)) {
            @fclose($this->masterSocket);
        }
        echo "[LiteSocket] Server stopped.\n";
    }
}
