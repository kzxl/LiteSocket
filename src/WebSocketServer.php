<?php

declare(strict_types=1);

namespace LiteSocket;

use LiteSocket\Connection\ConnectionPool;
use LiteSocket\Messaging\ClusterBridge;
use LiteSocket\Messaging\PubSub\LocalPubSub;
use LiteSocket\Messaging\PubSub\PubSubInterface;
use LiteSocket\Messaging\PubSub\RedisPubSub;
use LiteSocket\Messaging\RoomManager;
use LiteSocket\Messaging\Router;
use LiteSocket\Protocol\WebSocket\Frame;
use LiteSocket\Protocol\WebSocket\FrameParser;
use LiteSocket\Protocol\WebSocket\Handshake;
use LiteSocket\Runtime\EventLoopInterface;
use LiteSocket\Runtime\StreamSelectLoop;
use LiteSocket\Transport\StreamSocketTransport;
use LiteSocket\Transport\TransportInterface;
use OverflowException;
use Throwable;

/**
 * Sovereign, Zero-Dependency RFC 6455 WebSocket Server 2.x for PHP 8.2+.
 *
 * Coordinates 5 Modular Cores:
 * - Runtime:    Non-blocking EventLoop with independent timer queue
 * - Transport:  Non-blocking TCP server transport
 * - Protocol:   RFC 6455 Handshake & Streaming Frame Parser
 * - Connection: Memory-guarded ReadBuffer & Non-blocking WriteQueue with Backpressure
 * - Messaging:  Room Pub/Sub, Router dispatcher, and PubSub adapters
 */
class WebSocketServer
{
    private string $host;
    private int $port;
    private array $options;

    // 5 Modular Cores
    private TransportInterface $transport;
    private EventLoopInterface $loop;
    private ConnectionPool $pool;
    private FrameParser $frameParser;
    private RoomManager $rooms;
    private Router $router;
    private PubSubInterface $pubsub;
    private ?ClusterBridge $clusterBridge = null;

    /** @var array<string, callable[]> */
    private array $eventHandlers = [
        'connect' => [],
        'message' => [],
        'close'   => [],
        'error'   => [],
        'tick'    => [],
    ];

    private int $nextConnectionId = 1;
    private float $idleTimeout = 120.0;
    private bool $running = false;

    public function __construct(
        string $host = '0.0.0.0',
        int $port = 8088,
        array $options = [],
        ?EventLoopInterface $loop = null,
        ?TransportInterface $transport = null
    ) {
        $this->host = $host;
        $this->port = $port;
        $this->options = array_merge([
            'maxReadBuffer'  => 2097152,  // 2MB
            'maxWriteBuffer' => 4194304,  // 4MB
            'maxFrameSize'   => 2097152,  // 2MB
            'idleTimeout'    => 120.0,    // 120s
            'allowedOrigins' => null,     // null = allow all
            'so_reuseport'   => true,
            'backlog'        => 1024,
        ], $options);

        $this->idleTimeout = (float)$this->options['idleTimeout'];

        // Instantiate modular cores
        $this->loop = $loop ?? new StreamSelectLoop();
        $this->transport = $transport ?? new StreamSocketTransport();
        $this->pool = new ConnectionPool();
        $this->frameParser = new FrameParser((int)$this->options['maxFrameSize']);
        $this->rooms = new RoomManager();
        $this->router = new Router();
        $this->pubsub = new LocalPubSub();
    }

    // --- Accessors for 5 Cores ---

    public function getLoop(): EventLoopInterface
    {
        return $this->loop;
    }

    public function getTransport(): TransportInterface
    {
        return $this->transport;
    }

    public function getRoomManager(): RoomManager
    {
        return $this->rooms;
    }

    public function getRouter(): Router
    {
        return $this->router;
    }

    public function getPubSub(): PubSubInterface
    {
        return $this->pubsub;
    }

    public function setPubSub(PubSubInterface $pubsub): self
    {
        $this->pubsub = $pubsub;
        if ($pubsub instanceof RedisPubSub) {
            $pubsub->attachLoop($this->loop);
        }
        return $this;
    }

    /**
     * Enable multi-node cluster scaling via Pub/Sub (e.g. Redis).
     */
    public function enableCluster(?PubSubInterface $pubsub = null, ?string $nodeId = null, string $channelPrefix = 'litesocket:cluster:room:'): self
    {
        if ($pubsub !== null) {
            $this->setPubSub($pubsub);
        }

        $this->clusterBridge = new ClusterBridge($this, $this->pubsub, $nodeId, $channelPrefix);
        return $this;
    }

    public function getClusterBridge(): ?ClusterBridge
    {
        return $this->clusterBridge;
    }

    public function isClusterEnabled(): bool
    {
        return $this->clusterBridge !== null;
    }

    /**
     * Join a connection to a room and ensure cluster-wide subscription.
     */
    public function joinRoom(string $room, Connection $conn): self
    {
        $this->rooms->join($room, $conn);
        if ($this->clusterBridge !== null) {
            $this->clusterBridge->subscribeRoom($room);
        }
        return $this;
    }

    /**
     * Remove a connection from a room and clean up cluster-wide subscription if empty.
     */
    public function leaveRoom(string $room, Connection $conn): self
    {
        $this->rooms->leave($room, $conn);
        if ($this->clusterBridge !== null && !$this->rooms->hasRoom($room)) {
            $this->clusterBridge->unsubscribeRoom($room);
        }
        return $this;
    }

    public function getConnectionPool(): ConnectionPool
    {
        return $this->pool;
    }

    /**
     * @return array<string, Connection>
     */
    public function getConnections(): array
    {
        return $this->pool->all();
    }

    public function getConnectionCount(): int
    {
        return $this->pool->count();
    }

    // --- Event & Route Registration ---

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
     * Register a message type route handler.
     *
     * @param string $type e.g. 'player_move', 'chat'
     * @param callable $handler fn(Connection $conn, mixed $payload)
     */
    public function route(string $type, callable $handler): self
    {
        $this->router->route($type, $handler);
        return $this;
    }

    // --- Broadcast APIs ---

    /**
     * Broadcast a message to all connected clients.
     */
    public function broadcastAll(mixed $message, ?Connection $exclude = null, bool $droppable = false): int
    {
        $payload = is_string($message) ? $message : json_encode($message, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        $excludeId = $exclude?->getId();
        $count = 0;

        foreach ($this->pool->all() as $connId => $conn) {
            if ($excludeId !== null && $connId === $excludeId) {
                continue;
            }
            if ($conn->isOpen() && $conn->sendText($payload, $droppable)) {
                $count++;
            }
        }

        return $count;
    }

    /**
     * Concise alias for broadcastAll
     */
    public function broadcast(mixed $message, ?Connection $exclude = null, bool $droppable = false): int
    {
        return $this->broadcastAll($message, $exclude, $droppable);
    }

    /**
     * Broadcast a message to all connections in a specific room.
     */
    public function broadcastToRoom(string $room, mixed $message, ?Connection $exclude = null, bool $droppable = false): int
    {
        if ($this->clusterBridge !== null) {
            return $this->clusterBridge->broadcast($room, $message, $exclude, $droppable);
        }
        return $this->rooms->broadcast($room, $message, $exclude, $droppable);
    }

    // --- Lifecycle & Server Loop ---

    /**
     * Start the server and enter the event loop.
     *
     * @param float|null $tickInterval Tick interval in seconds (e.g., 0.05 for 20 TPS).
     */
    public function run(?float $tickInterval = 0.05): void
    {
        // 1. Bind and listen TCP socket
        $this->transport->listen($this->host, $this->port, $this->options);
        $masterSocket = $this->transport->getMasterSocket();

        // 2. Register master socket in read set of EventLoop
        $this->loop->addRead($masterSocket, function ($socket) {
            $this->handleAccept();
        });

        // 3. Register Game Tick as an independent recurring timer (NO LONGER BOUND TO SELECT TIMEOUT)
        if ($tickInterval !== null && $tickInterval > 0) {
            $this->loop->every($tickInterval, function () use ($tickInterval) {
                $this->emit('tick', $tickInterval);
            });
        }

        // 4. Register periodic idle timeout & stale connection sweep
        if ($this->idleTimeout > 0) {
            $this->loop->every(10.0, function () {
                $this->sweepIdleConnections();
            });
        }

        $this->running = true;
        echo sprintf("[LiteSocket 2.x] Listening on ws://%s:%d (PID: %d)\n", $this->host, $this->port, getmypid());

        // 5. Run the event loop
        try {
            $this->loop->run();
        } finally {
            $this->shutdown();
        }
    }

    public function stop(): void
    {
        $this->running = false;
        $this->loop->stop();
    }

    // --- Internal Socket & Protocol Handlers ---

    private function handleAccept(): void
    {
        $accepted = $this->transport->accept();
        if (!$accepted) {
            return;
        }

        [$clientSocket, $peerName] = $accepted;
        $connId = 'c_' . ($this->nextConnectionId++);

        $conn = new Connection(
            $connId,
            $clientSocket,
            $peerName,
            (int)$this->options['maxReadBuffer'],
            (int)$this->options['maxWriteBuffer']
        );

        // When connection has pending writes that couldn't be flushed immediately, register in loop write set
        $conn->setOnWriteNeeded(function (Connection $c) {
            $sock = $c->getSocket();
            if (is_resource($sock)) {
                $this->loop->addWrite($sock, function ($s) use ($c) {
                    $this->handleClientWrite($c);
                });
            }
        });

        $this->pool->add($conn);

        // Register client socket for read notifications
        $this->loop->addRead($clientSocket, function ($socket) use ($conn) {
            $this->handleClientRead($conn);
        });
    }

    private function handleClientRead(Connection $conn): void
    {
        $socket = $conn->getSocket();
        if (!is_resource($socket)) {
            $this->disconnect($conn);
            return;
        }

        $data = @fread($socket, 8192);

        if ($data === false || $data === '') {
            $this->disconnect($conn);
            return;
        }

        try {
            $conn->appendBuffer($data);

            if (!$conn->isHandshakeDone()) {
                $this->handleHandshake($conn);
            } else {
                $this->handleFrames($conn);
            }
        } catch (OverflowException $e) {
            $this->handleError($e);
            $this->disconnect($conn);
        } catch (Throwable $e) {
            $this->handleError($e);
        }
    }

    private function handleClientWrite(Connection $conn): void
    {
        $socket = $conn->getSocket();
        if (!is_resource($socket)) {
            $this->disconnect($conn);
            return;
        }

        $conn->flush();

        // If all pending bytes have been flushed, remove socket from writable notification set
        if (!$conn->hasPendingWrites()) {
            $this->loop->removeWrite($socket);
        }
    }

    private function handleHandshake(Connection $conn): void
    {
        $allowedOrigins = $this->options['allowedOrigins'] ?? null;
        $done = Handshake::handle($conn, $allowedOrigins);

        if ($done) {
            // Auto-join room from query param if provided (e.g. ?room=CH-1_SanctuaryHaven)
            $autoRoom = $conn->getQueryParam('room');
            if ($autoRoom !== null && $autoRoom !== '') {
                $this->rooms->join($autoRoom, $conn);
            }

            $this->emit('connect', $conn);
        }
    }

    private function handleFrames(Connection $conn): void
    {
        $frames = $this->frameParser->parse($conn);

        foreach ($frames as $frame) {
            $opcode = $frame['opcode'];
            $payload = $frame['payload'];

            switch ($opcode) {
                case Frame::OPCODE_TEXT:
                    $this->processTextMessage($conn, $payload);
                    break;

                case Frame::OPCODE_BINARY:
                    $this->emit('message', $conn, $payload, null);
                    break;

                case Frame::OPCODE_PING:
                    $conn->writeRaw(Frame::encodePong($payload), false);
                    break;

                case Frame::OPCODE_PONG:
                    $conn->touch();
                    break;

                case Frame::OPCODE_CLOSE:
                    $this->disconnect($conn);
                    return;

                default:
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
            $type = (string)$json['type'];

            if ($type === 'subscribe' && !empty($json['room'])) {
                $this->rooms->join((string)$json['room'], $conn);
                $conn->sendJson(['type' => 'subscribed', 'room' => $json['room']]);
                return;
            }

            if ($type === 'unsubscribe' && !empty($json['room'])) {
                $this->rooms->leave((string)$json['room'], $conn);
                $conn->sendJson(['type' => 'unsubscribed', 'room' => $json['room']]);
                return;
            }

            if ($type === 'ping') {
                $conn->sendJson(['type' => 'pong', 'timestamp' => microtime(true)]);
                return;
            }

            // Route via Router dispatcher if a route is registered
            if ($this->router->has($type)) {
                $this->router->dispatch($conn, $type, $json);
            }
        }

        // Always emit 'message' event for backwards compatibility
        $this->emit('message', $conn, $message, $json);
    }

    public function disconnect(Connection $conn): void
    {
        $sock = $conn->getSocket();
        if (is_resource($sock)) {
            $this->loop->removeRead($sock);
            $this->loop->removeWrite($sock);
        }

        $this->rooms->leaveAll($conn);
        $this->emit('close', $conn);

        $this->pool->remove($conn);
        $conn->close();
    }

    private function sweepIdleConnections(): void
    {
        $now = microtime(true);
        foreach ($this->pool->all() as $conn) {
            if (($now - $conn->getLastActivityTime()) > $this->idleTimeout) {
                $conn->close(1000, 'Connection idle timeout');
                $this->disconnect($conn);
            }
        }
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
            }
        }
        echo sprintf("[LiteSocket Error] %s in %s:%d\n", $e->getMessage(), $e->getFile(), $e->getLine());
    }

    private function shutdown(): void
    {
        foreach ($this->pool->all() as $conn) {
            $this->disconnect($conn);
        }

        $this->transport->close();
        echo "[LiteSocket 2.x] Server stopped.\n";
    }
}
