<?php

declare(strict_types=1);

namespace LiteSocket\Messaging\PubSub;

use LiteSocket\Runtime\EventLoopInterface;
use RuntimeException;
use Throwable;

/**
 * Sovereign, Zero-Dependency Redis Pub/Sub Driver.
 * Pure PHP 8.2 stream socket client implementing RESP protocol.
 * Zero ext-redis or Composer dependencies.
 */
class RedisPubSub implements PubSubInterface
{
    private string $host;
    private int $port;
    private ?string $password;
    private int $database;
    private float $timeout;
    private ?EventLoopInterface $loop;

    /** @var resource|null Dedicated socket for PUBLISH commands */
    private $pubSocket = null;

    /** @var resource|null Dedicated non-blocking socket for SUBSCRIBE mode */
    private $subSocket = null;

    private string $readBuffer = '';

    /** @var array<string, array<int, callable>> channel => [id => callable] */
    private array $subscribers = [];

    /** @var array<int, string> id => channel */
    private array $subMap = [];

    /** @var array<string, bool> Channels registered on Redis */
    private array $activeChannels = [];

    private int $nextSubId = 1;
    private bool $subSocketInLoop = false;

    public function __construct(
        string $host = '127.0.0.1',
        int $port = 6379,
        ?string $password = null,
        int $database = 0,
        float $timeout = 2.0,
        ?EventLoopInterface $loop = null
    ) {
        $this->host = $host;
        $this->port = $port;
        $this->password = $password;
        $this->database = $database;
        $this->timeout = $timeout;
        $this->loop = $loop;
    }

    public function attachLoop(EventLoopInterface $loop): void
    {
        $this->loop = $loop;
        $this->registerSubSocketToLoop();
    }

    /**
     * Publish a message to a Redis channel.
     */
    public function publish(string $channel, mixed $message): void
    {
        $payload = is_string($message) ? $message : json_encode($message, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        $command = RespParser::encode(['PUBLISH', $channel, $payload]);

        $socket = $this->getPublisherSocket();
        $written = @fwrite($socket, $command);

        if ($written === false || $written < strlen($command)) {
            // Reconnect and retry once
            $this->closePublisherSocket();
            $socket = $this->getPublisherSocket();
            @fwrite($socket, $command);
        }

        // Consume publisher response (integer: count of receivers)
        $this->readPublisherResponse($socket);
    }

    /**
     * Subscribe to a Redis channel with a callback.
     */
    public function subscribe(string $channel, callable $handler): int
    {
        $id = $this->nextSubId++;
        $this->subscribers[$channel][$id] = $handler;
        $this->subMap[$id] = $channel;

        if (!isset($this->activeChannels[$channel])) {
            $socket = $this->getSubscriberSocket();
            $command = RespParser::encode(['SUBSCRIBE', $channel]);
            @fwrite($socket, $command);
            $this->activeChannels[$channel] = true;

            $this->registerSubSocketToLoop();
        }

        return $id;
    }

    /**
     * Unsubscribe by subscription ID.
     */
    public function unsubscribe(int $subscriptionId): void
    {
        if (!isset($this->subMap[$subscriptionId])) {
            return;
        }

        $channel = $this->subMap[$subscriptionId];
        unset($this->subscribers[$channel][$subscriptionId], $this->subMap[$subscriptionId]);

        if (empty($this->subscribers[$channel])) {
            unset($this->subscribers[$channel], $this->activeChannels[$channel]);

            if ($this->subSocket && is_resource($this->subSocket)) {
                $command = RespParser::encode(['UNSUBSCRIBE', $channel]);
                @fwrite($this->subSocket, $command);
            }
        }
    }

    /**
     * Process available incoming bytes from subscriber socket (called by EventLoop).
     */
    public function handleSubscriberData(): void
    {
        if (!$this->subSocket || !is_resource($this->subSocket)) {
            return;
        }

        $chunk = @fread($this->subSocket, 8192);
        if ($chunk === false || ($chunk === '' && feof($this->subSocket))) {
            $this->handleSubscriberDisconnect();
            return;
        }

        $this->readBuffer .= $chunk;

        while (($frame = RespParser::parse($this->readBuffer)) !== null) {
            if (!is_array($frame) || empty($frame)) {
                continue;
            }

            $type = (string) $frame[0];

            if ($type === 'message' && count($frame) >= 3) {
                $channel = (string) $frame[1];
                $message = $frame[2];

                if (!empty($this->subscribers[$channel])) {
                    foreach ($this->subscribers[$channel] as $handler) {
                        try {
                            $handler($channel, $message);
                        } catch (Throwable $e) {
                            echo sprintf("[LiteSocket RedisPubSub Error] %s\n", $e->getMessage());
                        }
                    }
                }
            } elseif ($type === 'pmessage' && count($frame) >= 4) {
                $channel = (string) $frame[2];
                $message = $frame[3];

                if (!empty($this->subscribers[$channel])) {
                    foreach ($this->subscribers[$channel] as $handler) {
                        try {
                            $handler($channel, $message);
                        } catch (Throwable $e) {
                            echo sprintf("[LiteSocket RedisPubSub Error] %s\n", $e->getMessage());
                        }
                    }
                }
            }
        }
    }

    public function close(): void
    {
        $this->closePublisherSocket();
        $this->closeSubscriberSocket();
    }

    /**
     * @return resource
     */
    private function getPublisherSocket()
    {
        if ($this->pubSocket && is_resource($this->pubSocket)) {
            return $this->pubSocket;
        }

        $this->pubSocket = $this->createConnection(blocking: true);
        return $this->pubSocket;
    }

    /**
     * @return resource
     */
    private function getSubscriberSocket()
    {
        if ($this->subSocket && is_resource($this->subSocket)) {
            return $this->subSocket;
        }

        $this->subSocket = $this->createConnection(blocking: false);
        return $this->subSocket;
    }

    /**
     * @return resource
     */
    private function createConnection(bool $blocking)
    {
        $remote = sprintf('tcp://%s:%d', $this->host, $this->port);
        $errno = 0;
        $errstr = '';

        $socket = @stream_socket_client($remote, $errno, $errstr, $this->timeout, STREAM_CLIENT_CONNECT);
        if (!$socket) {
            throw new RuntimeException(sprintf("Failed to connect to Redis at %s: [%d] %s", $remote, $errno, $errstr));
        }

        // Authenticate if password provided
        if ($this->password !== null && $this->password !== '') {
            $auth = RespParser::encode(['AUTH', $this->password]);
            fwrite($socket, $auth);
            $resp = fgets($socket);
            if ($resp === false || str_starts_with($resp, '-')) {
                fclose($socket);
                throw new RuntimeException("Redis authentication failed: " . trim((string)$resp));
            }
        }

        // Select database if non-zero
        if ($this->database > 0) {
            $select = RespParser::encode(['SELECT', (string)$this->database]);
            fwrite($socket, $select);
            $resp = fgets($socket);
            if ($resp === false || str_starts_with($resp, '-')) {
                fclose($socket);
                throw new RuntimeException("Redis SELECT {$this->database} failed: " . trim((string)$resp));
            }
        }

        stream_set_blocking($socket, $blocking);
        return $socket;
    }

    private function registerSubSocketToLoop(): void
    {
        if ($this->loop && $this->subSocket && is_resource($this->subSocket) && !$this->subSocketInLoop) {
            $this->loop->addRead($this->subSocket, function () {
                $this->handleSubscriberData();
            });
            $this->subSocketInLoop = true;
        }
    }

    private function readPublisherResponse($socket): void
    {
        $buffer = '';
        while (!feof($socket)) {
            $line = fgets($socket);
            if ($line === false) break;
            $buffer .= $line;
            $parsed = RespParser::parse($buffer);
            if ($parsed !== null) {
                break;
            }
        }
    }

    private function handleSubscriberDisconnect(): void
    {
        $this->closeSubscriberSocket();

        // Attempt reconnection and resubscribe if loop is running
        if ($this->loop && !empty($this->activeChannels)) {
            $this->loop->delay(1.0, function () {
                try {
                    $socket = $this->getSubscriberSocket();
                    $channels = array_keys($this->activeChannels);
                    foreach ($channels as $ch) {
                        @fwrite($socket, RespParser::encode(['SUBSCRIBE', $ch]));
                    }
                    $this->registerSubSocketToLoop();
                } catch (Throwable $e) {
                    echo sprintf("[LiteSocket RedisPubSub Reconnect Error] %s\n", $e->getMessage());
                }
            });
        }
    }

    private function closePublisherSocket(): void
    {
        if ($this->pubSocket && is_resource($this->pubSocket)) {
            @fclose($this->pubSocket);
            $this->pubSocket = null;
        }
    }

    private function closeSubscriberSocket(): void
    {
        if ($this->subSocket && is_resource($this->subSocket)) {
            if ($this->loop && $this->subSocketInLoop) {
                $this->loop->removeRead($this->subSocket);
                $this->subSocketInLoop = false;
            }
            @fclose($this->subSocket);
            $this->subSocket = null;
            $this->readBuffer = '';
        }
    }

    public function __destruct()
    {
        $this->close();
    }
}
