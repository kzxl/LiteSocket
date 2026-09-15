<?php

declare(strict_types=1);

namespace LiteSocket\Connection;

use LiteSocket\Connection;

/**
 * Manages active connection lifecycle with O(1) indexed lookup by ID and socket descriptor.
 */
class ConnectionPool
{
    /** @var array<string, Connection> connId => Connection */
    private array $connections = [];

    /** @var array<int, Connection> socketId => Connection */
    private array $socketMap = [];

    /**
     * Add a new connection to the pool.
     */
    public function add(Connection $conn): void
    {
        $connId = $conn->getId();
        $this->connections[$connId] = $conn;

        $sock = $conn->getSocket();
        if (is_resource($sock)) {
            $this->socketMap[(int)$sock] = $conn;
        }
    }

    /**
     * Remove connection from the pool.
     */
    public function remove(Connection $conn): void
    {
        $connId = $conn->getId();
        unset($this->connections[$connId]);

        $sock = $conn->getSocket();
        if (is_resource($sock)) {
            unset($this->socketMap[(int)$sock]);
        }
    }

    /**
     * Lookup connection by connection ID.
     */
    public function getById(string $connId): ?Connection
    {
        return $this->connections[$connId] ?? null;
    }

    /**
     * Lookup connection by socket resource (O(1)).
     *
     * @param resource|int $socket
     */
    public function getBySocket($socket): ?Connection
    {
        $sockId = is_resource($socket) ? (int)$socket : (int)$socket;
        return $this->socketMap[$sockId] ?? null;
    }

    public function has(string $connId): bool
    {
        return isset($this->connections[$connId]);
    }

    /**
     * @return array<string, Connection>
     */
    public function all(): array
    {
        return $this->connections;
    }

    public function count(): int
    {
        return count($this->connections);
    }

    public function clear(): void
    {
        $this->connections = [];
        $this->socketMap = [];
    }
}
