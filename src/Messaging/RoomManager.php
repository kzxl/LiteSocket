<?php

declare(strict_types=1);

namespace LiteSocket\Messaging;

use LiteSocket\Connection;

/**
 * Ultra-lightweight in-memory Room / Channel Pub-Sub manager.
 * Zero enterprise bloat: pure PHP arrays with O(1) membership operations.
 */
class RoomManager
{
    /** @var array<string, array<string, Connection>> roomName => [connId => Connection] */
    private array $rooms = [];

    /**
     * Add connection to a room.
     */
    public function join(string $room, Connection $conn): void
    {
        $connId = $conn->getId();
        if (!isset($this->rooms[$room])) {
            $this->rooms[$room] = [];
        }
        $this->rooms[$room][$connId] = $conn;
        $conn->join($room);
    }

    /**
     * Remove connection from a room.
     */
    public function leave(string $room, Connection $conn): void
    {
        $connId = $conn->getId();
        if (isset($this->rooms[$room][$connId])) {
            unset($this->rooms[$room][$connId]);
            if (empty($this->rooms[$room])) {
                unset($this->rooms[$room]);
            }
        }
        $conn->leave($room);
    }

    /**
     * Remove connection from all joined rooms.
     */
    public function leaveAll(Connection $conn): void
    {
        foreach ($conn->getRooms() as $room) {
            $this->leave($room, $conn);
        }
    }

    /**
     * Broadcast a message to all connections in a room.
     *
     * @param string $room
     * @param mixed $message String, array, or JSON-serializable object
     * @param Connection|null $exclude Optional connection to omit
     * @param bool $droppable Whether frames can be discarded under backpressure
     * @return int Number of clients enqueued
     */
    public function broadcast(string $room, mixed $message, ?Connection $exclude = null, bool $droppable = false): int
    {
        if (!isset($this->rooms[$room])) {
            return 0;
        }

        $payload = is_string($message) ? $message : json_encode($message, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        $excludeId = $exclude?->getId();
        $count = 0;

        foreach ($this->rooms[$room] as $connId => $conn) {
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
     * Get all connections in a room.
     *
     * @return array<string, Connection>
     */
    public function getClients(string $room): array
    {
        return $this->rooms[$room] ?? [];
    }

    /**
     * Get count of active connections in a room.
     */
    public function getCount(string $room): int
    {
        return isset($this->rooms[$room]) ? count($this->rooms[$room]) : 0;
    }

    /**
     * Check if a room has any active members.
     */
    public function hasRoom(string $room): bool
    {
        return !empty($this->rooms[$room]);
    }

    /**
     * Get all active room names.
     *
     * @return string[]
     */
    public function getRooms(): array
    {
        return array_keys($this->rooms);
    }

    /**
     * Clear all rooms.
     */
    public function clear(): void
    {
        $this->rooms = [];
    }
}
