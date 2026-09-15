<?php

declare(strict_types=1);

namespace LiteSocket\Messaging;

use LiteSocket\Connection;
use LiteSocket\Messaging\PubSub\PubSubInterface;
use LiteSocket\WebSocketServer;
use Throwable;

/**
 * Multi-Node Cluster Bridge for LiteSocket.
 * Coordinates horizontal room replication across multiple processes/servers via PubSubInterface.
 * Features automatic echo-loop elimination via unique Node IDs and dynamic room subscription lifecycle.
 */
class ClusterBridge
{
    private WebSocketServer $server;
    private PubSubInterface $pubsub;
    private string $nodeId;
    private string $channelPrefix;

    /** @var array<string, int> room => subscriptionId */
    private array $activeSubscriptions = [];

    public function __construct(
        WebSocketServer $server,
        PubSubInterface $pubsub,
        ?string $nodeId = null,
        string $channelPrefix = 'litesocket:cluster:room:'
    ) {
        $this->server = $server;
        $this->pubsub = $pubsub;
        $this->nodeId = $nodeId ?? bin2hex(random_bytes(8));
        $this->channelPrefix = $channelPrefix;
    }

    public function getNodeId(): string
    {
        return $this->nodeId;
    }

    public function getPubSub(): PubSubInterface
    {
        return $this->pubsub;
    }

    /**
     * Broadcast to a room across the entire cluster.
     * Broadcasts immediately to local connections, then publishes envelope to PubSub.
     *
     * @return int Number of local clients enqueued
     */
    public function broadcast(string $room, mixed $message, ?Connection $exclude = null, bool $droppable = false): int
    {
        // 1. Broadcast locally
        $localCount = $this->server->getRoomManager()->broadcast($room, $message, $exclude, $droppable);

        // 2. Publish to cluster channel
        $channel = $this->channelPrefix . $room;
        $envelope = [
            'node'    => $this->nodeId,
            'room'    => $room,
            'exclude' => $exclude?->getId(),
            'payload' => $message,
        ];

        try {
            $this->pubsub->publish($channel, $envelope);
        } catch (Throwable $e) {
            echo sprintf("[LiteSocket ClusterBridge Publish Error] %s\n", $e->getMessage());
        }

        return $localCount;
    }

    /**
     * Subscribe this node to a room channel on the cluster.
     */
    public function subscribeRoom(string $room): void
    {
        if (isset($this->activeSubscriptions[$room])) {
            return;
        }

        $channel = $this->channelPrefix . $room;
        $subId = $this->pubsub->subscribe($channel, function (string $ch, mixed $rawMessage) use ($room) {
            $this->handleClusterMessage($room, $rawMessage);
        });

        $this->activeSubscriptions[$room] = $subId;
    }

    /**
     * Unsubscribe this node from a room channel on the cluster.
     */
    public function unsubscribeRoom(string $room): void
    {
        if (isset($this->activeSubscriptions[$room])) {
            $subId = $this->activeSubscriptions[$room];
            unset($this->activeSubscriptions[$room]);
            $this->pubsub->unsubscribe($subId);
        }
    }

    /**
     * Handle incoming message from cluster PubSub.
     */
    public function handleClusterMessage(string $room, mixed $rawMessage): void
    {
        $envelope = is_string($rawMessage) ? json_decode($rawMessage, true) : $rawMessage;
        if (!is_array($envelope) || !isset($envelope['node'], $envelope['payload'])) {
            return;
        }

        // Echo loop prevention: Ignore messages originating from this node
        if ($envelope['node'] === $this->nodeId) {
            return;
        }

        // Broadcast to local connections in this room
        $this->server->getRoomManager()->broadcast($room, $envelope['payload']);
    }

    /**
     * Check if a room is currently subscribed to on the cluster.
     */
    public function isSubscribed(string $room): bool
    {
        return isset($this->activeSubscriptions[$room]);
    }
}
