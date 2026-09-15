<?php

declare(strict_types=1);

namespace LiteSocket\Tests\Unit;

use PHPUnit\Framework\TestCase;
use LiteSocket\WebSocketServer;
use LiteSocket\Connection;
use LiteSocket\Messaging\ClusterBridge;
use LiteSocket\Messaging\PubSub\LocalPubSub;

class ClusterBridgeTest extends TestCase
{
    public function testClusterBroadcastAndEchoElimination(): void
    {
        $serverA = new WebSocketServer('127.0.0.1', 9901);
        $serverB = new WebSocketServer('127.0.0.1', 9902);

        // Shared pubsub bus between node A and node B (simulating Redis)
        $sharedBus = new LocalPubSub();

        $serverA->enableCluster($sharedBus, nodeId: 'node-A');
        $serverB->enableCluster($sharedBus, nodeId: 'node-B');

        // Connect client to server B in room 'lobby'
        $clientSocket = fopen('php://memory', 'r+');
        $connOnB = new Connection('conn-b-1', $clientSocket, '127.0.0.1', 50000);
        $connOnB->setHandshakeDone(true);
        $serverB->joinRoom('lobby', $connOnB);

        $this->assertTrue($serverB->getClusterBridge()->isSubscribed('lobby'));

        // Node A broadcasts to room 'lobby'
        $serverA->broadcastToRoom('lobby', ['msg' => 'Hello from Node A']);

        // Check that client on Node B received the frame
        rewind($clientSocket);
        $writtenToB = stream_get_contents($clientSocket);
        $this->assertNotEmpty($writtenToB);
        $this->assertStringContainsString('Hello from Node A', $writtenToB);

        // Ensure echo loop didn't trigger:
        // Client on Node A (if any) shouldn't receive duplicate from cluster
        $clientSocketA = fopen('php://memory', 'r+');
        $connOnA = new Connection('conn-a-1', $clientSocketA, '127.0.0.1', 50001);
        $connOnA->setHandshakeDone(true);
        $serverA->joinRoom('lobby', $connOnA);

        // Reset stream on A
        ftruncate($clientSocketA, 0);
        rewind($clientSocketA);

        // Broadcast from A again
        $serverA->broadcastToRoom('lobby', ['msg' => 'Ping 2']);

        rewind($clientSocketA);
        $contentA = stream_get_contents($clientSocketA);
        // Should contain Ping 2 exactly once (not duplicated by cluster echo)
        $this->assertSame(1, substr_count($contentA, 'Ping 2'));

        // Clean up
        $serverB->leaveRoom('lobby', $connOnB);
        $this->assertFalse($serverB->getClusterBridge()->isSubscribed('lobby'));
    }
}
