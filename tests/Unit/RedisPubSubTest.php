<?php

declare(strict_types=1);

namespace LiteSocket\Tests\Unit;

use PHPUnit\Framework\TestCase;
use LiteSocket\Messaging\PubSub\RedisPubSub;
use LiteSocket\Runtime\StreamSelectLoop;

class RedisPubSubTest extends TestCase
{
    public function testInstantiationAndSubscriptionBookkeeping(): void
    {
        $loop = new StreamSelectLoop();
        $pubsub = new RedisPubSub('127.0.0.1', 6379, loop: $loop);

        $this->assertInstanceOf(RedisPubSub::class, $pubsub);

        $called = false;
        $handler = function (string $channel, mixed $msg) use (&$called) {
            $called = true;
        };

        // Note: Connecting to real Redis might not be available in local environment,
        // so we test subscription bookkeeping and teardown safely.
        $reflection = new \ReflectionClass($pubsub);
        $subscribersProp = $reflection->getProperty('subscribers');
        $subscribersProp->setAccessible(true);

        $subMapProp = $reflection->getProperty('subMap');
        $subMapProp->setAccessible(true);

        // Manually register in internal data structures to verify handleSubscriberData
        $subId = 1;
        $subscribersProp->setValue($pubsub, ['chat' => [$subId => $handler]]);
        $subMapProp->setValue($pubsub, [$subId => 'chat']);

        // Simulate subscriber socket present with RESP frame
        $dummySocket = fopen('php://memory', 'r+');
        fwrite($dummySocket, "*3\r\n$7\r\nmessage\r\n$4\r\nchat\r\n$11\r\nhello redis\r\n");
        rewind($dummySocket);

        $subSocketProp = $reflection->getProperty('subSocket');
        $subSocketProp->setAccessible(true);
        $subSocketProp->setValue($pubsub, $dummySocket);

        // Call handleSubscriberData
        $pubsub->handleSubscriberData();

        $this->assertTrue($called);

        // Test unsubscribe
        $pubsub->unsubscribe($subId);
        $this->assertArrayNotHasKey('chat', $subscribersProp->getValue($pubsub));

        fclose($dummySocket);
    }
}
