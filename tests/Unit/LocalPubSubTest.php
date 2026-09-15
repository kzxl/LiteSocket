<?php

declare(strict_types=1);

namespace LiteSocket\Tests\Unit;

use LiteSocket\Messaging\PubSub\LocalPubSub;
use PHPUnit\Framework\TestCase;

class LocalPubSubTest extends TestCase
{
    public function testPublishAndSubscribe(): void
    {
        $pubsub = new LocalPubSub();
        $received = [];

        $subId = $pubsub->subscribe('chat.room1', function (string $channel, mixed $msg) use (&$received) {
            $received[] = $msg;
        });

        $pubsub->publish('chat.room1', 'Hello Room 1');
        $pubsub->publish('chat.other', 'Ignored Message');

        $this->assertCount(1, $received);
        $this->assertEquals('Hello Room 1', $received[0]);

        // Test unsubscribe
        $pubsub->unsubscribe($subId);
        $pubsub->publish('chat.room1', 'Second Message');
        $this->assertCount(1, $received); // Should not increase
    }
}
