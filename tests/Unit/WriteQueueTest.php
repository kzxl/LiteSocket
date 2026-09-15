<?php

declare(strict_types=1);

namespace LiteSocket\Tests\Unit;

use LiteSocket\Connection\WriteQueue;
use OverflowException;
use PHPUnit\Framework\TestCase;

class WriteQueueTest extends TestCase
{
    public function testEnqueueAndFlush(): void
    {
        $domain = DIRECTORY_SEPARATOR === '\\' ? STREAM_PF_INET : STREAM_PF_UNIX;
        $pair = stream_socket_pair($domain, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        [$sockA, $sockB] = $pair;
        stream_set_blocking($sockA, false);
        stream_set_blocking($sockB, false);

        $queue = new WriteQueue(1024);
        $this->assertFalse($queue->hasPendingData());

        $queue->enqueue('HELLO_WORLD');
        $this->assertTrue($queue->hasPendingData());
        $this->assertEquals(11, $queue->length());

        $flushed = $queue->flush($sockA);
        $this->assertEquals(11, $flushed);
        $this->assertFalse($queue->hasPendingData());

        $received = fread($sockB, 1024);
        $this->assertEquals('HELLO_WORLD', $received);

        fclose($sockA);
        fclose($sockB);
    }

    public function testBackpressureOverflowThrowsExceptionForReliable(): void
    {
        $queue = new WriteQueue(10); // 10 bytes max

        $this->expectException(OverflowException::class);
        $queue->enqueue('THIS_IS_LONGER_THAN_10_BYTES', false);
    }

    public function testBackpressureDropsFrameWhenDroppableIsTrue(): void
    {
        $queue = new WriteQueue(10); // 10 bytes max

        $ok = $queue->enqueue('THIS_IS_LONGER_THAN_10_BYTES', true);
        $this->assertFalse($ok);
        $this->assertEquals(1, $queue->getDroppedFramesCount());
        $this->assertFalse($queue->hasPendingData());
    }
}
