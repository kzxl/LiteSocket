<?php

declare(strict_types=1);

namespace LiteSocket\Tests\Unit;

use LiteSocket\Runtime\StreamSelectLoop;
use PHPUnit\Framework\TestCase;

class StreamSelectLoopTest extends TestCase
{
    public function testAfterTimerExecutes(): void
    {
        $loop = new StreamSelectLoop();
        $executed = false;

        $loop->after(0.01, function () use (&$executed, $loop) {
            $executed = true;
            $loop->stop();
        });

        $loop->run();
        $this->assertTrue($executed);
    }

    public function testEveryTimerExecutesMultipleTimes(): void
    {
        $loop = new StreamSelectLoop();
        $count = 0;

        $loop->every(0.01, function () use (&$count, $loop) {
            $count++;
            if ($count >= 3) {
                $loop->stop();
            }
        });

        $loop->run();
        $this->assertGreaterThanOrEqual(3, $count);
    }

    public function testCancelTimerPreventsExecution(): void
    {
        $loop = new StreamSelectLoop();
        $executed = false;

        $tid = $loop->after(0.02, function () use (&$executed) {
            $executed = true;
        });

        $loop->cancelTimer($tid);

        // Run another timer that stops the loop after 0.04s
        $loop->after(0.04, function () use ($loop) {
            $loop->stop();
        });

        $loop->run();
        $this->assertFalse($executed);
    }

    public function testSocketReadAndWriteMultiplexing(): void
    {
        $domain = DIRECTORY_SEPARATOR === '\\' ? STREAM_PF_INET : STREAM_PF_UNIX;
        $pair = stream_socket_pair($domain, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        $this->assertIsArray($pair);

        [$sockA, $sockB] = $pair;
        stream_set_blocking($sockA, false);
        stream_set_blocking($sockB, false);

        $loop = new StreamSelectLoop();
        $received = '';

        $loop->addRead($sockB, function ($sock) use (&$received, $loop) {
            $data = fread($sock, 1024);
            $received .= $data;
            if ($received === 'PING') {
                $loop->stop();
            }
        });

        $loop->addWrite($sockA, function ($sock) use ($loop) {
            fwrite($sock, 'PING');
            $loop->removeWrite($sock);
        });

        $loop->run();

        $this->assertEquals('PING', $received);

        fclose($sockA);
        fclose($sockB);
    }
}
