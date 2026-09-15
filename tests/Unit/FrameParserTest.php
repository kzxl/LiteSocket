<?php

declare(strict_types=1);

namespace LiteSocket\Tests\Unit;

use LiteSocket\Connection;
use LiteSocket\Protocol\WebSocket\Frame;
use LiteSocket\Protocol\WebSocket\FrameParser;
use OverflowException;
use PHPUnit\Framework\TestCase;

class FrameParserTest extends TestCase
{
    private function createMockConnection(): Connection
    {
        $domain = DIRECTORY_SEPARATOR === '\\' ? STREAM_PF_INET : STREAM_PF_UNIX;
        $pair = stream_socket_pair($domain, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        return new Connection('test_1', $pair[0]);
    }

    public function testParseSingleFrame(): void
    {
        $conn = $this->createMockConnection();
        $parser = new FrameParser();

        $encoded = Frame::encodeText('Hello World');
        $conn->appendBuffer($encoded);

        $frames = $parser->parse($conn);
        $this->assertCount(1, $frames);
        $this->assertEquals(Frame::OPCODE_TEXT, $frames[0]['opcode']);
        $this->assertEquals('Hello World', $frames[0]['payload']);
        $this->assertEquals(0, strlen($conn->getBuffer()));
    }

    public function testParseMultipleFramesInOneBuffer(): void
    {
        $conn = $this->createMockConnection();
        $parser = new FrameParser();

        $encoded1 = Frame::encodeText('Message 1');
        $encoded2 = Frame::encodeText('Message 2');
        $conn->appendBuffer($encoded1 . $encoded2);

        $frames = $parser->parse($conn);
        $this->assertCount(2, $frames);
        $this->assertEquals('Message 1', $frames[0]['payload']);
        $this->assertEquals('Message 2', $frames[1]['payload']);
        $this->assertEquals(0, strlen($conn->getBuffer()));
    }

    public function testParseFragmentedFrameAcrossReads(): void
    {
        $conn = $this->createMockConnection();
        $parser = new FrameParser();

        $fullFrame = Frame::encodeText('Long fragmented content');
        $part1 = substr($fullFrame, 0, 4);
        $part2 = substr($fullFrame, 4);

        // Read Part 1: incomplete
        $conn->appendBuffer($part1);
        $frames = $parser->parse($conn);
        $this->assertEmpty($frames);
        $this->assertEquals(4, strlen($conn->getBuffer()));

        // Read Part 2: completes frame
        $conn->appendBuffer($part2);
        $frames = $parser->parse($conn);
        $this->assertCount(1, $frames);
        $this->assertEquals('Long fragmented content', $frames[0]['payload']);
        $this->assertEquals(0, strlen($conn->getBuffer()));
    }

    public function testExceedingMaxFrameSizeThrowsException(): void
    {
        $conn = $this->createMockConnection();
        $parser = new FrameParser(10); // max 10 bytes

        $largeFrame = Frame::encodeText('1234567890EXTRA');
        $conn->appendBuffer($largeFrame);

        $this->expectException(OverflowException::class);
        $parser->parse($conn);
    }
}
