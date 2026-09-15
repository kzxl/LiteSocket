<?php

declare(strict_types=1);

namespace LiteSocket\Tests\Unit;

use PHPUnit\Framework\TestCase;
use LiteSocket\Messaging\PubSub\RespParser;
use InvalidArgumentException;

class RespParserTest extends TestCase
{
    public function testEncodeGeneratesValidResp(): void
    {
        $encoded = RespParser::encode(['PUBLISH', 'chat:general', 'hello world']);
        $expected = "*3\r\n$7\r\nPUBLISH\r\n$12\r\nchat:general\r\n$11\r\nhello world\r\n";
        $this->assertSame($expected, $encoded);
    }

    public function testParseSimpleString(): void
    {
        $buffer = "+OK\r\n";
        $res = RespParser::parse($buffer);
        $this->assertSame('OK', $res);
        $this->assertSame('', $buffer);
    }

    public function testParseError(): void
    {
        $buffer = "-ERR unknown command 'FOO'\r\n";
        $res = RespParser::parse($buffer);
        $this->assertInstanceOf(InvalidArgumentException::class, $res);
        $this->assertSame("ERR unknown command 'FOO'", $res->getMessage());
        $this->assertSame('', $buffer);
    }

    public function testParseInteger(): void
    {
        $buffer = ":42\r\n";
        $res = RespParser::parse($buffer);
        $this->assertSame(42, $res);
        $this->assertSame('', $buffer);
    }

    public function testParseBulkString(): void
    {
        $buffer = "$5\r\nhello\r\n";
        $res = RespParser::parse($buffer);
        $this->assertSame('hello', $res);
        $this->assertSame('', $buffer);
    }

    public function testParseNullBulkString(): void
    {
        $buffer = "$-1\r\n";
        $res = RespParser::parse($buffer);
        $this->assertNull($res);
        $this->assertSame('', $buffer);
    }

    public function testParseIncompleteDataReturnsNullAndLeavesBuffer(): void
    {
        $buffer = "$5\r\nhel"; // Incomplete
        $res = RespParser::parse($buffer);
        $this->assertNull($res);
        $this->assertSame("$5\r\nhel", $buffer);

        // Complete the buffer
        $buffer .= "lo\r\n";
        $res = RespParser::parse($buffer);
        $this->assertSame('hello', $res);
        $this->assertSame('', $buffer);
    }

    public function testParseArrayOfBulkStrings(): void
    {
        $buffer = "*3\r\n$7\r\nmessage\r\n$5\r\nlobby\r\n$5\r\nhello\r\n";
        $res = RespParser::parse($buffer);
        $this->assertSame(['message', 'lobby', 'hello'], $res);
        $this->assertSame('', $buffer);
    }

    public function testParsePipelinedMessages(): void
    {
        $buffer = "+OK\r\n:100\r\n$4\r\ntest\r\n";

        $f1 = RespParser::parse($buffer);
        $this->assertSame('OK', $f1);

        $f2 = RespParser::parse($buffer);
        $this->assertSame(100, $f2);

        $f3 = RespParser::parse($buffer);
        $this->assertSame('test', $f3);

        $this->assertSame('', $buffer);
    }
}
