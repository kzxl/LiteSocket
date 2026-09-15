<?php

declare(strict_types=1);

namespace LiteSocket\Tests\Unit;

use LiteSocket\Connection\ReadBuffer;
use OverflowException;
use PHPUnit\Framework\TestCase;

class ReadBufferTest extends TestCase
{
    public function testAppendAndConsume(): void
    {
        $buffer = new ReadBuffer(1024);
        $buffer->append('ABCDEF');

        $this->assertEquals(6, $buffer->length());
        $this->assertEquals('ABCDEF', $buffer->get());

        $buffer->consume(3);
        $this->assertEquals(3, $buffer->length());
        $this->assertEquals('DEF', $buffer->get());

        $buffer->clear();
        $this->assertEquals(0, $buffer->length());
        $this->assertEquals('', $buffer->get());
    }

    public function testBufferOverflowThrowsException(): void
    {
        $buffer = new ReadBuffer(5);

        $this->expectException(OverflowException::class);
        $buffer->append('TOOLONGDATA');
    }
}
