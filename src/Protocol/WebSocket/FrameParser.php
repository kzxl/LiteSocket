<?php

declare(strict_types=1);

namespace LiteSocket\Protocol\WebSocket;

use LiteSocket\Connection;
use OverflowException;

/**
 * Stream parser for extracting discrete WebSocket frames from connection buffers.
 * Handles fragmentation boundaries and control frames (Ping/Pong/Close).
 */
class FrameParser
{
    private int $maxFrameSize;

    public function __construct(int $maxFrameSize = 2097152) // 2MB
    {
        $this->maxFrameSize = $maxFrameSize;
    }

    /**
     * Parse all fully received frames from the connection's read buffer.
     *
     * @param Connection $conn
     * @return array<int, array{opcode: int, payload: string}>
     * @throws OverflowException If payload exceeds maxFrameSize.
     */
    public function parse(Connection $conn): array
    {
        $frames = [];

        while (strlen($conn->getBuffer()) > 0) {
            $frame = Frame::decode($conn->getBuffer());
            if ($frame === null) {
                // Incomplete frame header or payload, wait for next socket read
                break;
            }

            if (strlen($frame['payload']) > $this->maxFrameSize) {
                $conn->close(1009, 'Message payload exceeds maximum allowed size');
                throw new OverflowException("WebSocket frame payload exceeds limit of {$this->maxFrameSize} bytes");
            }

            $conn->consumeBuffer($frame['bytesConsumed']);
            $frames[] = [
                'opcode'  => $frame['opcode'],
                'payload' => $frame['payload'],
            ];
        }

        return $frames;
    }

    public function getMaxFrameSize(): int
    {
        return $this->maxFrameSize;
    }

    public function setMaxFrameSize(int $size): void
    {
        $this->maxFrameSize = $size;
    }
}
