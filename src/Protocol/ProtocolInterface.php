<?php

declare(strict_types=1);

namespace LiteSocket\Protocol;

use LiteSocket\Connection;

/**
 * Protocol contract for framing, handshakes, and wire parsing.
 */
interface ProtocolInterface
{
    /**
     * Attempt handshake on connection. Returns true if handshake completed, false if waiting for more data.
     */
    public function handshake(Connection $conn): bool;

    /**
     * Parse complete messages/frames from connection's read buffer.
     *
     * @return array<int, array{opcode: int, payload: string}>
     */
    public function parseFrames(Connection $conn): array;
}
