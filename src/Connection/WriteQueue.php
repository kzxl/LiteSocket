<?php

declare(strict_types=1);

namespace LiteSocket\Connection;

use OverflowException;

/**
 * Non-blocking Write Buffer with Backpressure protection.
 * Queues outgoing frames in RAM and flushes chunks during stream_select writable events.
 */
class WriteQueue
{
    private string $buffer = '';
    private int $maxBufferSize;
    private int $droppedFrames = 0;

    public function __construct(int $maxBufferSize = 4194304) // Default 4MB
    {
        $this->maxBufferSize = $maxBufferSize;
    }

    /**
     * Enqueue a raw frame payload to be written asynchronously.
     *
     * @param string $data Raw frame bytes
     * @param bool $droppable If true, discard payload when buffer exceeds capacity instead of throwing.
     * @return bool True if enqueued, false if dropped due to backpressure.
     * @throws OverflowException If non-droppable payload causes buffer to exceed maxBufferSize.
     */
    public function enqueue(string $data, bool $droppable = false): bool
    {
        $dataLength = strlen($data);
        if ($dataLength === 0) {
            return true;
        }

        $newLength = strlen($this->buffer) + $dataLength;
        if ($newLength > $this->maxBufferSize) {
            if ($droppable) {
                $this->droppedFrames++;
                return false;
            }
            throw new OverflowException(
                "Write buffer overflow: {$newLength} bytes exceeds limit of {$this->maxBufferSize} bytes. Backpressure triggered."
            );
        }

        $this->buffer .= $data;
        return true;
    }

    /**
     * Attempt to flush pending data to the stream socket.
     *
     * @param resource $socket
     * @return int Number of bytes successfully written
     */
    public function flush($socket): int
    {
        if ($this->buffer === '' || !is_resource($socket)) {
            return 0;
        }

        $written = @fwrite($socket, $this->buffer);
        if ($written === false || $written === 0) {
            return 0;
        }

        $this->buffer = (string)substr($this->buffer, $written);
        return $written;
    }

    /**
     * Check whether there is pending data awaiting transmission.
     */
    public function hasPendingData(): bool
    {
        return $this->buffer !== '';
    }

    /**
     * Current size of unwritten bytes in queue.
     */
    public function length(): int
    {
        return strlen($this->buffer);
    }

    /**
     * Count of droppable frames that were discarded due to backpressure.
     */
    public function getDroppedFramesCount(): int
    {
        return $this->droppedFrames;
    }

    /**
     * Clear all pending data in queue.
     */
    public function clear(): void
    {
        $this->buffer = '';
    }

    public function getMaxBufferSize(): int
    {
        return $this->maxBufferSize;
    }

    public function setMaxBufferSize(int $size): void
    {
        $this->maxBufferSize = $size;
    }
}
