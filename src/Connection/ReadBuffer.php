<?php

declare(strict_types=1);

namespace LiteSocket\Connection;

use OverflowException;

/**
 * Memory-guarded read buffer for incoming socket byte streams.
 * Protects against buffer overflow / OOM DoS attacks.
 */
class ReadBuffer
{
    private string $buffer = '';
    private int $maxBufferSize;

    public function __construct(int $maxBufferSize = 2097152) // Default 2MB
    {
        $this->maxBufferSize = $maxBufferSize;
    }

    /**
     * Append incoming raw data to buffer with strict size limit.
     *
     * @throws OverflowException If buffer exceeds maximum allowed capacity.
     */
    public function append(string $data): void
    {
        $newLength = strlen($this->buffer) + strlen($data);
        if ($newLength > $this->maxBufferSize) {
            throw new OverflowException("Read buffer overflow: {$newLength} bytes exceeds limit of {$this->maxBufferSize} bytes");
        }
        $this->buffer .= $data;
    }

    /**
     * Get the raw buffer string.
     */
    public function get(): string
    {
        return $this->buffer;
    }

    /**
     * Get current buffer byte length.
     */
    public function length(): int
    {
        return strlen($this->buffer);
    }

    /**
     * Consume (remove) leading N bytes from buffer.
     */
    public function consume(int $bytes): void
    {
        if ($bytes <= 0) {
            return;
        }
        $this->buffer = (string)substr($this->buffer, $bytes);
    }

    /**
     * Clear all contents in buffer.
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
