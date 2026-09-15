<?php

declare(strict_types=1);

namespace LiteSocket\Transport;

/**
 * Contract for network socket transport.
 */
interface TransportInterface
{
    /**
     * Bind and listen on host and port.
     */
    public function listen(string $host, int $port, array $options = []): void;

    /**
     * Accept a pending incoming client connection.
     *
     * @return array{0: resource, 1: string}|null [socket, peerAddress] or null if none
     */
    public function accept(): ?array;

    /**
     * Get the master listening socket resource.
     *
     * @return resource|null
     */
    public function getMasterSocket();

    /**
     * Close the master listening socket.
     */
    public function close(): void;
}
