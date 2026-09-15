<?php

declare(strict_types=1);

namespace LiteSocket\Transport;

use RuntimeException;

/**
 * Native PHP stream socket TCP server transport.
 */
class StreamSocketTransport implements TransportInterface
{
    /** @var resource|null */
    private $masterSocket = null;
    private string $host = '0.0.0.0';
    private int $port = 8088;

    public function listen(string $host, int $port, array $options = []): void
    {
        $this->host = $host;
        $this->port = $port;

        $address = "tcp://{$this->host}:{$this->port}";
        $backlog = (int)($options['backlog'] ?? 1024);
        $reusePort = (bool)($options['so_reuseport'] ?? true);

        $context = stream_context_create([
            'socket' => [
                'so_reuseport' => $reusePort ? 1 : 0,
                'backlog'      => $backlog,
            ],
        ]);

        $errno = 0;
        $errstr = '';
        $this->masterSocket = @stream_socket_server(
            $address,
            $errno,
            $errstr,
            STREAM_SERVER_BIND | STREAM_SERVER_LISTEN,
            $context
        );

        if (!$this->masterSocket) {
            throw new RuntimeException("LiteSocket failed to bind on {$address}: [{$errno}] {$errstr}");
        }

        stream_set_blocking($this->masterSocket, false);
    }

    public function accept(): ?array
    {
        if (!is_resource($this->masterSocket)) {
            return null;
        }

        $peerName = '';
        $clientSocket = @stream_socket_accept($this->masterSocket, 0, $peerName);

        if (!$clientSocket) {
            return null;
        }

        stream_set_blocking($clientSocket, false);
        return [$clientSocket, (string)$peerName];
    }

    public function getMasterSocket()
    {
        return $this->masterSocket;
    }

    public function close(): void
    {
        if (is_resource($this->masterSocket)) {
            @fclose($this->masterSocket);
            $this->masterSocket = null;
        }
    }
}
