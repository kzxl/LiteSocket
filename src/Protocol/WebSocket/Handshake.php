<?php

declare(strict_types=1);

namespace LiteSocket\Protocol\WebSocket;

use LiteSocket\Connection;

/**
 * RFC 6455 WebSocket Handshake handler.
 * Validates HTTP upgrade requests, computes SHA-1 Sec-WebSocket-Accept token, and validates origins.
 */
class Handshake
{
    public const MAGIC_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
    public const MAX_HEADER_BYTES = 4096;

    /**
     * Process handshake for a connecting client.
     *
     * @param Connection $conn
     * @param array<string>|null $allowedOrigins Optional origin whitelist (null = allow all)
     * @return bool True if handshake succeeded, false if waiting for more header data.
     */
    public static function handle(Connection $conn, ?array $allowedOrigins = null): bool
    {
        $buffer = $conn->getBuffer();
        $headerEnd = strpos($buffer, "\r\n\r\n");

        if ($headerEnd === false) {
            if (strlen($buffer) > self::MAX_HEADER_BYTES) {
                self::sendHttpError($conn, 400, 'Handshake headers exceed maximum allowed size');
                $conn->close(1002, 'Handshake headers too large');
            }
            return false; // Wait for more data
        }

        $rawHeaders = substr($buffer, 0, $headerEnd);
        $conn->consumeBuffer($headerEnd + 4);

        $lines = explode("\r\n", $rawHeaders);
        $requestLine = array_shift($lines);
        $parts = explode(' ', (string)$requestLine);

        if (count($parts) < 2 || strtoupper($parts[0]) !== 'GET') {
            self::sendHttpError($conn, 400, 'Invalid HTTP Request Line');
            $conn->close(1002, 'Invalid HTTP method');
            return false;
        }

        // Parse path and query parameters
        $urlParts = parse_url($parts[1]);
        $path = $urlParts['path'] ?? '/';
        $query = [];
        if (!empty($urlParts['query'])) {
            parse_str($urlParts['query'], $query);
        }
        $conn->setQueryParams($query);
        $conn->set('path', $path);

        // Parse HTTP headers
        $headers = [];
        foreach ($lines as $line) {
            $colon = strpos($line, ':');
            if ($colon !== false) {
                $key = strtolower(trim(substr($line, 0, $colon)));
                $val = trim(substr($line, $colon + 1));
                $headers[$key] = $val;
            }
        }
        $conn->setHeaders($headers);

        // Validate Sec-WebSocket-Key
        $secKey = $headers['sec-websocket-key'] ?? null;
        if (!$secKey) {
            self::sendHttpError($conn, 400, 'Missing Sec-WebSocket-Key Header');
            $conn->close(1002, 'Missing Sec-WebSocket-Key');
            return false;
        }

        // Origin validation if configured
        if ($allowedOrigins !== null && !empty($allowedOrigins)) {
            $origin = $headers['origin'] ?? '';
            if (!in_array($origin, $allowedOrigins, true)) {
                self::sendHttpError($conn, 403, 'Forbidden Origin');
                $conn->close(1008, 'Origin not allowed');
                return false;
            }
        }

        // Compute Sec-WebSocket-Accept token
        $acceptKey = base64_encode(sha1($secKey . self::MAGIC_GUID, true));

        $response = "HTTP/1.1 101 Switching Protocols\r\n"
            . "Upgrade: websocket\r\n"
            . "Connection: Upgrade\r\n"
            . "Sec-WebSocket-Accept: {$acceptKey}\r\n"
            . "Server: LiteSocket/2.0\r\n\r\n";

        $socket = $conn->getSocket();
        if (is_resource($socket)) {
            @fwrite($socket, $response);
        }

        $conn->setHandshakeDone(true);
        return true;
    }

    private static function sendHttpError(Connection $conn, int $statusCode, string $statusText): void
    {
        $socket = $conn->getSocket();
        if (is_resource($socket)) {
            $response = "HTTP/1.1 {$statusCode} {$statusText}\r\n"
                . "Content-Type: text/plain\r\n"
                . "Connection: close\r\n\r\n"
                . $statusText;
            @fwrite($socket, $response);
        }
    }
}
