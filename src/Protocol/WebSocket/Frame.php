<?php

declare(strict_types=1);

namespace LiteSocket\Protocol\WebSocket;

/**
 * RFC 6455 WebSocket Frame encoder and decoder.
 */
class Frame
{
    public const OPCODE_CONTINUATION = 0x0;
    public const OPCODE_TEXT         = 0x1;
    public const OPCODE_BINARY       = 0x2;
    public const OPCODE_CLOSE        = 0x8;
    public const OPCODE_PING         = 0x9;
    public const OPCODE_PONG         = 0xA;

    /**
     * Encode a text payload into an unmasked RFC 6455 WebSocket frame (Server -> Client).
     */
    public static function encodeText(string $payload): string
    {
        return self::encode($payload, self::OPCODE_TEXT);
    }

    /**
     * Encode a binary payload into an unmasked frame.
     */
    public static function encodeBinary(string $payload): string
    {
        return self::encode($payload, self::OPCODE_BINARY);
    }

    /**
     * Encode a Ping frame.
     */
    public static function encodePing(string $payload = ''): string
    {
        return self::encode($payload, self::OPCODE_PING);
    }

    /**
     * Encode a Pong frame.
     */
    public static function encodePong(string $payload = ''): string
    {
        return self::encode($payload, self::OPCODE_PONG);
    }

    /**
     * Encode a Close frame with optional status code (default 1000 Normal Closure).
     */
    public static function encodeClose(int $statusCode = 1000, string $reason = ''): string
    {
        $payload = pack('n', $statusCode) . $reason;
        return self::encode($payload, self::OPCODE_CLOSE);
    }

    /**
     * Encode a payload into an RFC 6455 frame.
     */
    public static function encode(string $payload, int $opcode): string
    {
        $firstByte = 0x80 | ($opcode & 0x0F); // FIN bit set to 1
        $length = strlen($payload);

        if ($length <= 125) {
            $header = pack('CC', $firstByte, $length);
        } elseif ($length <= 65535) {
            $header = pack('CCn', $firstByte, 126, $length);
        } else {
            // 64-bit length (pack 'J' unsigned 64-bit big-endian)
            $header = pack('CCJ', $firstByte, 127, $length);
        }

        return $header . $payload;
    }

    /**
     * Decode a raw buffer from socket into a frame array, or null if incomplete.
     *
     * @return array{fin: bool, opcode: int, payload: string, bytesConsumed: int}|null
     */
    public static function decode(string $buffer): ?array
    {
        $bufferLength = strlen($buffer);
        if ($bufferLength < 2) {
            return null; // Incomplete header
        }

        $byte1 = ord($buffer[0]);
        $byte2 = ord($buffer[1]);

        $fin = ($byte1 & 0x80) === 0x80;
        $opcode = $byte1 & 0x0F;
        $isMasked = ($byte2 & 0x80) === 0x80;
        $payloadLength = $byte2 & 0x7F;

        $offset = 2;

        if ($payloadLength === 126) {
            if ($bufferLength < $offset + 2) {
                return null;
            }
            $payloadLength = unpack('n', substr($buffer, $offset, 2))[1];
            $offset += 2;
        } elseif ($payloadLength === 127) {
            if ($bufferLength < $offset + 8) {
                return null;
            }
            $payloadLength = unpack('J', substr($buffer, $offset, 8))[1];
            $offset += 8;
        }

        $maskingKey = '';
        if ($isMasked) {
            if ($bufferLength < $offset + 4) {
                return null;
            }
            $maskingKey = substr($buffer, $offset, 4);
            $offset += 4;
        }

        if ($bufferLength < $offset + $payloadLength) {
            return null; // Partial payload, wait for more data
        }

        $rawPayload = substr($buffer, $offset, $payloadLength);
        $payload = '';

        if ($isMasked) {
            $maskLength = 4;
            $payloadLen = strlen($rawPayload);
            $unmasked = '';
            for ($i = 0; $i < $payloadLen; $i++) {
                $unmasked .= $rawPayload[$i] ^ $maskingKey[$i % $maskLength];
            }
            $payload = $unmasked;
        } else {
            $payload = $rawPayload;
        }

        return [
            'fin'           => $fin,
            'opcode'        => $opcode,
            'payload'       => $payload,
            'bytesConsumed' => $offset + $payloadLength,
        ];
    }
}
