<?php

declare(strict_types=1);

namespace LiteSocket\Messaging\PubSub;

use InvalidArgumentException;

/**
 * Ultra-lightweight streaming RESP (REdis Serialization Protocol) Parser & Encoder.
 * Zero external dependencies. Pure PHP 8.2+.
 */
class RespParser
{
    /**
     * Encode command arguments into RESP array format.
     *
     * @param array<int, string|int|float> $args
     */
    public static function encode(array $args): string
    {
        $out = '*' . count($args) . "\r\n";
        foreach ($args as $arg) {
            $str = (string) $arg;
            $out .= '$' . strlen($str) . "\r\n" . $str . "\r\n";
        }
        return $out;
    }

    /**
     * Try parsing one complete RESP frame from buffer.
     * Modifies $buffer by reference, removing consumed bytes.
     *
     * Returns mixed:
     * - array/string/int/null/bool when frame is complete.
     * - null when frame is incomplete (need more data in buffer).
     */
    public static function parse(string &$buffer): mixed
    {
        if ($buffer === '') {
            return null;
        }

        $type = $buffer[0];
        $crlfPos = strpos($buffer, "\r\n");
        if ($crlfPos === false) {
            return null; // Need more data for line
        }

        $line = substr($buffer, 1, $crlfPos - 1);

        switch ($type) {
            case '+': // Simple string
                $buffer = substr($buffer, $crlfPos + 2);
                return $line;

            case '-': // Error
                $buffer = substr($buffer, $crlfPos + 2);
                return new InvalidArgumentException($line);

            case ':': // Integer
                $buffer = substr($buffer, $crlfPos + 2);
                return (int) $line;

            case '$': // Bulk string
                $length = (int) $line;
                if ($length === -1) {
                    $buffer = substr($buffer, $crlfPos + 2);
                    return null; // Null bulk string
                }

                $dataStart = $crlfPos + 2;
                $totalNeeded = $dataStart + $length + 2; // data + \r\n
                if (strlen($buffer) < $totalNeeded) {
                    return null; // Incomplete bulk string data
                }

                $data = substr($buffer, $dataStart, $length);
                $buffer = substr($buffer, $totalNeeded);
                return $data;

            case '*': // Array
                $count = (int) $line;
                if ($count === -1) {
                    $buffer = substr($buffer, $crlfPos + 2);
                    return null; // Null array
                }

                if ($count === 0) {
                    $buffer = substr($buffer, $crlfPos + 2);
                    return [];
                }

                // Temporary buffer to attempt parsing all elements
                $tempBuffer = substr($buffer, $crlfPos + 2);
                $elements = [];

                for ($i = 0; $i < $count; $i++) {
                    if ($tempBuffer === '') {
                        return null; // Incomplete array elements
                    }

                    $element = self::parse($tempBuffer);
                    if ($element === null && $tempBuffer !== '' && $tempBuffer[0] === '$' && strpos($tempBuffer, "\r\n") !== false) {
                        // Incomplete bulk string inside array
                        return null;
                    }
                    if ($element === null && $tempBuffer === '') {
                        return null;
                    }

                    $elements[] = $element;
                }

                // If all elements were successfully parsed, update actual buffer
                $buffer = $tempBuffer;
                return $elements;

            default:
                // Unknown protocol prefix, discard line to avoid infinite loop
                $buffer = substr($buffer, $crlfPos + 2);
                return null;
        }
    }
}
