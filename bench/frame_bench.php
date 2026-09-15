<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use LiteSocket\Protocol\WebSocket\Frame;

echo "=== LiteSocket 2.x Benchmark: Frame Encode/Decode ===\n";

$iterations = 200000;
$payload = json_encode([
    'type'      => 'player_move',
    'id'        => 'c_9999',
    'x'         => 2050.45,
    'y'         => 1980.12,
    'timestamp' => microtime(true),
]);

// 1. Benchmark Encode
$start = microtime(true);
for ($i = 0; $i < $iterations; $i++) {
    $encoded = Frame::encodeText($payload);
}
$encodeTime = microtime(true) - $start;
$encodeOps = (int)($iterations / $encodeTime);

echo sprintf("  Encode: %d frames in %.3f s -> %s frames/sec\n", $iterations, $encodeTime, number_format($encodeOps));

// 2. Benchmark Decode
$encodedFrame = Frame::encodeText($payload);
$start = microtime(true);
for ($i = 0; $i < $iterations; $i++) {
    $decoded = Frame::decode($encodedFrame);
}
$decodeTime = microtime(true) - $start;
$decodeOps = (int)($iterations / $decodeTime);

echo sprintf("  Decode: %d frames in %.3f s -> %s frames/sec\n", $iterations, $decodeTime, number_format($decodeOps));
echo sprintf("  Peak Memory: %.2f MB\n", memory_get_peak_usage(true) / 1024 / 1024);
echo "=====================================================\n";
