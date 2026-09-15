<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use LiteSocket\Connection\WriteQueue;

echo "=== LiteSocket 2.x Benchmark: WriteQueue & Flush ===\n";

$domain = DIRECTORY_SEPARATOR === '\\' ? STREAM_PF_INET : STREAM_PF_UNIX;
$pair = stream_socket_pair($domain, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
[$sockA, $sockB] = $pair;
stream_set_blocking($sockA, false);
stream_set_blocking($sockB, false);

$iterations = 50000;
$chunk = str_repeat('X', 512); // 512 bytes packet
$queue = new WriteQueue(10 * 1024 * 1024); // 10MB capacity

$start = microtime(true);
for ($i = 0; $i < $iterations; $i++) {
    $queue->enqueue($chunk);
    if ($queue->length() > 65536) {
        $queue->flush($sockA);
        // drain receiver so buffer doesn't fill OS TCP pipe
        while (fread($sockB, 65536) !== '') {
            break;
        }
    }
}
$time = microtime(true) - $start;
$throughputMB = ($iterations * 512) / (1024 * 1024) / $time;

echo sprintf("  Processed: %d packets (512B) in %.3f s\n", $iterations, $time);
echo sprintf("  Throughput: %.2f MB/sec\n", $throughputMB);
echo sprintf("  Peak Memory: %.2f MB\n", memory_get_peak_usage(true) / 1024 / 1024);
echo "====================================================\n";

fclose($sockA);
fclose($sockB);
