# LiteSocket 2.x

[![Latest Version](https://img.shields.io/github/v/release/kzxl/LiteSocket?label=version&color=blue)](https://github.com/kzxl/LiteSocket/releases)
[![PHP Version](https://img.shields.io/badge/php-%3E%3D8.2-8892BF.svg)](https://php.net)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Tests: Passing](https://img.shields.io/badge/tests-19%20passed-brightgreen.svg)](tests/)

An ultra-high-performance, zero-dependency RFC 6455 WebSocket Server and Server-Sent Events (SSE) Streamer for **PHP 8.2+** featuring 5 modular cores, non-blocking WriteQueue with Backpressure, Router dispatcher, and independent EventLoop.

Part of the **LitePlatform** sovereign software suite (< 10MB RAM, zero 3rd-party vendor lock-in).

---

## ⚡ Architectural Highlights (LiteSocket 2.x)

LiteSocket 2.x transitions from a single God-Class server into 5 decoupled cores:

```
LiteSocket/
├── Runtime/      # StreamSelectLoop: multiplexed non-blocking I/O + independent microtime Timer Queue
├── Transport/    # StreamSocketTransport: TCP bind, listen (SO_REUSEPORT), accept
├── Protocol/     # RFC 6455 Handshake, Origin validation, Streaming FrameParser
├── Connection/   # ConnectionPool (O(1)), ReadBuffer (OOM Guard), WriteQueue (Backpressure)
└── Messaging/    # Router dispatcher, lightweight in-memory RoomManager, PubSub contract
```

### Key Enhancements in 2.x
- **Independent EventLoop**: Network I/O is completely decoupled from game/application tick. Network events trigger with zero latency, while game ticks execute via recurring timers.
- **WriteQueue & Partial-Write Protection**: Outgoing data is buffered in RAM. Non-blocking `@fwrite()` prevents TCP stream corruption on slow or lagging clients.
- **Backpressure & Drop Policies**: Configurable buffer limit (`maxWriteBuffer`, default 4MB). Droppable packets (e.g. player movement / telemetry) are discarded gracefully when buffers are full.
- **Message Router**: Expressive routing (`$server->route('chat', ...)`), removing massive application `switch-case` blocks.
- **100% Backward Compatible**: Existing applications run seamlessly without code changes.

---

## 📦 Installation

### Option 1: Standard Composer (via Packagist)
```bash
composer require kzxl/lite-socket
```

### Option 2: Local Path Repository (Monorepo)
```json
{
    "repositories": [
        {
            "type": "path",
            "url": "../libs/LiteSocket",
            "options": {
                "symlink": true
            }
        }
    ],
    "require": {
        "kzxl/lite-socket": "@dev"
    }
}
```

---

## 🚀 Quick Start

### Basic Server with Routing

```php
<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use LiteSocket\WebSocketServer;
use LiteSocket\Connection;

$server = new WebSocketServer(host: '0.0.0.0', port: 8088);

// 1. Connection Event
$server->on('connect', function (Connection $conn) {
    echo "Client connected: {$conn->id()} from {$conn->ip()}\n";
    $conn->sendJson(['type' => 'welcome', 'connId' => $conn->id()]);
});

// 2. Expressive Message Routing (LiteSocket 2.x)
$server->route('player_move', function (Connection $conn, array $data) use ($server) {
    $room = $conn->rooms()[0] ?? 'general';
    // Broadcast position update to all other players in this room (droppable under backpressure)
    $server->broadcastToRoom($room, [
        'type' => 'player_moved',
        'id'   => $conn->id(),
        'x'    => $data['x'] ?? 0,
        'y'    => $data['y'] ?? 0,
    ], exclude: $conn, droppable: true);
});

$server->route('chat', function (Connection $conn, array $data) use ($server) {
    $server->broadcastAll([
        'type'    => 'chat_message',
        'sender'  => $conn->id(),
        'message' => $data['text'] ?? '',
    ]);
});

// 3. Disconnect Event
$server->on('close', function (Connection $conn) {
    echo "Client disconnected: {$conn->id()}\n";
});

// Run server with authoritative game tick (20 TPS)
$server->run(tickInterval: 0.05);
```

---

## 📊 Performance Benchmarks

Measured on PHP 8.2.12 (Single-core CPU):

| Benchmark Component | Throughput | Peak RAM |
| :--- | :--- | :--- |
| **WebSocket Frame Encode** | **3,529,578 frames / sec** | `4.00 MB` |
| **WebSocket Frame Decode** | **2,277,773 frames / sec** | `4.00 MB` |
| **WriteQueue + Flush I/O** | **650.45 MB / sec** (50,000 pkts in 0.038s) | `4.00 MB` |
| **Test Suite Run (19 tests)**| **0.153 seconds** | `8.00 MB` |

Run benchmarks locally:
```bash
php bench/frame_bench.php
php bench/write_queue_bench.php
```

---

## 🧪 Testing

```bash
composer test
# Or directly:
vendor/bin/phpunit
```

---

## 📄 License

MIT License — Copyright (c) 2026 Phong Vo (kzxl).
