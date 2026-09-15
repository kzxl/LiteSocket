# LiteSocket

[![Latest Version](https://img.shields.io/github/v/release/kzxl/LiteSocket?label=version&color=blue)](https://github.com/kzxl/LiteSocket/releases)
[![PHP Version](https://img.shields.io/badge/php-%3E%3D8.2-8892BF.svg)](https://php.net)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

An ultra-lightweight, zero-dependency, sovereign RFC 6455 WebSocket Server and Server-Sent Events (SSE) Streamer for **PHP 8.2+** with Room Pub/Sub and cross-platform TypeScript Client.

Part of the **LitePlatform** sovereign software suite (< 10MB RAM, zero 3rd-party vendor lock-in).

---

## 📦 Installation

### Option 1: Standard Composer (via Packagist)
```bash
composer require kzxl/lite-socket
```

### Option 2: Direct from Git Repository (VCS)
To pull directly from the official GitHub repository without waiting for Packagist synchronization, add the VCS repository to your project's `composer.json`:

```json
{
    "repositories": [
        {
            "type": "vcs",
            "url": "https://github.com/kzxl/LiteSocket.git"
        }
    ],
    "require": {
        "kzxl/lite-socket": "^1.1.0"
    }
}
```
Or configure via CLI:
```bash
composer config repositories.lite-socket vcs https://github.com/kzxl/LiteSocket.git
composer require kzxl/lite-socket:^1.1.0
```

### Option 3: Local Path Repository (Monorepo / Development)
For local development where changes should reflect immediately via symlink:
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

## ⚡ Key Features

- **Zero 3rd-Party Dependencies**: Pure PHP 8.2+ using native `stream_socket_server` and `stream_select`. No C-extensions (`ext-swoole`), no Node.js sidecars.
- **RFC 6455 Compliant**: Handshake calculation with SHA1 base64 sec-key, binary/text framing, masking/unmasking, and ping/pong heartbeats.
- **Room / Channel Pub-Sub**: Shard connections into game zones (`CH-1_SanctuaryHaven`), market tickers, or private chat channels.
- **Dual Transport (VPS + Shared Hosting)**:
  - **WebSocket Daemon**: Full-duplex TCP for VPS and containers (`php bin/socket.php`).
  - **Server-Sent Events (SSE) Fallback**: For cPanel or environments where custom TCP ports are blocked.
- **High-Performance Event Loop**: Non-blocking `stream_select` with configurable tick interval (e.g., 20 TPS for authoritative game tick).
- **TypeScript SDK Included**: Ready-to-use client (`LiteSocketClient.ts`) with exponential backoff auto-reconnect and room listeners.

---

## 🚀 Quick Start (WebSocket Server)

Create `bin/socket.php`:

```php
<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use LiteSocket\WebSocketServer;
use LiteSocket\Connection;

$server = new WebSocketServer(host: '0.0.0.0', port: 8088);

// Connection event
$server->on('connect', function (Connection $conn) use ($server) {
    echo "Client connected: {$conn->getId()} from {$conn->getRemoteAddress()}\n";
    $conn->sendJson(['type' => 'welcome', 'connId' => $conn->getId()]);
});

// Message event
$server->on('message', function (Connection $conn, string $raw, ?array $json) use ($server) {
    if (!$json) return;

    switch ($json['type'] ?? '') {
        case 'player_move':
            // Broadcast player position to everyone in the same room
            $room = $conn->getRooms()[0] ?? 'general';
            $server->broadcastToRoom($room, [
                'type' => 'player_moved',
                'id'   => $conn->getId(),
                'x'    => $json['x'],
                'y'    => $json['y']
            ], $conn);
            break;

        case 'chat':
            $server->broadcastAll([
                'type'    => 'chat_broadcast',
                'sender'  => $conn->getId(),
                'message' => $json['text']
            ]);
            break;
    }
});

// Run non-blocking event loop (20 TPS tick rate)
$server->run(tickInterval: 0.05);
```

Run daemon:
```bash
php bin/socket.php
```

---

## 🌐 TypeScript Client Usage

```typescript
import { LiteSocketClient } from './LiteSocketClient';

const socket = new LiteSocketClient({
  url: 'ws://localhost:8088',
  autoJoinRooms: ['CH-1_SanctuaryHaven'],
  reconnect: true,
  reconnectInterval: 1500,
});

socket.on('player_moved', (data) => {
  console.log(`Player ${data.id} moved to (${data.x}, ${data.y})`);
});

// Send player input
socket.send('player_move', { x: 2050, y: 1980 });
```

---

## 🧪 Testing

```bash
composer install
vendor/bin/phpunit
```

---

## 📄 License

MIT License — Copyright (c) 2026 Phong Vo (kzxl).
