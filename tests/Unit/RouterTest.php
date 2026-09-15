<?php

declare(strict_types=1);

namespace LiteSocket\Tests\Unit;

use LiteSocket\Connection;
use LiteSocket\Messaging\Router;
use PHPUnit\Framework\TestCase;

class RouterTest extends TestCase
{
    private function createMockConnection(): Connection
    {
        $domain = DIRECTORY_SEPARATOR === '\\' ? STREAM_PF_INET : STREAM_PF_UNIX;
        $pair = stream_socket_pair($domain, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        return new Connection('test_1', $pair[0]);
    }

    public function testRouteDispatch(): void
    {
        $router = new Router();
        $conn = $this->createMockConnection();

        $dispatched = false;
        $receivedPayload = null;

        $router->route('player_move', function (Connection $c, mixed $payload) use (&$dispatched, &$receivedPayload) {
            $dispatched = true;
            $receivedPayload = $payload;
        });

        $this->assertTrue($router->has('player_move'));
        $this->assertFalse($router->has('unknown_event'));

        $result = $router->dispatch($conn, 'player_move', ['x' => 10, 'y' => 20]);
        $this->assertTrue($result);
        $this->assertTrue($dispatched);
        $this->assertEquals(['x' => 10, 'y' => 20], $receivedPayload);
    }

    public function testFallbackHandler(): void
    {
        $router = new Router();
        $conn = $this->createMockConnection();

        $fallbackCalled = false;
        $router->fallback(function (Connection $c, string $type, mixed $payload) use (&$fallbackCalled) {
            $fallbackCalled = true;
        });

        $result = $router->dispatch($conn, 'non_existent', []);
        $this->assertTrue($result);
        $this->assertTrue($fallbackCalled);
    }
}
