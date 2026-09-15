<?php

declare(strict_types=1);

namespace LiteSocket\Tests\Integration;

use LiteSocket\Connection;
use LiteSocket\Protocol\WebSocket\Frame;
use LiteSocket\Protocol\WebSocket\Handshake;
use LiteSocket\WebSocketServer;
use PHPUnit\Framework\TestCase;

class WebSocketServerTest extends TestCase
{
    public function testServerInitializationAndCoordinatingCores(): void
    {
        $server = new WebSocketServer('127.0.0.1', 18088);

        $this->assertNotNull($server->getLoop());
        $this->assertNotNull($server->getTransport());
        $this->assertNotNull($server->getRoomManager());
        $this->assertNotNull($server->getRouter());
        $this->assertNotNull($server->getPubSub());
        $this->assertEquals(0, $server->getConnectionCount());
    }

    public function testRfc6455HandshakeVector(): void
    {
        $domain = DIRECTORY_SEPARATOR === '\\' ? STREAM_PF_INET : STREAM_PF_UNIX;
        $pair = stream_socket_pair($domain, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        [$clientSock, $serverSock] = $pair;
        stream_set_blocking($clientSock, false);
        stream_set_blocking($serverSock, false);

        $conn = new Connection('client_1', $serverSock);

        // Standard RFC 6455 Test Vector: Key = dGhlIHNhbXBsZSBub25jZQ==
        // Expected Accept = s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
        $request = "GET /chat?room=general HTTP/1.1\r\n"
            . "Host: server.example.com\r\n"
            . "Upgrade: websocket\r\n"
            . "Connection: Upgrade\r\n"
            . "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
            . "Sec-WebSocket-Version: 13\r\n\r\n";

        $conn->appendBuffer($request);

        $success = Handshake::handle($conn);
        $this->assertTrue($success);
        $this->assertTrue($conn->isOpen());
        $this->assertEquals('general', $conn->getQueryParam('room'));

        // Read response on client side
        $response = fread($clientSock, 4096);
        $this->assertStringContainsString('HTTP/1.1 101 Switching Protocols', $response);
        $this->assertStringContainsString('Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=', $response);

        fclose($clientSock);
        $conn->close();
    }

    public function testRoomBroadcastIntegration(): void
    {
        $domain = DIRECTORY_SEPARATOR === '\\' ? STREAM_PF_INET : STREAM_PF_UNIX;
        $pair1 = stream_socket_pair($domain, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        $pair2 = stream_socket_pair($domain, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);

        $conn1 = new Connection('c_1', $pair1[1]);
        $conn2 = new Connection('c_2', $pair2[1]);
        $conn1->setHandshakeDone(true);
        $conn2->setHandshakeDone(true);

        $server = new WebSocketServer();
        $rooms = $server->getRoomManager();

        $rooms->join('vip_lounge', $conn1);
        $rooms->join('vip_lounge', $conn2);

        $sentCount = $server->broadcastToRoom('vip_lounge', ['type' => 'announcement', 'text' => 'Welcome VIPs'], $conn1);
        $this->assertEquals(1, $sentCount); // excluded conn1

        // Verify conn2 client received the encoded text frame
        $raw = fread($pair2[0], 4096);
        $frame = Frame::decode($raw);
        $this->assertNotNull($frame);
        $this->assertEquals(Frame::OPCODE_TEXT, $frame['opcode']);
        $this->assertStringContainsString('Welcome VIPs', $frame['payload']);

        fclose($pair1[0]);
        fclose($pair2[0]);
        $conn1->close();
        $conn2->close();
    }
}
