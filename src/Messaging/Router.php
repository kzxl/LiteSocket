<?php

declare(strict_types=1);

namespace LiteSocket\Messaging;

use LiteSocket\Connection;

/**
 * Message routing dispatcher. Eliminates massive switch-case blocks in application code.
 */
class Router
{
    /** @var array<string, callable> */
    private array $routes = [];

    /** @var callable|null fn(Connection $conn, string $type, mixed $payload) */
    private $fallbackHandler = null;

    /**
     * Register a handler for a specific message type.
     *
     * @param string $type e.g. 'player_move', 'chat', 'order.create'
     * @param callable $handler fn(Connection $conn, mixed $payload)
     */
    public function route(string $type, callable $handler): self
    {
        $this->routes[$type] = $handler;
        return $this;
    }

    /**
     * Set a fallback handler for unrouted message types.
     */
    public function fallback(callable $handler): self
    {
        $this->fallbackHandler = $handler;
        return $this;
    }

    public function has(string $type): bool
    {
        return isset($this->routes[$type]);
    }

    /**
     * Dispatch a message to the registered route handler.
     *
     * @return bool True if a route or fallback was executed, false otherwise.
     */
    public function dispatch(Connection $conn, string $type, mixed $payload): bool
    {
        if (isset($this->routes[$type])) {
            ($this->routes[$type])($conn, $payload);
            return true;
        }

        if ($this->fallbackHandler !== null) {
            ($this->fallbackHandler)($conn, $type, $payload);
            return true;
        }

        return false;
    }

    /**
     * @return array<string, callable>
     */
    public function getRoutes(): array
    {
        return $this->routes;
    }
}
