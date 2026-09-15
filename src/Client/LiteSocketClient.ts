/**
 * LiteSocket Universal Real-Time Client (Browser, Node, Three.js)
 * Supports WebSocket RFC 6455 with automatic reconnection, heartbeat, and Room Pub/Sub.
 */
export type SocketStatus = 'connecting' | 'connected' | 'disconnected';

export interface LiteSocketOptions {
  url: string;
  reconnect?: boolean;
  reconnectInterval?: number;
  maxReconnectInterval?: number;
  heartbeatInterval?: number;
  token?: string;
  autoJoinRooms?: string[];
}

export class LiteSocketClient {
  private url: string;
  private ws: WebSocket | null = null;
  private status: SocketStatus = 'disconnected';
  private reconnectEnabled: boolean;
  private reconnectInterval: number;
  private maxReconnectInterval: number;
  private currentReconnectDelay: number;
  private reconnectTimer: any = null;
  private heartbeatInterval: number;
  private heartbeatTimer: any = null;
  private token?: string;
  private subscribedRooms: Set<string> = new Set();
  private eventHandlers: Map<string, Array<(data: any) => void>> = new Map();
  private statusListeners: Array<(status: SocketStatus) => void> = [];

  constructor(options: LiteSocketOptions) {
    this.url = options.url;
    this.reconnectEnabled = options.reconnect ?? true;
    this.reconnectInterval = options.reconnectInterval ?? 1000;
    this.maxReconnectInterval = options.maxReconnectInterval ?? 10000;
    this.currentReconnectDelay = this.reconnectInterval;
    this.heartbeatInterval = options.heartbeatInterval ?? 25000;
    this.token = options.token;

    if (options.autoJoinRooms) {
      options.autoJoinRooms.forEach((r) => this.subscribedRooms.add(r));
    }
  }

  public getStatus(): SocketStatus {
    return this.status;
  }

  public isConnected(): boolean {
    return this.status === 'connected';
  }

  /**
   * Connect to WebSocket server.
   */
  public connect(): void {
    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    this.setStatus('connecting');

    // Build URL with query params
    const wsUrl = new URL(this.url);
    if (this.token) {
      wsUrl.searchParams.set('token', this.token);
    }

    try {
      this.ws = new WebSocket(wsUrl.toString());

      this.ws.onopen = () => {
        this.setStatus('connected');
        this.currentReconnectDelay = this.reconnectInterval;
        this.startHeartbeat();

        // Resubscribe all registered rooms
        for (const room of this.subscribedRooms) {
          this.sendRaw({ type: 'subscribe', room });
        }

        this.emit('_open', null);
      };

      this.ws.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);
          if (message.type === 'pong') {
            return; // Heartbeat response
          }

          if (message.event) {
            this.emit(message.event, message.data ?? message);
          } else if (message.type) {
            this.emit(message.type, message);
          } else {
            this.emit('message', message);
          }
        } catch {
          this.emit('raw_message', event.data);
        }
      };

      this.ws.onclose = () => {
        this.cleanup();
        this.setStatus('disconnected');
        this.emit('_close', null);
        this.scheduleReconnect();
      };

      this.ws.onerror = (err) => {
        this.emit('_error', err);
      };
    } catch (err) {
      this.setStatus('disconnected');
      this.scheduleReconnect();
    }
  }

  /**
   * Disconnect and stop reconnection attempts.
   */
  public disconnect(): void {
    this.reconnectEnabled = false;
    this.cleanup();
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.setStatus('disconnected');
  }

  /**
   * Subscribe to a room / channel.
   */
  public subscribe(room: string): void {
    this.subscribedRooms.add(room);
    if (this.isConnected()) {
      this.sendRaw({ type: 'subscribe', room });
    }
  }

  /**
   * Unsubscribe from a room.
   */
  public unsubscribe(room: string): void {
    this.subscribedRooms.delete(room);
    if (this.isConnected()) {
      this.sendRaw({ type: 'unsubscribe', room });
    }
  }

  /**
   * Publish an event to a room.
   */
  public publish(room: string, event: string, data: any): void {
    this.sendRaw({
      type: 'publish',
      room,
      event,
      data,
    });
  }

  /**
   * Send a typed message payload to server.
   */
  public send(type: string, data: any = {}): void {
    this.sendRaw({ type, ...data });
  }

  /**
   * Register an event handler.
   */
  public on(event: string, handler: (data: any) => void): this {
    if (!this.eventHandlers.has(event)) {
      this.eventHandlers.set(event, []);
    }
    this.eventHandlers.get(event)!.push(handler);
    return this;
  }

  /**
   * Remove an event handler.
   */
  public off(event: string, handler?: (data: any) => void): this {
    if (!handler) {
      this.eventHandlers.delete(event);
    } else {
      const handlers = this.eventHandlers.get(event);
      if (handlers) {
        this.eventHandlers.set(
          event,
          handlers.filter((h) => h !== handler)
        );
      }
    }
    return this;
  }

  /**
   * Listen for connection status changes.
   */
  public onStatusChange(callback: (status: SocketStatus) => void): void {
    this.statusListeners.push(callback);
    callback(this.status);
  }

  // --- Internals ---

  private setStatus(status: SocketStatus): void {
    if (this.status !== status) {
      this.status = status;
      this.statusListeners.forEach((cb) => cb(status));
    }
  }

  private sendRaw(data: any): boolean {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data));
      return true;
    }
    return false;
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.heartbeatTimer = setInterval(() => {
      this.sendRaw({ type: 'ping' });
    }, this.heartbeatInterval);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  private scheduleReconnect(): void {
    if (!this.reconnectEnabled || this.reconnectTimer) {
      return;
    }

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.currentReconnectDelay = Math.min(this.currentReconnectDelay * 1.5, this.maxReconnectInterval);
      this.connect();
    }, this.currentReconnectDelay);
  }

  private cleanup(): void {
    this.stopHeartbeat();
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  private emit(event: string, data: any): void {
    const handlers = this.eventHandlers.get(event);
    if (handlers) {
      handlers.forEach((h) => {
        try {
          h(data);
        } catch (e) {
          console.error(`[LiteSocket] Error in event '${event}' handler:`, e);
        }
      });
    }
  }
}
