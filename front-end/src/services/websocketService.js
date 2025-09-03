class WebSocketService {
    constructor() {
        this.ws = null;
        this.messageHandlers = [];
        this.isConnecting = false;
    }

    connect() {
        // Prevent multiple simultaneous connection attempts
        if (this.ws?.readyState === WebSocket.OPEN || this.ws?.readyState === WebSocket.CONNECTING || this.isConnecting) {
            console.log('WebSocket connection already exists or is connecting');
            return;
        }

        this.isConnecting = true;

        // Use secure WebSocket if the page is served over HTTPS
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        let wsUrl;
        
        if (import.meta.env.DEV) {
            wsUrl = `${protocol}//localhost:8000/api/v1/ws`;
        } else {
            wsUrl = `${protocol}//${window.location.host}/api/v1/ws`;
        }
        
        this.ws = new WebSocket(wsUrl);

        this.ws.onmessage = (event) => {
            this.messageHandlers.forEach(handler => handler(event.data));
        };

        this.ws.onopen = () => {
            console.log('WebSocket connected');
            this.isConnecting = false;
        };

        this.ws.onclose = () => {
            console.log('WebSocket connection closed');
            this.isConnecting = false;
            // Attempt to reconnect after 5 seconds
            setTimeout(() => this.connect(), 5000);
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
            this.isConnecting = false;
        };
    }

    onMessage(handler) {
        this.messageHandlers.push(handler);
        return () => {
            this.messageHandlers = this.messageHandlers.filter(h => h !== handler);
        };
    }

    disconnect() {
        if (this.ws) {
            this.ws.close();
        }
    }
}

export const websocketService = new WebSocketService();
