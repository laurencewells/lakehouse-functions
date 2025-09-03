import logging as L
from typing import List
from fastapi import WebSocket, WebSocketDisconnect

class ConnectionManager:
    """
    Manages WebSocket connections and handles broadcasting messages to connected clients.
    
    This class provides functionality to:
    - Track active WebSocket connections
    - Handle client connections and disconnections
    - Broadcast messages to all connected clients
    - Handle connection errors gracefully
    
    Attributes:
        active_connections (List[WebSocket]): List of currently active WebSocket connections
    """
    def __init__(self):
        """Initialize the connection manager with an empty list of connections."""
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        """
        Accept a new WebSocket connection and add it to active connections.
        
        Args:
            websocket (WebSocket): The WebSocket connection to accept
            
        Raises:
            WebSocketException: If the connection cannot be accepted
        """
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        """
        Remove a WebSocket connection from active connections.
        
        Args:
            websocket (WebSocket): The WebSocket connection to remove
            
        Note:
            This method is called both for normal disconnections and error cases
        """
        self.active_connections.remove(websocket)

    async def broadcast_log(self, message: str) -> None:
        """
        Send a log message to all connected clients.
        
        This method attempts to send the message to all active connections,
        handling disconnections and errors gracefully by removing problematic
        connections from the active list.
        
        Args:
            message (str): The message to broadcast to all clients
            
        Note:
            Failed connections are automatically removed from active_connections
        """
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except WebSocketDisconnect:
                self.disconnect(connection)
            except Exception as e:
                L.error(f"Error sending message to websocket: {str(e)}")
                self.disconnect(connection)

# Create a global connection manager instance
manager = ConnectionManager()
