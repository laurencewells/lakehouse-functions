"""
Main application entry point for the serverless function platform.

This module creates and configures the FastAPI application using the
application factory pattern for clean separation of concerns.
"""

import logging as L
from fastapi import WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from common.app_factory import app_factory
from common.websocket_manager import manager
from common.config import is_development, get_cors_origins, get_static_files_directory
from triggers.execute import execute_action

# Create the FastAPI application
app = app_factory.create_app()


@app.get("/api/v1/health")
async def health_check() -> dict:
    """
    Health check endpoint to verify the application is running.
    
    Returns:
        dict: A dictionary containing the status of the application
              {"status": "healthy"}
    """
    return {"status": "healthy"}


@app.get("/api/v1/functions")
async def list_functions() -> dict:
    """
    Endpoint to list all configured functions and their source code.
    
    This endpoint returns all functions defined in the YAML configuration
    along with their implementation code. This is useful for debugging
    and monitoring purposes.
    
    Returns:
        dict: A dictionary containing a list of functions with their configurations
              and source code {"functions": [...]}
              
    Note:
        Each function object includes:
        - Original configuration from YAML
        - Source code from the implementation file
    """
    functions = app_factory.config.get('functions', []).copy()  # Create a copy to avoid modifying original
    for function in functions:
        try:
            with open(f"functions/{function['name']}_function.py", "r") as f:
                function['code'] = f.read()
        except FileNotFoundError:
            function['code'] = "# Function file not found"
        except Exception as e:
            function['code'] = f"# Error reading function file: {str(e)}"
    return {"functions": functions}

@app.post("/api/v1/functions/execute")
async def execute_function_adhoc(payload: dict) -> dict:
    """
    Ad-hoc function execution endpoint.

    This endpoint allows for the execution of a function by name.
    The function name is passed in the request body as JSON.

    Args:
        payload (dict): The request body containing the function name.

    Returns:
        dict: The result of the function execution.
    """
    function_name = payload.get("function_name")
    print(function_name)
    if not function_name:
        return {"error": "Missing 'function_name' in request body"}
    await manager.broadcast_log("Executing ad-hoc function: " + function_name)
    result = await execute_action(function_name)
    
    # Broadcast appropriate message based on execution result
    if result.get("status") == "success":
        await manager.broadcast_log(f"Successfully executed function: {function_name}")
    elif result.get("status") == "error":
        await manager.broadcast_log(f"Error executing function {function_name}: {result.get('message', 'Unknown error')}")
    else:
        # Handle legacy format or unexpected result structure
        await manager.broadcast_log(f"Function {function_name} executed with result: {str(result)}")
    
    return result


@app.websocket("/api/v1/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """
    WebSocket endpoint for real-time communication with clients.
    
    This endpoint handles:
    - Client connections and disconnections
    - Real-time message broadcasting
    - System logs and status updates
    
    Args:
        websocket (WebSocket): The WebSocket connection instance
        
    Note:
        The connection is managed by the WebSocket manager which handles:
        - Connection pooling
        - Broadcasting messages to all connected clients
        - Clean disconnection handling
        
    Raises:
        WebSocketDisconnect: When client disconnects (handled gracefully)
        Exception: Other WebSocket errors (logged and connection cleaned up)
    """
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            # Echo the message back to the sender
            await websocket.send_text(f"Message received: {data}")
    except WebSocketDisconnect:
        L.info("Client disconnected")
        manager.disconnect(websocket)
    except Exception as e:
        L.error(f"WebSocket error: {str(e)}")
        manager.disconnect(websocket)


# Log available routes for debugging
L.info("Available routes:")
for route in app.routes:
    L.info(f"  - {route.path} [{','.join(route.methods if hasattr(route, 'methods') else [])}]")
    
# Configure environment-specific settings
if is_development():
    L.info("Development mode - enabling CORS")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=get_cors_origins(),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"]
    )
else:
    # Configure static files for production
    try:
        static_dir = get_static_files_directory()
        app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
        L.info(f"Production mode - serving static files from {static_dir}")
    except Exception as e:
        L.error(f"ERROR - static files not found: {str(e)}")
