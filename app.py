import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import logging as L
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from triggers.unity_table_listener_function import unity_table_listener
from triggers.execute import execute_action
from common.websocket_manager import manager

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan context manager that handles application startup and shutdown.
    
    This function sets up all triggers (HTTP, timer, and Unity table) during application startup.
    It uses the asynccontextmanager to properly handle async setup and teardown.
    
    Args:
        app (FastAPI): The FastAPI application instance
        
    Yields:
        None: Control is yielded back to FastAPI after setup is complete
    """
    await setup_triggers()
    yield

app = FastAPI(lifespan=lifespan)


# Load function definitions from YAML file
def load_function_definitions(file_path: str) -> dict:
    """
    Load and parse function definitions from a YAML configuration file.
    
    Args:
        file_path (str): Path to the YAML configuration file containing function definitions
        
    Returns:
        dict: Parsed YAML content containing function configurations
        
    Raises:
        yaml.YAMLError: If the YAML file is malformed
        FileNotFoundError: If the specified file does not exist
    """
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

# Example usage
function_definitions = load_function_definitions('app.yaml')

# Initialize scheduler
scheduler = AsyncIOScheduler()

        
def check_function_exists(function: str) -> bool:
    """
    Check if a function implementation file exists in the functions directory.
    
    Args:
        function (str): Name of the function to check (without '_function.py' suffix)
        
    Returns:
        bool: True if the function implementation file exists, False otherwise
        
    Note:
        This function checks for files in the format 'functions/{function}_function.py'
    """
    file = f"functions/{function}_function.py"
    return os.path.exists(file)

# Set up triggers
async def setup_triggers() -> None:
    """
    Set up all function triggers defined in the YAML configuration.
    
    This async function processes each function definition and sets up the appropriate trigger
    using the TriggerHandlerFactory to create and configure the correct handler type.
    
    The function broadcasts progress messages via WebSocket during setup.
    
    Raises:
        Exception: If there's an error setting up any trigger. Errors are logged
                  and broadcast via WebSocket, but don't stop other triggers from being set up.
    """
    from triggers.handlers import TriggerHandlerFactory
    
    await manager.broadcast_log("Starting trigger setup...")
    
    for function_instance in function_definitions['functions']:
        function_name = function_instance['name']
        
        # Validate function exists
        if not check_function_exists(function_name):
            error_msg = f"Function {function_name} does not exist"
            L.error(error_msg)
            await manager.broadcast_log(error_msg)
            continue
            
        try:
            # Create appropriate handler
            handler = TriggerHandlerFactory.create_handler(
                function_name=function_name,
                trigger_config=function_instance['trigger'],
                app=app,
                scheduler=scheduler
            )
            
            # Set up the trigger
            await handler.setup()
            
        except Exception as e:
            error_msg = f"Error setting up trigger for {function_name}: {str(e)}"
            L.error(error_msg)
            await manager.broadcast_log(error_msg)

# Start the scheduler
scheduler.start()


@app.get("/api/v1/health")
async def health_check() -> dict:
    """
    Health check endpoint to verify the application is running.
    
    Returns:
        dict: A dictionary containing the status of the application
              {"status": "healthy"}
    """
    return {"status": "healthy"}

# endpoint to list the functions
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
    functions = function_definitions['functions']
    for function in functions:
        function['code'] = open(f"functions/{function['name']}_function.py", "r").read()
    return {"functions": functions}

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
        


if os.environ.get("ENV") == "DEV":
    print("Local mode")
    origins = [
        "http://127.0.0.1:5173",
        "http://127.0.0.1:8000",
        "http://localhost:8000",
        "http://localhost:5173"
    ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"]
    )
else:
    try:
        target_dir = "front-end/dist"
        app.mount("/", StaticFiles(directory=target_dir, html=True), name="site")
    except Exception as e:
        print(f'ERROR - static not found: {str(e)}')
