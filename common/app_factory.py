"""
Application factory for creating and configuring the FastAPI application.

This module provides a clean separation between application creation,
configuration loading, and setup logic.
"""

import logging as L
import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from common.config import load_config, get_functions_by_trigger_type
from common.websocket_manager import manager


class ServerlessAppFactory:
    """
    Factory class for creating and configuring the serverless FastAPI application.
    
    This class encapsulates all the application setup logic including:
    - Configuration loading
    - Route setup
    - Trigger setup
    - Environment-specific configuration
    """
    
    def __init__(self):
        """Initialize the application factory."""
        self.scheduler = AsyncIOScheduler()
        self._app: Optional[FastAPI] = None
        self.config = None
        
    def check_function_exists(self, function: str) -> bool:
        """
        Check if a function implementation file exists in the functions directory.
        
        Args:
            function (str): Name of the function to check (without '_function.py' suffix)
            
        Returns:
            bool: True if the function implementation file exists, False otherwise
        """
        file = f"functions/{function}_function.py"
        return os.path.exists(file)
        
    def setup_sync_routes(self, app: FastAPI) -> None:
        """
        Set up HTTP routes synchronously during app initialization.
        
        Args:
            app (FastAPI): The FastAPI application instance
        """
        http_functions = get_functions_by_trigger_type(self.config, 'http')
        
        for function_instance in http_functions:
            function_name = function_instance['name']
            trigger_config = function_instance['trigger']
            
            # Validate function exists
            if not self.check_function_exists(function_name):
                L.error(f"Function {function_name} does not exist")
                continue
                
            try:
                from triggers.handlers import TriggerHandlerFactory
                handler = TriggerHandlerFactory.create_handler(
                    function_name, trigger_config, app, self.scheduler
                )
                handler.setup()  # This is synchronous for HTTP handlers
                L.info(f"Set up HTTP route for {function_name}")
            except Exception as e:
                L.error(f"Error setting up HTTP route for {function_name}: {str(e)}")
                
    async def setup_async_triggers(self, app: FastAPI) -> None:
        """
        Set up async triggers (timer and Unity table triggers).
        
        Args:
            app (FastAPI): The FastAPI application instance
        """
        from triggers.handlers import TriggerHandlerFactory
        
        await manager.broadcast_log("Starting async trigger setup...")
        
        # Get only async triggers (timer and unity_table)
        async_functions = (
            get_functions_by_trigger_type(self.config, 'timer') +
            get_functions_by_trigger_type(self.config, 'unity_table')
        )
        
        for function_instance in async_functions:
            function_name = function_instance['name']
            trigger_config = function_instance['trigger']
                
            # Validate function exists
            if not self.check_function_exists(function_name):
                error_msg = f"Function {function_name} does not exist"
                L.error(error_msg)
                await manager.broadcast_log(error_msg)
                continue
            
            try:
                # Create appropriate handler (only async triggers: timer, unity_table)
                handler = TriggerHandlerFactory.create_handler(
                    function_name=function_name,
                    trigger_config=trigger_config,
                    app=app,
                    scheduler=self.scheduler
                )
                
                # Set up the async trigger
                await handler.setup()
                L.info(f"Set up async trigger for {function_name}")
                
            except Exception as e:
                error_msg = f"Error setting up trigger for {function_name}: {str(e)}"
                L.error(error_msg)
                await manager.broadcast_log(error_msg)
                

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """
        FastAPI lifespan context manager that handles application startup and shutdown.
        
        Args:
            app (FastAPI): The FastAPI application instance
        """
        try:
            L.info("Starting application setup...")
            
            # Set up async triggers
            await self.setup_async_triggers(app)
            
            # Start the scheduler
            self.scheduler.start()
            L.info("Scheduler started")
            
            # Log all registered routes after setup
            L.info("All registered routes after setup:")
            for route in app.routes:
                L.info(f"  - {route.path} [{','.join(route.methods if hasattr(route, 'methods') else [])}]")
                
            yield
            
        except Exception as e:
            L.error(f"Error during startup: {str(e)}")
            raise
        finally:
            # Shutdown scheduler
            if self.scheduler.running:
                self.scheduler.shutdown()
                L.info("Scheduler shutdown")
                
    def create_app(self) -> FastAPI:
        """
        Create and configure the FastAPI application.
        
        Returns:
            FastAPI: The configured application instance
            
        Raises:
            ConfigurationError: If configuration loading fails
        """
        # Load configuration
        try:
            self.config = load_config()
            L.info("Configuration loaded successfully")
        except Exception as e:
            L.error(f"Configuration error: {str(e)}")
            raise
            
        # Create FastAPI app with lifespan handler
        app = FastAPI(lifespan=self.lifespan)
        
        # Set up synchronous routes (HTTP triggers)
        self.setup_sync_routes(app)
        
        self._app = app
        return app
        
    @property
    def app(self) -> FastAPI:
        """
        Get the created application instance.
        
        Returns:
            FastAPI: The application instance
            
        Raises:
            RuntimeError: If the app hasn't been created yet
        """
        if self._app is None:
            raise RuntimeError("App not created yet. Call create_app() first.")
        return self._app


# Global factory instance
app_factory = ServerlessAppFactory()
