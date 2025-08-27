"""
Trigger handler classes for managing different types of function triggers.

This module provides a set of handler classes for different trigger types:
- HTTP triggers (REST endpoints)
- Timer triggers (scheduled jobs)
- Unity table triggers (table change monitoring)

Each handler implements a common interface for setup and execution,
while providing type-specific functionality.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import logging as L
from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from common.websocket_manager import manager
from triggers.execute import execute_action


class TriggerHandler(ABC):
    """
    Abstract base class for trigger handlers.
    
    This class defines the common interface and shared functionality
    for all trigger handlers. Each specific trigger type (HTTP, Timer, Unity)
    should implement this interface.
    
    Attributes:
        function_name (str): Name of the function to be triggered
        trigger_config (dict): Configuration for the trigger
        app (FastAPI): FastAPI application instance for HTTP endpoints
        scheduler (AsyncIOScheduler): Scheduler for timed tasks
    """
    
    def __init__(self, function_name: str, trigger_config: Dict[str, Any], 
                 app: Optional[FastAPI] = None, scheduler: Optional[AsyncIOScheduler] = None):
        """
        Initialize the trigger handler.
        
        Args:
            function_name (str): Name of the function to be triggered
            trigger_config (dict): Configuration for the trigger
            app (FastAPI, optional): FastAPI app instance for HTTP endpoints
            scheduler (AsyncIOScheduler, optional): Scheduler for timed tasks
        """
        self.function_name = function_name
        self.trigger_config = trigger_config
        self.app = app
        self.scheduler = scheduler
        
    async def log_message(self, message: str) -> None:
        """
        Log a message and broadcast it via WebSocket.
        
        Args:
            message (str): Message to log and broadcast
        """
        L.info(message)
        await manager.broadcast_log(message)
        
    async def handle_error(self, error: Exception, context: str) -> None:
        """
        Handle and log an error, broadcasting it via WebSocket.
        
        Args:
            error (Exception): The error that occurred
            context (str): Context description for the error
        """
        error_msg = f"{context}: {str(error)}"
        L.error(error_msg)
        await manager.broadcast_log(error_msg)
        
    @abstractmethod
    async def setup(self) -> None:
        """
        Set up the trigger handler.
        
        This method must be implemented by each trigger type to handle
        its specific setup requirements (e.g., creating endpoints,
        scheduling jobs).
        
        Raises:
            NotImplementedError: If the child class doesn't implement this method
        """
        pass
        
        
class HTTPTriggerHandler(TriggerHandler):
    """
    Handler for HTTP-triggered functions.
    
    This handler creates FastAPI endpoints that execute functions
    when HTTP requests are received.
    
    Attributes:
        Inherits all attributes from TriggerHandler
    """
    
    async def setup(self) -> None:
        """
        Set up an HTTP endpoint for the function.
        
        Creates a FastAPI route that executes the function when
        an HTTP request is received at the specified endpoint.
        
        Raises:
            ValueError: If FastAPI app instance is not provided
            KeyError: If required trigger configuration is missing
        """
        if not self.app:
            raise ValueError("FastAPI app instance required for HTTP triggers")
            
        endpoint = self.trigger_config.get('endpoint')
        method = self.trigger_config.get('method')
        if not endpoint or not method:
            raise KeyError("HTTP trigger requires 'endpoint' and 'method' configuration")
            
        await self.log_message(f"Setting up HTTP trigger for function: {self.function_name}")
        
        # Create closure to capture function name
        async def create_endpoint():
            try:
                await self.log_message(f"Executing HTTP-triggered function: {self.function_name}")
                await execute_action(self.function_name)
                await self.log_message(f"Successfully completed function: {self.function_name}")
                return {"status": "success"}
            except Exception as e:
                await self.handle_error(e, f"Error executing function {self.function_name}")
                return {"status": "error", "message": str(e)}
                
        # Register the endpoint with FastAPI
        self.app.api_route(endpoint, methods=[method])(create_endpoint)
        
        
class TimerTriggerHandler(TriggerHandler):
    """
    Handler for timer-triggered functions.
    
    This handler sets up scheduled jobs that execute functions
    based on cron schedules.
    
    Attributes:
        Inherits all attributes from TriggerHandler
    """
    
    async def setup(self) -> None:
        """
        Set up a scheduled job for the function.
        
        Creates an APScheduler job that executes the function
        according to the specified cron schedule.
        
        Raises:
            ValueError: If scheduler instance is not provided
            KeyError: If required trigger configuration is missing
        """
        if not self.scheduler:
            raise ValueError("Scheduler instance required for timer triggers")
            
        schedule = self.trigger_config.get('schedule')
        if not schedule:
            raise KeyError("Timer trigger requires 'schedule' configuration")
            
        await self.log_message(f"Setting up timer trigger for function: {self.function_name}")
        
        # Create cron trigger from schedule
        cron_trigger = CronTrigger.from_crontab(schedule)
        
        # Create job function
        async def run_scheduled_task():
            try:
                await self.log_message(f"Executing scheduled function: {self.function_name}")
                await execute_action(self.function_name)
                await self.log_message(f"Successfully completed scheduled function: {self.function_name}")
            except Exception as e:
                await self.handle_error(e, f"Error in scheduled function {self.function_name}")
                
        # Add job to scheduler
        self.scheduler.add_job(run_scheduled_task, cron_trigger)
        await self.log_message(f"Scheduled function {self.function_name} with cron: {schedule}")
        
        
class UnityTableTriggerHandler(TriggerHandler):
    """
    Handler for Unity table-triggered functions.
    
    This handler sets up monitoring jobs that watch Unity tables
    for changes and execute functions when changes are detected.
    
    Attributes:
        Inherits all attributes from TriggerHandler
    """
    
    async def setup(self) -> None:
        """
        Set up Unity table monitoring for the function.
        
        Creates an APScheduler job that periodically checks the specified
        Unity table for changes and executes the function when changes
        are detected.
        
        Raises:
            ValueError: If scheduler instance is not provided
            KeyError: If required trigger configuration is missing
        """
        if not self.scheduler:
            raise ValueError("Scheduler instance required for Unity table triggers")
            
        table_config = self.trigger_config.get('table_config')
        if not table_config:
            raise KeyError("Unity table trigger requires 'table_config' configuration")
            
        # Validate table name structure
        if not all(key in table_config for key in ['catalog', 'schema', 'name']):
            raise KeyError("Table name configuration must include 'catalog', 'schema', and 'name'")
            
        # Get check interval (default 60 seconds)
        interval = self.trigger_config.get('check_interval', 60)
        
        # Format the full table name with backticks to handle spaces
        def escape_name(name: str) -> str:
            """Wrap table name components in backticks for consistent escaping."""
            return f"`{name}`"
            
        catalog = escape_name(table_config['catalog'])
        schema = escape_name(table_config['schema'])
        name = escape_name(table_config['name'])
        full_table_name = f"{catalog}.{schema}.{name}"
        
        await self.log_message(f"Setting up Unity table trigger for function: {self.function_name}")
        
        # Create monitoring function
        async def monitor_unity_table():
            try:
                await self.log_message(f"Monitoring Unity table {full_table_name} for function: {self.function_name}")
                from triggers.unity_table_listener_function import unity_table_listener
                await unity_table_listener(full_table_name, self.function_name)
            except Exception as e:
                await self.handle_error(e, f"Error monitoring Unity table {full_table_name}")
                
        # Add job to scheduler
        self.scheduler.add_job(monitor_unity_table, 'interval', seconds=interval)
        await self.log_message(f"Set up Unity table monitor for {full_table_name} (checking every {interval} seconds)")
        
        
class TriggerHandlerFactory:
    """
    Factory class for creating appropriate trigger handlers.
    
    This class encapsulates the logic for creating the correct type of
    trigger handler based on the trigger configuration.
    """
    
    @staticmethod
    def create_handler(function_name: str, trigger_config: Dict[str, Any], 
                      app: Optional[FastAPI] = None, 
                      scheduler: Optional[AsyncIOScheduler] = None) -> TriggerHandler:
        """
        Create and return the appropriate trigger handler.
        
        Args:
            function_name (str): Name of the function to be triggered
            trigger_config (dict): Configuration for the trigger
            app (FastAPI, optional): FastAPI app instance for HTTP endpoints
            scheduler (AsyncIOScheduler, optional): Scheduler for timed tasks
            
        Returns:
            TriggerHandler: The appropriate handler instance for the trigger type
            
        Raises:
            ValueError: If trigger type is not recognized
        """
        trigger_type = trigger_config.get('type')
        if not trigger_type:
            raise ValueError("Trigger configuration must specify 'type'")
            
        handlers = {
            'http': HTTPTriggerHandler,
            'timer': TimerTriggerHandler,
            'unity_table': UnityTableTriggerHandler
        }
        
        handler_class = handlers.get(trigger_type)
        if not handler_class:
            raise ValueError(f"Unknown trigger type: {trigger_type}")
            
        return handler_class(function_name, trigger_config, app, scheduler)
