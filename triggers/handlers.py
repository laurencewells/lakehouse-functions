"""
Trigger handler classes for managing different types of function triggers.

This module provides a set of handler classes for different trigger types:
- HTTP triggers (REST endpoints) - synchronous
- Timer triggers (scheduled jobs) - asynchronous
- Unity table triggers (table change monitoring) - asynchronous

Each handler implements a common interface for setup and execution,
while providing type-specific functionality.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import logging as L
import asyncio
from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from common.websocket_manager import manager
from triggers.execute import execute_action


class TriggerHandler(ABC):
    """
    Abstract base class for all trigger handlers.
    
    This class defines the common interface and shared functionality
    for all trigger handlers.
    
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


class AsyncTriggerHandler(TriggerHandler):
    """
    Base class for asynchronous trigger handlers (Timer, Unity Table).
    """
    
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
        Set up the async trigger handler.
        
        This method must be implemented by each async trigger type to handle
        its specific setup requirements (e.g., scheduling jobs).
        
        Raises:
            NotImplementedError: If the child class doesn't implement this method
        """
        pass


class SyncTriggerHandler(TriggerHandler):
    """
    Base class for synchronous trigger handlers (HTTP).
    """
    
    def log_message(self, message: str) -> None:
        """
        Log a message synchronously.
        
        Args:
            message (str): Message to log
        """
        L.info(message)
        
    def handle_error(self, error: Exception, context: str) -> None:
        """
        Handle and log an error synchronously.
        
        Args:
            error (Exception): The error that occurred
            context (str): Context description for the error
        """
        error_msg = f"{context}: {str(error)}"
        L.error(error_msg)
        
    @abstractmethod
    def setup(self) -> None:
        """
        Set up the sync trigger handler.
        
        This method must be implemented by each sync trigger type to handle
        its specific setup requirements (e.g., creating routes).
        
        Raises:
            NotImplementedError: If the child class doesn't implement this method
        """
        pass


class TimerTriggerHandler(AsyncTriggerHandler):
    """
    Handler for timer-triggered functions.
    
    This handler sets up scheduled jobs that execute functions
    based on cron schedules.
    
    Attributes:
        Inherits all attributes from AsyncTriggerHandler
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
        
        
class UnityTableTriggerHandler(AsyncTriggerHandler):
    """
    Handler for Unity table-triggered functions.
    
    This handler sets up monitoring jobs that watch Unity tables
    for changes and execute functions when changes are detected.
    
    Attributes:
        Inherits all attributes from AsyncTriggerHandler
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
        
        # Format the full table name without backticks to handle spaces
        full_table_name = f"{table_config['catalog']}.{table_config['schema']}.{table_config['name']}"
        
        await self.log_message(f"Setting up Unity table trigger for function: {self.function_name}")
        
        # Create monitoring function
        async def monitor_unity_table():
            try:
                await self.log_message(f"Monitoring Unity table {full_table_name} for function: {self.function_name}")
                from triggers.unity_listener_functions import unity_table_listener
                await unity_table_listener(full_table_name, self.function_name)
            except Exception as e:
                await self.handle_error(e, f"Error monitoring Unity table {full_table_name}")
                
        # Add job to scheduler
        self.scheduler.add_job(monitor_unity_table, 'interval', seconds=interval)
        await self.log_message(f"Set up Unity table monitor for {full_table_name} (checking every {interval} seconds)")
        
class UnityVolumeTriggerHandler(AsyncTriggerHandler):
    """
    Handler for Unity table-triggered functions.
    
    This handler sets up monitoring jobs that watch Unity tables
    for changes and execute functions when changes are detected.
    
    Attributes:
        Inherits all attributes from AsyncTriggerHandler
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
            
        volume_config = self.trigger_config.get('volume_config')
        if not volume_config:
            raise KeyError("Unity table trigger requires 'volume_config' configuration")
            
        # Validate table name structure
        if not all(key in volume_config for key in ['catalog', 'schema', 'name']):
            raise KeyError("Volume name configuration must include 'catalog', 'schema', and 'name'")
            
        # Get check interval (default 60 seconds)
        interval = self.trigger_config.get('check_interval', 60)
        
        if volume_config.get('sub_path'):
            full_volume_path = f"/Volumes/{volume_config['catalog']}/{volume_config['schema']}/{volume_config['name']}/{volume_config.get('sub_path')}"
        else:
            full_volume_path = f"/Volumes/{volume_config['catalog']}/{volume_config['schema']}/{volume_config['name']}"
            
        # Format the full table name without backticks to handle spaces
        
        await self.log_message(f"Setting up Unity volume trigger for function: {self.function_name}")
        
        # Create monitoring function
        async def monitor_unity_volume():
            try:    
                await self.log_message(f"Monitoring Unity volume {full_volume_path} for function: {self.function_name}")
                from triggers.unity_listener_functions import unity_volume_listener
                await unity_volume_listener(full_volume_path, self.function_name)
            except Exception as e:
                await self.handle_error(e, f"Error monitoring Unity volume {full_volume_path}")
                
        # Add job to scheduler
        self.scheduler.add_job(monitor_unity_volume, 'interval', seconds=interval)
        await self.log_message(f"Set up Unity volume monitor for {full_volume_path} (checking every {interval} seconds)")
                           

class HTTPTriggerHandler(SyncTriggerHandler):
    """
    Handler for HTTP-triggered functions.
    
    This handler sets up an HTTP endpoint for the function.
    Must be synchronous because it's called during app initialization.
    """
    
    def setup(self) -> None:
        """
        Set up the HTTP endpoint for the function.
        """
        function_name = self.function_name
        trigger_config = self.trigger_config
        endpoint = trigger_config.get('endpoint')
        
        if not endpoint:
            self.handle_error(
                ValueError(f"HTTP trigger for {function_name} missing endpoint configuration"),
                "HTTP trigger setup"
            )
            return
            
        # Ensure endpoint starts with /
        full_endpoint = f"/api/v1{endpoint if endpoint.startswith('/') else '/' + endpoint}"
        
        # Get the HTTP method from config, default to GET
        method = trigger_config.get('method', 'GET').upper()
        
        # Create route handler
        self.log_message(f"Setting up HTTP route {method} {full_endpoint} for function {function_name}")
        
        # Define the handler using a closure to capture function_name
        async def create_handler(func_name=function_name):
            try:
                await manager.broadcast_log(f"Executing HTTP-triggered function: {func_name}")
                await execute_action(func_name)
                await manager.broadcast_log(f"Successfully completed function: {func_name}")
                return {"status": "success"}
            except Exception as e:
                error_msg = f"Error executing function {func_name}: {str(e)}"
                L.error(error_msg)
                await manager.broadcast_log(error_msg)
                return {"status": "error", "message": str(e)}
        
        # Set the handler name and docstring
        create_handler.__name__ = f"{function_name}_handler"
        create_handler.__doc__ = f"Handle {function_name} endpoint ({method})"
        
        # Register the route using the appropriate decorator based on method
        if method == 'GET':
            self.app.get(full_endpoint)(create_handler)
        elif method == 'POST':
            self.app.post(full_endpoint)(create_handler)
        else:
            self.log_message(f"Unsupported HTTP method {method} for function {function_name}, defaulting to GET")
            self.app.get(full_endpoint)(create_handler)


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
            'timer': TimerTriggerHandler,
            'unity_table': UnityTableTriggerHandler,
            'unity_volume': UnityVolumeTriggerHandler,
            'http': HTTPTriggerHandler
        }
        
        handler_class = handlers.get(trigger_type)
        if not handler_class:
            raise ValueError(f"Unknown trigger type: {trigger_type}")
            
        return handler_class(function_name, trigger_config, app, scheduler)