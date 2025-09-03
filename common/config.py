"""
Simple configuration management for the serverless application.
"""

import os
import yaml
import logging as L
from typing import Dict, List, Any


def load_config(file_path: str = 'app.yaml') -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        file_path (str): Path to the YAML configuration file
        
    Returns:
        dict: Parsed YAML content
        
    Raises:
        FileNotFoundError: If the configuration file doesn't exist
        yaml.YAMLError: If the YAML is malformed
    """
    try:
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        L.error(f"Configuration file {file_path} not found")
        raise
    except yaml.YAMLError as e:
        L.error(f"Error parsing YAML configuration: {str(e)}")
        raise


def get_functions_by_trigger_type(config: Dict[str, Any], trigger_type: str) -> List[Dict[str, Any]]:
    """
    Get all functions with a specific trigger type.
    
    Args:
        config (dict): The configuration dictionary
        trigger_type (str): The trigger type to filter by ('http', 'timer', 'unity_table')
        
    Returns:
        list: List of function configurations matching the trigger type
    """
    functions = config.get('functions', [])
    return [
        func for func in functions
        if func.get('trigger', {}).get('type') == trigger_type
    ]


def is_development() -> bool:
    """Check if running in development environment."""
    return os.environ.get("ENV") == "DEV"


def get_cors_origins() -> List[str]:
    """Get CORS origins for development."""
    return [
        "http://127.0.0.1:5173",
        "http://127.0.0.1:8000",
        "http://localhost:8000",
        "http://localhost:5173"
    ]


def get_static_files_directory() -> str:
    """Get static files directory for production."""
    return "front-end/dist"