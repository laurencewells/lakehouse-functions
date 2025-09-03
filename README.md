# Lakehouse Functions with FastAPI

A modern serverless application framework built with FastAPI that supports multiple trigger types for executing Python functions. This framework is designed to work seamlessly with Unity tables and provides flexible scheduling capabilities.

## Features

- 🔄 **Multiple Trigger Types**: Support for HTTP, timer-based, and Unity table change triggers
- 🎯 **Declarative Configuration**: Simple YAML-based function definitions
- 📊 **Unity Integration**: Built-in support for monitoring Unity table changes
- ⏰ **Flexible Scheduling**: Cron-style scheduling for automated tasks
- 🛡️ **Error Handling**: Robust error handling and logging
- 🚀 **Async Support**: Built on FastAPI and asyncio for high performance

## Prerequisites

- Python 3.10+
- Databricks account (for Unity table features)
- Access to Unity tables

## Installation

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Functions are defined in `app.yaml`. Each function can have one of three trigger types:

### 1. HTTP Triggers
Executes functions in response to HTTP requests.

```yaml
functions:
  - name: "example_http"
    trigger:
      type: "http"
      method: "POST"
      endpoint: "/run-example"
```

### 2. Timer Triggers
Executes functions on a schedule using cron syntax.

```yaml
functions:
  - name: "scheduled_task"
    trigger:
      type: "timer"
      schedule: "*/5 * * * *"  # Every 5 minutes
```

### 3. Unity Table Triggers
Monitors Unity tables for changes and executes functions in response.

```yaml
functions:
  - name: "unity_monitor"
    trigger:
      type: "unity_table"
      check_interval: 30  # Check every 30 seconds
    table_name: "_data.tpch.dim_customer"
```

## Function Implementation

Functions should be implemented in the `functions/` directory following the naming convention:
```
functions/{function_name}_function.py
```

## Environment Configuration

Required environment variables for Unity integration:
```yaml
env:
  - name: 'DATABRICKS_WAREHOUSE_PATH'
    value: '/sql/1.0/warehouses/<your-path>'
```

## Running the Application

Start the FastAPI server:
```bash
uvicorn app:app
```

The server will:
- Listen for HTTP requests at configured endpoints
- Execute scheduled tasks based on timer triggers
- Monitor Unity tables for changes

## Unity Table Trigger Details

The Unity table trigger system:

1. **Monitoring**: Continuously monitors specified Unity tables for changes using DESCRIBE HISTORY
2. **State Management**: Tracks the last processed version for each table
3. **Change Detection**:
   - First run: Records initial state
   - Subsequent runs: Detects and processes changes
4. **Function Execution**: Automatically triggers associated functions when changes are detected

## Error Handling

The framework includes comprehensive error handling:
- Function execution errors are caught and logged
- Unity table monitoring includes automatic retry mechanisms
- All errors are properly propagated to the response/logs

## Dependencies

Main dependencies:
- FastAPI: Web framework
- APScheduler: Task scheduling
- Databricks SDK: Unity table integration
- Databricks SQL Connector: Data access

## Project Structure

```
serverless-apps/
├── app.py              # Main application entry point
├── app.yaml            # Function and trigger definitions
├── common/             # Shared utilities
├── functions/          # Function implementations
├── triggers/           # Trigger implementations
└── requirements.txt    # Project dependencies
```