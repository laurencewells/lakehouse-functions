
import logging as L
from common.repository import Unity
from triggers.execute import execute_action
from common.websocket_manager import manager
# Store the last processed timestamp for each table
_table_timestamps = {}
_volume_timestamps = {}

async def unity_table_listener(table_name: str, function_name: str) -> dict:
    """
    Monitor changes in a Unity table and trigger a function when changes are detected.
    
    This function:
    1. Checks for changes in the specified Unity table using table.updated_at timestamp
    2. Maintains timestamp tracking to detect new changes
    3. Triggers the specified function when changes are detected
    4. Broadcasts status updates via WebSocket
    
    Args:
        table_name (str): The name of the Unity table to monitor
        function_name (str): The name of the function to trigger on changes
        
    Returns:
        dict: Status of the operation with the following structure:
            On success with changes:
                {
                    "status": "success",
                    "changes_detected": True,
                    "latest_timestamp": <timestamp_in_millis>,
                    "latest_timestamp_iso": <iso_timestamp_string>,
                    "action": "initial state recorded" | "changes processed"
                }
            On success without changes:
                {
                    "status": "success",
                    "changes_detected": False
                }
            On error:
                {
                    "status": "error",
                    "message": <error_message>
                }
                
    Note:
        - The first run for a table will record the initial state without triggering the function
        - Subsequent changes will trigger the specified function
        - Timestamp tracking persists across function calls using the _table_timestamps global dict
    """

    try:
        try:
            await manager.broadcast_log(f"Checking Unity table {table_name} for changes...")
        except Exception as ws_exc:
            L.warning(f"WebSocket broadcast failed: {ws_exc}")
        # Get Unity connection
        
        unity = Unity()
        # Get the last processed timestamp for this table
        last_timestamp = _table_timestamps.get(table_name)
        # Check for changes
        changes = await unity.detect_changes_by_timestamp(table_name, last_timestamp)
        if changes:
            change_msg = f"Detected changes in Unity table {table_name} - new timestamp: {changes['latest_timestamp_iso']}"
            L.info(change_msg)
            await manager.broadcast_log(change_msg)
            # Update the last processed timestamp
            _table_timestamps[table_name] = changes["latest_timestamp"]
            # If this is not just the initial state, trigger the example function
            if changes["type"] != "initial_state":
                await manager.broadcast_log(f"Triggering function {function_name} due to table changes")
                await execute_action(function_name)
                await manager.broadcast_log(f"Successfully executed function {function_name}")
            else:
                await manager.broadcast_log(f"Recorded initial state for table {table_name}")
            return {
                "status": "success",
                "changes_detected": True,
                "latest_timestamp": changes["latest_timestamp"],
                "latest_timestamp_iso": changes["latest_timestamp_iso"],
                "action": "initial state recorded" if changes["type"] == "initial_state" else "changes processed"
            }
        
        return {
            "status": "success",
            "changes_detected": False
        }
        
    except Exception as e:
        error_msg = f"Error monitoring Unity table {table_name}: {str(e)}"
        L.error(error_msg)
        await manager.broadcast_log(error_msg)
        return {"status": "error", "message": str(e)}

async def unity_volume_listener(volume_path: str, function_name: str) -> dict:
    """
    Monitor changes in a Unity volume and trigger a function when changes are detected.
    """
    try:
        try:
            await manager.broadcast_log(f"Checking Unity volume {volume_path} for changes...")
        except Exception as ws_exc:
            L.warning(f"WebSocket broadcast failed: {ws_exc}")
        # Get Unity connection
        
        unity = Unity()
        # Get the last processed timestamp for this table
        last_timestamp = _volume_timestamps.get(volume_path)
        # Check for changes
        changes = await unity.detect_volume_changes_by_timestamp(volume_path, last_timestamp)
        if changes:
            change_msg = f"Detected changes in Unity volume {volume_path} - new timestamp: {changes['latest_timestamp_iso']}"
            L.info(change_msg)
            await manager.broadcast_log(change_msg)
            # Update the last processed timestamp
            _volume_timestamps[volume_path] = changes["latest_timestamp"]
            # If this is not just the initial state, trigger the example function
            if changes["type"] != "initial_state":
                await manager.broadcast_log(f"Triggering function {function_name} due to table changes")
                await execute_action(function_name)
                await manager.broadcast_log(f"Successfully executed function {function_name}")
            else:
                await manager.broadcast_log(f"Recorded initial state for volume {volume_path}")
            return {
                "status": "success",
                "changes_detected": True,
                "latest_timestamp": changes["latest_timestamp"],
                "latest_timestamp_iso": changes["latest_timestamp_iso"],
                "action": "initial state recorded" if changes["type"] == "initial_state" else "changes processed"
            }
        
        return {
            "status": "success",
            "changes_detected": False
        }
        
    except Exception as e:
        error_msg = f"Error monitoring Unity volume {volume_path}: {str(e)}"
        L.error(error_msg)
        await manager.broadcast_log(error_msg)
        return {"status": "error", "message": str(e)}