
import logging as L
from common.repository import Unity
from triggers.execute import execute_action
from common.websocket_manager import manager
# Store the last processed version for each table
_table_versions = {}

async def unity_table_listener(table_name: str, function_name: str) -> dict:
    """
    Monitor changes in a Unity table and trigger a function when changes are detected.
    
    This function:
    1. Checks for changes in the specified Unity table using DESCRIBE HISTORY
    2. Maintains version tracking to detect new changes
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
                    "latest_version": <version_number>,
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
        - Version tracking persists across function calls using the _table_versions global dict
    """
    try:
        try:
            await manager.broadcast_log(f"Checking Unity table {table_name} for changes...")
        except Exception as ws_exc:
            L.warning(f"WebSocket broadcast failed: {ws_exc}")
        # Get Unity connection
        unity = Unity()
        # Get the last processed version for this table
        last_version = _table_versions.get(table_name)
        # Check for changes
        changes = await unity.detect_changes(table_name, last_version)
        if changes:
            change_msg = f"Detected changes in Unity table {table_name} - new version: {changes['latest_version']}"
            L.info(change_msg)
            await manager.broadcast_log(change_msg)
            # Update the last processed version
            _table_versions[table_name] = changes["latest_version"]
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
                "latest_version": changes["latest_version"],
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
