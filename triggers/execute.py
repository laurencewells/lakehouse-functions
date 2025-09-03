import asyncio
import importlib
import logging as L

# Function to execute actions
async def execute_action(function: str) -> dict:
    """
    Dynamically imports and executes a function by name.
    
    This function attempts to:
    1. Import the function module from either triggers or functions directory
    2. Execute the module's main function, handling both sync and async implementations
    3. Return the result or error information
    
    Args:
        function (str): Name of the function to execute (without '_function.py' suffix)
        
    Returns:
        dict: Either the function's result or an error message if execution failed
        
    Raises:
        ImportError: If the function module cannot be found
        Exception: For any other errors during execution (caught and returned as error dict)
    """
    print(f"Executing function: {function}")
    try:
        # Dynamically import the function module
        try:
            # First try to import from triggers folder
            module = importlib.import_module(f"triggers.{function}_function")
        except ImportError:
            # If not found in triggers, try functions folder
            module = importlib.import_module(f"functions.{function}_function")
        if hasattr(module, 'main'):
            # Convert the synchronous function to async if needed
            if asyncio.iscoroutinefunction(module.main):
                result = await module.main()
            else:
                # Run sync functions in a thread pool to avoid blocking
                result = await asyncio.to_thread(module.main)
            return result
        else:
            L.error(f"No main function found in {function}_function")
            return {"error": "Function not found"}
    except Exception as e:
        L.error(f"Error executing function {function}: {str(e)}")
        return {"error": str(e)}