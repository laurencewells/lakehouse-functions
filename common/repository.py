from common.authentication import DatabricksAuthentication
from pandas import DataFrame
import asyncio
from typing import Optional
import logging as L

class Unity:
    """
    A class to interact with Unity tables through Databricks SQL.
    
    This class provides functionality to:
    - Execute SQL statements synchronously and asynchronously
    - Monitor Unity table versions and changes
    - Handle table history and versioning
    
    The class uses DatabricksAuthentication for secure access to Unity tables.
    
    Attributes:
        client: Authenticated Databricks SQL client instance
    """
    def __init__(self) -> None:
        """Initialize Unity class with an authenticated Databricks client."""
        self.client = DatabricksAuthentication().client

    async def run_sql_statement_async(self, statement: str) -> Optional[DataFrame]:
        """
        Execute a SQL statement asynchronously and return results as a DataFrame.
        
        This method:
        1. Executes the SQL statement asynchronously using Databricks cursor
        2. Converts the result to an Arrow table
        3. Returns the data as a pandas DataFrame
        
        Args:
            statement (str): The SQL statement to execute
            
        Returns:
            Optional[DataFrame]: Results as a pandas DataFrame, or None if no results
            
        Raises:
            Exception: If there's an error executing the SQL statement
            
        Note:
            This method is non-blocking and suitable for long-running queries
        """
        with self.client.cursor() as cursor:
            call = cursor.execute_async(statement)
            result = await asyncio.to_thread(lambda: call.get_async_execution_result())
            return await asyncio.to_thread(lambda: call.fetchall_arrow().to_pandas())
            
    def run_sql_statement(self, statement: str) -> Optional[DataFrame]:
        """
        Execute a SQL statement synchronously and return results as a DataFrame.
        
        This method:
        1. Executes the SQL statement using Databricks cursor
        2. Converts the result to an Arrow table
        3. Returns the data as a pandas DataFrame
        
        Args:
            statement (str): The SQL statement to execute
            
        Returns:
            Optional[DataFrame]: Results as a pandas DataFrame, or None if no results
            
        Raises:
            Exception: If there's an error executing the SQL statement
            
        Note:
            This method is blocking and will wait for the query to complete
            For long-running queries, prefer run_sql_statement_async
        """
        with self.client.cursor() as cursor:
            cursor.execute(statement)
            return cursor.fetchall_arrow().to_pandas()

    async def get_latest_version(self, table_name: str) -> int:
        """
        Get the latest version number for a Unity table using DESCRIBE HISTORY.
        Returns the most recent version number.
        """
        try:
            history_query = f"DESCRIBE HISTORY {table_name} LIMIT 1"
            history = await self.run_sql_statement_async(history_query)
            
            if history.empty:
                raise ValueError(f"No history found for table {table_name}")
            
            return history["version"].iloc[0]
        except Exception as e:
            raise Exception(f"Error getting table history: {str(e)}")

    async def detect_changes(self, table_name: str, last_processed_version: Optional[int] = None) -> Optional[dict]:
        """
        Detect changes in a Unity table by comparing versions.
        
        This method checks if there have been any changes to the table since
        the last processed version by comparing version numbers from the table history.
        
        Args:
            table_name (str): The name of the Unity table to check
            last_processed_version (Optional[int]): The last version that was processed.
                                                  If None, indicates first run.
            
        Returns:
            Optional[dict]: Information about detected changes with the following structure:
                For initial state (first run):
                    {
                        "type": "initial_state",
                        "latest_version": <version_number>
                    }
                For detected changes:
                    {
                        "type": "changes_detected",
                        "latest_version": <version_number>
                    }
                If no changes: None
                
        Raises:
            Exception: If there's an error accessing table history or detecting changes
            
        Note:
            The first run (last_processed_version=None) always returns initial state
            without triggering change detection
        """
        try:
            L.info(f"Detecting changes for table {table_name} with last processed version {last_processed_version}")
            latest_version = await self.get_latest_version(table_name)
            
            # If no last version provided, return initial state
            if last_processed_version is None:
                return {
                    "type": "initial_state",
                    "latest_version": latest_version,
                }
            
            # Check if there are any changes
            if latest_version == last_processed_version:
                return None
            
            return {
                "type": "changes_detected",
                "latest_version": latest_version,
            }
            
        except Exception as e:
            raise Exception(f"Error detecting changes: {str(e)}")

