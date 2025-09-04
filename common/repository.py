import json
from common.authentication import DatabricksAuthentication
from pandas import DataFrame
import asyncio
from typing import Optional
import logging as L
import datetime

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
        self.workspace_client = DatabricksAuthentication().get_workspace_client()

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

    def convert_millis_to_timestamp(self, millis: int) -> str:
        """Convert milliseconds since epoch to ISO 8601 timestamp."""
        return datetime.datetime.fromtimestamp(millis / 1000.0).isoformat()
    
    def get_version_and_timestamp(self) -> tuple[int, int]:
        table_info = self.workspace_client.tables.get("_data.tpch.dim_customer", include_delta_metadata=True)
        delta_props = table_info.delta_runtime_properties_kvpairs.delta_runtime_properties

        # Parse the commit attributes to get version and file status
        commit_attrs = json.loads(delta_props['delta.commitAttributes'])
        modification_time = commit_attrs['fileStatus']['modificationTime']
        version = [commit_attrs['version']]
        return version, modification_time

    async def get_table_last_updated(self, table_name: str) -> int:
        """
        Get the last updated timestamp for a Unity table using the workspace client.
        Returns the updated_at timestamp in milliseconds.
        """
        try:
            version, modification_time = await asyncio.to_thread(self.get_version_and_timestamp)
            return int(modification_time)
        except Exception as e:
            raise Exception(f"Error getting table last updated timestamp: {str(e)}")

    async def detect_changes_by_timestamp(self, table_name: str, last_processed_timestamp: Optional[int] = None) -> Optional[dict]:
        """
        Detect changes in a Unity table by comparing last updated timestamps.
        
        This method checks if there have been any changes to the table since
        the last processed timestamp using the table's updated_at field.
        
        Args:
            table_name (str): The name of the Unity table to check
            last_processed_timestamp (Optional[int]): The last timestamp that was processed (in milliseconds).
                                                    If None, indicates first run.
            
        Returns:
            Optional[dict]: Information about detected changes with the following structure:
                For initial state (first run):
                    {
                        "type": "initial_state",
                        "latest_timestamp": <timestamp_in_millis>,
                        "latest_timestamp_iso": <iso_timestamp_string>
                    }
                For detected changes:
                    {
                        "type": "changes_detected",
                        "latest_timestamp": <timestamp_in_millis>,
                        "latest_timestamp_iso": <iso_timestamp_string>
                    }
                If no changes: None
                
        Raises:
            Exception: If there's an error accessing table information or detecting changes
            
        Note:
            The first run (last_processed_timestamp=None) always returns initial state
            without triggering change detection
        """
        try:
            L.info(f"Detecting changes for table {table_name} with last processed timestamp {last_processed_timestamp}")
            latest_timestamp = await self.get_table_last_updated(table_name)
            
            # If no last timestamp provided, return initial state
            if last_processed_timestamp is None:
                return {
                    "type": "initial_state",
                    "latest_timestamp": latest_timestamp,
                    "latest_timestamp_iso": self.convert_millis_to_timestamp(latest_timestamp)
                }
            
            # Check if there are any changes
            if latest_timestamp == last_processed_timestamp:
                return None
            
            return {
                "type": "changes_detected",
                "latest_timestamp": latest_timestamp,
                "latest_timestamp_iso": self.convert_millis_to_timestamp(latest_timestamp)
            }
            
        except Exception as e:
            raise Exception(f"Error detecting changes by timestamp: {str(e)}")

