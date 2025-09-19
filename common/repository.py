import json
from common.authentication import DatabricksAuthentication
from pandas import DataFrame
import asyncio
from typing import Optional
import logging as L
import datetime

VOLUME_LIST_LIMIT = 10 # 10k limit

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
    
    async def get_table_update_version(self, table_name: str) -> int:
        table_info = await asyncio.to_thread(self.workspace_client.tables.get, table_name, include_delta_metadata=True)
        delta_props = table_info.delta_runtime_properties_kvpairs.delta_runtime_properties

        # Parse the commit attributes to get version number if no commit attributes, return 0 this means the cache has gone and table has not been updated
        try:
            commit_attrs = json.loads(delta_props['delta.commitAttributes'])
            version = [commit_attrs['version']]
            return int(version[0])
        except Exception as e:
            return 0

    async def get_table_last_updated(self, table_name: str) -> int:
        """
        Get the last updated timestamp for a Unity table using the workspace client.
        Returns the updated_at timestamp in milliseconds.
        """
        try:
            version = await self.get_table_update_version()
            if version is None:
                raise ValueError("Table version is None")
            return int(version)
        except Exception as e:
            raise Exception(f"Error getting table last updated timestamp: {str(e)}")
    
    async def file_list_update_timestamp(self, volume_path: str) -> Optional[int]:
        """
        Get the last updated timestamp for a Unity volume using the workspace client.
        Returns the updated_at timestamp in milliseconds.
        """
        file_info = await asyncio.to_thread(self.workspace_client.files.list_directory_contents, volume_path)
        if not file_info:
            raise Exception(f"No file info found for volume {volume_path}")
        file_dates = []
        if hasattr(file_info, 'next_page_token'):
            limit_reached = False
            limit = VOLUME_LIST_LIMIT
            while file_info or limit_reached:
                for f in file_info:
                    if f.last_modified is not None:
                        file_dates.append(f.last_modified)
                    limit -= 1
                    if limit == 0:
                        limit_reached = True
                file_info = await asyncio.to_thread(
                    self.workspace_client.files.list_directory_contents, volume_path, page_token=file_info.next_page_token)
        else:
            for f in file_info:
                if f.last_modified is not None:
                    file_dates.append(f.last_modified)
        return int(max(file_dates)) if file_dates else None
        
    async def get_volume_last_updated_timestamp(self, volume_path: str) -> int:
        """
        Get the last updated timestamp for a Unity volume using the workspace client.
        Returns the updated_at timestamp in milliseconds.
        """
        try:
            modification_time = await self.file_list_update_timestamp(volume_path)
            if modification_time:
                return modification_time
            else:
                return 0
        except Exception as e:
            raise Exception(f"Error getting volume last updated timestamp: {str(e)}")

    async def detect_changes_by_version(self, table_name: str, last_processed_version: Optional[int] = None) -> Optional[dict]:
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
                        "latest_version": <version_number>,
                    }
                For detected changes:
                    {
                        "type": "changes_detected",
                        "latest_version": <version_number>,
                    }
                If no changes: None
                
        Raises:
            Exception: If there's an error accessing table information or detecting changes
            
        Note:
            The first run (last_processed_timestamp=None) always returns initial state
            without triggering change detection
        """
        try:
            L.info(f"Detecting changes for table {table_name} with last processed version {last_processed_version}")
            latest_version = await self.get_table_update_version(table_name)
            
            # If no last timestamp provided, return initial state
            if last_processed_version is None:
                return {
                    "type": "initial_state",
                    "latest_version": latest_version,
                }
            
            # Check if there are any changes
            if latest_version <= last_processed_version:
                return None
            
            return {
                "type": "changes_detected",
                "latest_version": latest_version,
            }
            
        except Exception as e:
            raise Exception(f"Error detecting changes by version: {str(e)}")
        
        
    async def detect_volume_changes_by_timestamp(self, volume_path: str, last_processed_timestamp: Optional[int] = None) -> Optional[dict]:
        """
        Detect changes in a Unity volume by comparing last updated timestamps.
        
        This method checks if there have been any changes to the volume since
        the last processed timestamp using the volume's updated_at field.
        
        Args:
            volume_path (str): The path of the Unity volume to check
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
            Exception: If there's an error accessing volume information or detecting changes
            
        Note:
            The first run (last_processed_timestamp=None) always returns initial state
            without triggering change detection
        """
        try:
            L.info(f"Detecting changes for volume {volume_path} with last processed timestamp {last_processed_timestamp}")
            latest_timestamp = await self.get_volume_last_updated_timestamp(volume_path)
            
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

