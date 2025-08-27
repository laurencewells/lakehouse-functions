import os
from datetime import datetime
from databricks import sql
from databricks.sql.client import Connection
from databricks.sdk.core import Config, oauth_service_principal
import logging

class DatabricksAuthentication:
    """
    Handles authentication to Databricks services using various authentication methods.
    
    This class supports multiple authentication methods:
    1. Bearer token authentication (provided directly)
    2. Personal access token (via DATABRICKS_TOKEN environment variable)
    3. Service principal OAuth (via client ID and secret environment variables)
    
    The class automatically selects the appropriate authentication method based on
    available credentials, with the following precedence:
    1. Bearer token (if provided in constructor)
    2. Personal access token (if DATABRICKS_TOKEN exists)
    3. Service principal (if DATABRICKS_CLIENT_ID exists)
    
    Required Environment Variables:
        DATABRICKS_HOST: The Databricks workspace hostname
        DATABRICKS_WAREHOUSE_PATH: The HTTP path to the SQL warehouse
        
    Optional Environment Variables (one set required):
        DATABRICKS_TOKEN: Personal access token for authentication
        or
        DATABRICKS_CLIENT_ID: OAuth client ID for service principal
        DATABRICKS_CLIENT_SECRET: OAuth client secret for service principal
        
    Attributes:
        server (str): Databricks workspace hostname
        path (str): HTTP path to SQL warehouse
        bearer (str): Optional bearer token for direct authentication
        client: Authenticated Databricks SQL client
    """
    
    def __init__(self, bearer: str = None) -> None:
        """
        Initialize Databricks authentication with optional bearer token.
        
        Args:
            bearer (str, optional): Bearer token for direct authentication.
                                  If not provided, other auth methods will be attempted.
        
        Raises:
            ValueError: If no valid authentication method is available
        """
        self.server = os.getenv("DATABRICKS_HOST")
        self.path = os.getenv("DATABRICKS_WAREHOUSE_PATH")
        self.bearer = bearer
        self.client = self._get_client()

    def _get_client(self) -> Connection:
        """
        Create and return an authenticated Databricks SQL client.
        
        This method attempts authentication in the following order:
        1. Bearer token (if provided in constructor)
        2. Personal access token (if DATABRICKS_TOKEN exists)
        3. Service principal OAuth (if DATABRICKS_CLIENT_ID exists)
        
        Returns:
            sql.Connection: Authenticated Databricks SQL connection
            
        Raises:
            ValueError: If no valid authentication method is available
            
        Note:
            The connection is configured with the local timezone for consistent
            timestamp handling across different environments.
        """
        if self.bearer:
            logging.log(logging.INFO, "Using bearer authentication")
            return sql.connect(
                server_hostname=self.server,
                http_path=self.path,
                access_token=self.bearer,
                session_configuration= {"timezone": self._local_tz()},
            )
        local_tz = self._local_tz()
        if "DATABRICKS_TOKEN" in os.environ:
            logging.log(logging.INFO, "Using token authentication")
            return sql.connect(
                server_hostname=self.server,
                http_path=self.path,
                access_token=os.getenv("DATABRICKS_TOKEN"),
                session_configuration= {"timezone": local_tz},
            )
        elif "DATABRICKS_CLIENT_ID" in os.environ:
            logging.log(logging.INFO, "Using machine authentication")
            return sql.connect(
                server_hostname=self.server,
                http_path=self.path,
                credentials_provider=self.__credential_provider,
                session_configuration={"timezone": local_tz},
            )
        else:
            raise ValueError("No authentication method provided")

    def __credential_provider(self) -> str:
        """
        Create an OAuth service principal credential provider.
        
        This private method configures and returns a service principal OAuth
        credential provider using client ID and secret from environment variables.
        
        Returns:
            str: OAuth access token for authentication
            
        Raises:
            ValueError: If required environment variables are missing
            
        Note:
            This method is used internally by _get_client when using
            service principal authentication.
        """
        config = Config(
            host=f"https://{self.server}",
            client_id=os.getenv("DATABRICKS_CLIENT_ID"),
            client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
        )
        return oauth_service_principal(config)

    def _local_tz(self) -> str:
        """
        Get the local timezone offset in +/-HH:MM format.
        
        This method determines the local system timezone and formats its
        UTC offset in a format suitable for Databricks SQL connections.
        
        Returns:
            str: Timezone offset in format '+HH:MM' or '-HH:MM'
            
        Example:
            For UTC+1 returns '+01:00'
            For UTC-7 returns '-07:00'
        """
        now = datetime.now().astimezone()
        # Get the UTC offset in hours and minutes
        utc_offset = now.strftime('%z')
        # Format the UTC offset as +HH:MM or -HH:MM
        formatted_offset = f"{utc_offset[:3]}:{utc_offset[3:]}"
        return formatted_offset
