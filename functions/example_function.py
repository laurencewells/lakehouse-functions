import datetime
from common.authentication import DatabricksAuthentication


def main():
    def convert_millis_to_timestamp(millis: int) -> str:
        """Convert milliseconds since epoch to ISO 8601 timestamp."""
        return datetime.datetime.fromtimestamp(millis / 1000.0).isoformat()

    auth = DatabricksAuthentication()
    client = auth.get_workspace_client()
    last_update = client.tables.get("_data.tpch.dim_customer").updated_at
    print(f"Table last updated at: {convert_millis_to_timestamp(last_update)}")


if __name__ == "__main__":
    main()
