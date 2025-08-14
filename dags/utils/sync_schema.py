from datetime import datetime
import pandas as pd
from sqlalchemy.future import Engine
from datetime import datetime


def get_production_tables(production_schema: str, engine: Engine, **kwargs) -> None:
    """
    Get list of tables from production which are not temporary, views, or materialized views.
    
    Parameters
    ----------
    api_conn_id : str
        The connection id to be used to connect to the API
    """

    query = f"""
    SELECT
        TABLE_NAME table_name,
        COALESCE(UPDATE_TIME, CREATE_TIME) update_time,
        DATA_LENGTH data_length
    FROM
        information_schema.tables
    WHERE
        TABLE_SCHEMA = '{production_schema}'
        AND NOT REGEXP_LIKE(table_name, '(?i)(temp|tmp|test|tst)')
        AND TABLE_TYPE = 'BASE TABLE';
    """

    print(query)

    connection = engine.connect()
    table_info = pd.read_sql(query, connection)
    
    print(f"Found {len(table_info)} tables in production schema {production_schema}.")

    print(f"Filtering tables that has modified today...")

    table_info = table_info[table_info["update_time"] <= datetime.now()]
    
    print(f"Found {len(table_info)} tables in production schema {production_schema} after filtering.")

    print(table_info.head())

    kwargs["ti"].xcom_push(
        key="production_tables",
        value=table_info["table_name"].tolist()
    )
    return

def sync_schema(staging_schema:str, production_schema:str, engine: Engine, sync_limit_rows: int = 75000, **kwargs) -> None:
    """
    Sync schema with the latest changes from production schema.

    Parameters
    ----------
    staging_schema : str
        The name of the schema to be synced.
    production_schema : str
        The name of the production schema to be used as the source of truth.
    engine : Engine
        The Connectable SQLAlchemy engine to be used to connect to the database.
    """
    table_list = kwargs["ti"].xcom_pull(
        task_ids="get_production_tables_task",
        key="production_tables"
    )

    if table_list is None or len(table_list) == 0:
        print("No tables to sync.")
        return

    print(f"Syncing {len(table_list)} tables from production schema {production_schema} to schema {staging_schema}...")

    for table in table_list:
        print(f"Syncing table {table}...")
        query = f"""
        DROP TABLE IF EXISTS `{staging_schema}`.`{table}`;
        CREATE TABLE `{staging_schema}`.`{table}` LIKE `{production_schema}`.`{table}`;
        -- Copy data from production to
        INSERT INTO `{staging_schema}`.`{table}`
        SELECT * FROM `{production_schema}`.`{table}` LIMIT {sync_limit_rows};
        """
        with engine.connect() as connection:
            connection.execute(query)
        print(f"Table {table} synced successfully.")
    print("All tables synced successfully.")
