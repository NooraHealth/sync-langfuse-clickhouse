from datetime import datetime, timezone
from typing import Iterator, Optional
from pathlib import Path
import yaml
import dlt
import clickhouse_connect
import utils

# Load params and start date at module level
params = utils.get_params('langfuse_clickhouse')
start_date = datetime.strptime(params['start_date'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

# dlt internal tables — never load these as resources
dlt_tables = {"_dlt_version", "_dlt_loads", "_dlt_pipeline_state"}

# Load the import schema
import_schema_path = Path("schemas/import") / "langfuse_clickhouse_source.schema.yaml"
with import_schema_path.open() as f:
    import_schema = yaml.safe_load(f)

# Columns that are Maps/Arrays in ClickHouse and must be cast to string.
# All other columns are selected as-is by their name from the schema file.
cast_columns = {"metadata": "toJSONString(metadata) as metadata", "tags": "toString(tags) as tags", "input": "toString(input) as input", "output": "toString(output) as output"}

def columns_from_schema(table: str) -> list[str]:
    columns = import_schema["tables"][table]["columns"].keys()
    return [cast_columns.get(col, col) for col in columns]

# Helpers
def get_client(host: str, port: int, database: str, username: str, password: str):
    return clickhouse_connect.get_client(host = host, port = port, database = database, username = username, password = password, compress = True)

def fetch_all(client, table: str, columns: list[str], cursor: datetime) -> list[dict]:
    col_sql = ",\n            ".join(columns)
    query = f"""
        select
          {col_sql}
        from {table} final
        where updated_at > %(cursor)s
        order by updated_at asc
    """
    result = client.query(query, parameters={"cursor": cursor})
    return [dict(zip(result.column_names, row)) for row in result.result_rows]

def make_resource(table: str, client):
    columns = columns_from_schema(table)
    @dlt.resource(name = table, table_name = table, primary_key = ["id", "project_id"], write_disposition = "merge")
    def resource(updated_at: dlt.sources.incremental[datetime] = dlt.sources.incremental("updated_at", initial_value = start_date,),
    ) -> Iterator[list]:
        rows = fetch_all(client, table, columns, updated_at.last_value,)
        if rows:
            yield rows

    resource.__name__ = table
    return resource

# Source
@dlt.source(name="langfuse_clickhouse_source", max_table_nesting=0)
def langfuse_clickhouse_source(
    host: str = dlt.config.value,
    port: int = dlt.config.value,
    database: str = dlt.config.value,
    username: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
) -> list:
    client = get_client(host, port, database, username, password)
    tables = [t for t in import_schema["tables"] if t not in dlt_tables]
    return [
        make_resource(table, client)
        for table in tables
    ]

# Pipeline
def run() -> None:
    pipeline = dlt.pipeline(
        pipeline_name = params['pipeline_name'],
        destination = dlt.destinations.bigquery(location =params['location']),
        dataset_name = params['dataset_name'],
        progress = "log",
        import_schema_path = "schemas/import",
        export_schema_path = "schemas/export",
    )
    load_info = pipeline.run(langfuse_clickhouse_source())
    print(load_info)


if __name__ == "__main__":
    run()
