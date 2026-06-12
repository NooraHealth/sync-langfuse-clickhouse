from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator
import clickhouse_connect
import dlt
import os
import yaml


def get_params(source_name, params_path="params.yaml", envir=None):
    with open(params_path) as f:
        all_params = yaml.safe_load(f)

    if envir is None:
        github_ref_name = os.getenv("GITHUB_REF_NAME")
        envir = "prod" if github_ref_name == "main" else "dev"

    print(f"env='{envir}' | source='{source_name}'")

    params = all_params[envir][source_name]
    return params


def get_import_schema():
    import_schema_path = Path("schemas/import") / "langfuse_clickhouse_source.schema.yaml"
    with import_schema_path.open() as f:
        import_schema = yaml.safe_load(f)
    return import_schema


def get_columns_from_schema(table: str) -> list[str]:
    # Columns that are Maps/Arrays in ClickHouse and must be cast to string.
    # All other columns are selected as-is by their name from the schema file.
    cast_columns = {
        "metadata": "toString(metadata) as metadata",
        "tags": "toString(tags) as tags",
        "input": "toString(input) as input",
        "output": "toString(output) as output",
    }
    import_schema = get_import_schema()
    columns = import_schema["tables"][table]["columns"].keys()
    return [cast_columns.get(col, col) for col in columns]


# Helpers
def fetch_all(client, table: str, columns: list[str], cursor: datetime) -> list[dict]:
    col_sql = ",\n            ".join(columns)
    query = f"""
        select
          {col_sql}
        from {table}
        where updated_at > %(cursor)s
        order by updated_at
    """
    result = client.query(query, parameters={"cursor": cursor})
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


def make_resource(table: str, start_date, client):
    columns = get_columns_from_schema(table)

    @dlt.resource(
        name=table,
        table_name=table,
        primary_key=["id", "project_id"],
        write_disposition="merge")
    def resource(
        updated_at: dlt.sources.incremental[datetime] = dlt.sources.incremental(
            "updated_at", initial_value=start_date)) -> Iterator[list]:
        rows = fetch_all(client, table, columns, updated_at.last_value)
        if rows:
            yield rows

    resource.__name__ = table
    return resource


# Source
@dlt.source(name="langfuse_clickhouse_source", max_table_nesting=0)
def langfuse_clickhouse_source(
    tables,
    start_date,
    host: str = dlt.config.value,
    port: int = dlt.config.value,
    database: str = dlt.config.value,
    username: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
) -> list:
    client = clickhouse_connect.get_client(
        host=host, port=port, database=database, username=username, password=password)
    result = [make_resource(table, start_date, client) for table in tables]
    return result


# Pipeline
def run() -> None:
    params = get_params("langfuse_clickhouse")
    start_date = datetime.strptime(
      params["start_date"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

    import_schema = get_import_schema()
    tables = [t for t in import_schema["tables"] if not t.startswith("_dlt")]

    pipeline = dlt.pipeline(
        pipeline_name=params["pipeline_name"],
        destination="bigquery",
        dataset_name=params["dataset_name"],
        progress="log",
        import_schema_path="schemas/import",
        export_schema_path="schemas/export",
    )
    load_info = pipeline.run(
        langfuse_clickhouse_source(tables=tables, start_date=start_date))
    print(load_info)


if __name__ == "__main__":
    run()
