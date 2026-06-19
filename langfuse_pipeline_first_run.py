from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator
import clickhouse_connect
import dlt
import os
import yaml


def get_params(source_name, params_path="params.yaml", envir=None):
    print(f"{source_name=}")
    with open(params_path) as f:
        all_params = yaml.safe_load(f)

    if envir is None:
        github_ref_name = os.getenv("GITHUB_REF_NAME")
        envir = "prod" if github_ref_name == "main" else "dev"
    print(f"{envir=}")

    params = all_params[source_name][envir]
    return params

def get_import_schema():
    import_schema_path = Path("schemas/import") / "langfuse_clickhouse_source.schema.yaml"
    with import_schema_path.open() as f:
        import_schema = yaml.safe_load(f)
    return import_schema


def get_columns_from_schema(table_name: str) -> list[str]:
    # Columns that are Maps/Arrays in ClickHouse and must be cast to string.
    # All other columns are selected as-is by their name from the schema file.
    cast_columns = {
        "metadata": "toJSONString(metadata) as metadata",
        "tags": "toString(tags) as tags",
        "input": "toString(input) as input",
        "output": "toString(output) as output",
    }
    import_schema = get_import_schema()
    columns = import_schema["tables"][table_name]["columns"].keys()
    return [cast_columns.get(col, col) for col in columns]


# Helpers
def fetch_rows(client, table_name: str, columns: list[str], cursor, batch_size) -> list[dict]:
    col_sql = ", ".join(columns)
    query = f"select {col_sql} from {table_name} where updated_at > %(cursor)s order by updated_at limit %(batch_size)s"
    print(f"{query=}")
    print(f"{cursor=}")
    result = client.query(query, parameters={"cursor": cursor, "batch_size": batch_size})
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


def make_resource(table_name: str, start_date: datetime, client):
    columns = get_columns_from_schema(table_name)

    @dlt.resource(
        name=table_name,
        table_name=table_name,
        primary_key=["id", "project_id"],
        write_disposition="merge")
    def resource(updated_at=dlt.sources.incremental("updated_at", initial_value=start_date)) -> Iterator[list]:
	batch_size = 5000
        page_cursor = updated_at.last_value
        while (rows := fetch_rows(client, table_name, columns, page_cursor, batch_size)):
            yield rows
            if len(rows) < batch_size:
                break
            page_cursor = rows[-1]["updated_at"]

    resource.__name__ = table_name
    return resource


# Source
@dlt.source(name="langfuse_clickhouse_source", max_table_nesting=0)
def langfuse_clickhouse_source(
    table_names,
    start_date,
    host: str = dlt.config.value,
    port: int = dlt.config.value,
    database: str = dlt.config.value,
    username: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
) -> list:
    client = clickhouse_connect.get_client(
        host=host, port=port, database=database, username=username, password=password)
    result = [make_resource(table_name, start_date, client) for table_name in table_names]
    return result


# Pipeline
def run() -> None:
    params = get_params("langfuse_clickhouse")
    print(f"{params=}")
    start_date = datetime.strptime(
        params["start_date"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    print(f"{start_date=}")

    import_schema = get_import_schema()
    table_names = [t for t in import_schema["tables"] if not t.startswith("_dlt")]
    print(f"{table_names=}")

    pipeline = dlt.pipeline(
        pipeline_name=params["pipeline_name"],
        destination=dlt.destinations.bigquery(location=params["location"]),
        dataset_name=params["dataset_name"],
        progress="log",
        import_schema_path="schemas/import",
        export_schema_path="schemas/export",
        pipelines_dir=".dlt/pipelines",
    )
    load_info = pipeline.run(langfuse_clickhouse_source(
        table_names=table_names, start_date=start_date))
    print(load_info)


if __name__ == "__main__":
    run()
