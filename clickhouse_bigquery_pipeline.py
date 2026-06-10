from datetime import datetime
from typing import Iterator, Optional

import dlt

#start date for import
_INITIAL_VALUE = datetime.strptime("2026-06-03 06:00:00", "%Y-%m-%d %H:%M:%S")

#list of all columns to be imported
_TABLES: dict[str, list[str]] = {
    "traces": [
        "id", "timestamp", "name", "user_id",
        "toString(metadata) AS metadata",
        "release", "version", "project_id", "public", "bookmarked",
        "toString(tags) AS tags",
        "toString(input)  AS input",
        "toString(output) AS output",
        "session_id", "environment", "is_deleted",
        "created_at", "updated_at", "event_ts",
    ],
    "scores": [
        "id", "timestamp", "project_id", "trace_id", "observation_id",
        "name", "value", "source", "comment", "author_user_id", "config_id",
        "data_type", "string_value", "long_string_value", "queue_id",
        "environment", "session_id", "dataset_run_id",
        "toString(metadata) AS metadata",
        "is_deleted", "execution_trace_id",
        "created_at", "updated_at", "event_ts",
    ],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_client(host: str, port: int, database: str,
                username: str, password: str):
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=host, port=port, database=database,
        username=username, password=password,
        compress=True,
    )


def _fetch_all(client, table: str, columns: list[str],
               cursor: datetime, project_filter: str) -> list[dict]:
    col_sql = ",\n            ".join(columns)
    query = f"""
        SELECT
            {col_sql}
        FROM {table} FINAL
        WHERE updated_at > %(cursor)s
          AND is_deleted = 0
          {project_filter}
        ORDER BY updated_at ASC
    """
    result = client.query(query, parameters={"cursor": cursor})
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


def _make_resource(table: str, client, project_filter: str):
    columns = _TABLES[table]

    @dlt.resource(
        name=table,
        table_name=table,
        primary_key=["id", "project_id"],
        write_disposition="merge",
    )
    def _resource(
        updated_at: dlt.sources.incremental[datetime] = dlt.sources.incremental(
            "updated_at",
            initial_value=_INITIAL_VALUE,
        ),
    ) -> Iterator[list]:
        rows = _fetch_all(
            client, table, columns,
            updated_at.last_value, project_filter,
        )
        if rows:
            yield rows

    _resource.__name__ = table
    return _resource


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------

@dlt.source(name="langfuse_clickhouse_source", max_table_nesting=0)
def langfuse_clickhouse_source(
    host: str = dlt.config.value,
    port: int = dlt.config.value,
    database: str = dlt.config.value,
    username: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    project_id: Optional[str] = dlt.config.value,
) -> list:
    client = _get_client(host, port, database, username, password)
    project_filter = f"AND project_id = '{project_id}'" if project_id else ""

    return [
        _make_resource(table, client, project_filter)
        for table in _TABLES
    ]


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="langfuse_clickhouse_pipeline",
        destination="bigquery",
        dataset_name="clickhouse-co-pilot",
        progress="log",
        export_schema_path="schemas/export",
    )
    load_info = pipeline.run(langfuse_clickhouse_source())
    print(load_info)


if __name__ == "__main__":
    run()
