
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator
import clickhouse_connect
import argparse
import dlt
import tomllib
import os
import slack_sdk
import yaml
import time


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
    cast_columns = {
        "metadata": "toJSONString(metadata) as metadata",
        "tags": "toString(tags) as tags",
        "input": "toString(input) as input",
        "output": "toString(output) as output",
    }
    import_schema = get_import_schema()
    columns = import_schema["tables"][table_name]["columns"].keys()
    return [cast_columns.get(col, col) for col in columns]


def fetch_rows(client, table_name: str, columns: list[str], cursor, batch_size) -> list[dict]:
    col_sql = ", ".join(columns)
    query = f"select {col_sql} from {table_name} where updated_at > %(cursor)s order by updated_at limit %(batch_size)s"
    print(f"{query=}")
    print(f"{cursor=}")
    result = client.query(query, parameters={"cursor": cursor, "batch_size": batch_size})
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


def make_resource(table_name: str, start_date: datetime | None, client, max_attempts, delays):
    columns = get_columns_from_schema(table_name)

    @dlt.resource(
        name=table_name,
        table_name=table_name,
        primary_key=["id", "project_id"],
        write_disposition="merge")
    def resource(updated_at=dlt.sources.incremental("updated_at", initial_value=start_date)) -> Iterator[list]:
        batch_size = 5000

        page_cursor = updated_at.last_value
        today = datetime.now(timezone.utc).date()

        for attempt in range(1, max_attempts + 1):

            while (rows := fetch_rows(client, table_name, columns, page_cursor, batch_size)):
                yield rows
                page_cursor = rows[-1]["updated_at"]
                if len(rows) < batch_size:
                    break

            max_date = page_cursor.date() if page_cursor else None
            print(f"{table_name=} {attempt=} {page_cursor =} {max_date=} {today=}")

            if max_date == today:
                print(f"Data for today loaded after {attempt=} attempts.")
                break

            if attempt == max_attempts:
                print(
                    f"WARNING: {table_name=} did not reach today's data after "
                    f"{max_attempts} attempts. Last page_cursor.date ={page_cursor.date}."
                )
                break

            print(
                f"{table_name=} page_cursor.date date ({max_date}) is not today ({today}); "
                f"retrying (attempt {attempt + 1}/{max_attempts}) after {delays}s"
            )
            time.sleep(delays)

    resource.__name__ = table_name
    return resource


@dlt.source(name="langfuse_clickhouse_source", max_table_nesting=0)
def langfuse_clickhouse_source(
    table_names,
    start_date: datetime | None,
    max_attempts,
    delay,
    host: str = dlt.config.value,
    port: int = dlt.config.value,
    database: str = dlt.config.value,
    username: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
) -> list:
    client = clickhouse_connect.get_client(
        host=host, port=port, database=database, username=username, password=password)
    return [make_resource(table_name, start_date, client, max_attempts, delay) for table_name in table_names]


def get_slack_message_text(e, pipeline_name):
    text = (
      f":warning: Sync for *{pipeline_name}* failed with the following error:"
      f"\n\n`{str(e)}`"
    )
    run_url = os.getenv("RUN_URL")
    if run_url is not None:
        text += f"\n\nPlease see the GitHub Actions <{run_url}|workflow run log>."
    return text


def send_message_to_slack(text, channel_id):
    with open(Path(".dlt/secrets.toml"), "rb") as f:
        token = tomllib.load(f)["slack_token"]
    client = slack_sdk.WebClient(token=token)
    try:
        client.chat_postMessage(channel=channel_id, text=text)
    except Exception as e:
        raise e


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--use-start-date', type=str, default='False')
    return parser.parse_args()


def run(params) -> None:
    print(f"{params=}")
    args = parse_args()

    if args.use_start_date == 'True':
        start_date = datetime.strptime(
            params["start_date"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    else:
        start_date = None
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
    )

    pipeline.sync_destination()

    load_info = pipeline.run(langfuse_clickhouse_source(table_names=table_names, start_date=start_date, max_attempts=params["max_attempts"], delay=params["delay"]))
    print(load_info)


if __name__ == "__main__":
    try:
        params = get_params("langfuse_clickhouse")
        run(params)
    except Exception as e:
        if os.getenv("GITHUB_REF_NAME") == "main":
            text = get_slack_message_text(e, params["pipeline_name"])
            send_message_to_slack(text, params["slack_channel_id"])
        raise e