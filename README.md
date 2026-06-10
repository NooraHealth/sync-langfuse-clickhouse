# Langfuse ClickHouse → BigQuery Pipeline

Incrementally loads **traces** and **scores** from Langfuse's internal ClickHouse database into BigQuery using [dlt](https://dlthub.com).

---

## How it works

The pipeline connects directly to Langfuse's ClickHouse database over HTTP and reads the `traces` and `scores` tables. On the first run it loads all records from the `_INITIAL_VALUE` date onwards. On every subsequent run it only fetches rows where `updated_at` is newer than the last run — no full table scans.

| Table | Incremental cursor | Primary key | Write mode |
|---|---|---|---|
| `traces` | `updated_at` | `(id, project_id)` | merge (upsert) |
| `scores` | `updated_at` | `(id, project_id)` | merge (upsert) |

Rows with `is_deleted = 1` are excluded. ClickHouse's `ReplacingMergeTree` duplicates are collapsed using `FINAL` before reading.

---

## Project structure

```
.
├── .dlt/
│   ├── config.toml       # non-sensitive config (committed to git)
│   └── secrets.toml      # credentials (gitignored)
├── clickhouse_bigquery_pipeline.py
├── requirements.txt
└── README.md
```

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure `.dlt/config.toml`

```toml
[sources.langfuse_clickhouse_source]
host       = "your-clickhouse-host"
port       = 8123
database   = "default"
project_id = ""            # optional: single Langfuse project UUID; leave empty for all projects
```

### 3. Configure `.dlt/secrets.toml`

```toml
[sources.langfuse_clickhouse_source]
username = "default"
password = "your-clickhouse-password"

[destination.bigquery.credentials]
project_id   = "your-gcp-project-id"
client_email = "your-service-account@your-gcp-project-id.iam.gserviceaccount.com"
private_key  = "-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----\n"
```

> **Note:** `secrets.toml` is gitignored and should never be committed. Share credentials with teammates via a password manager or GCP Secret Manager.
>
> **Alternative:** Instead of inlining BigQuery credentials, set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable pointing to a service account JSON file.

### 4. Run

```bash
python clickhouse_bigquery_pipeline.py
```

---

## BigQuery output

Data is written to the `clickhouse-co-pilot` dataset. After the first run you will see:

- `clickhouse-co-pilot.traces` — one row per Langfuse trace
- `clickhouse-co-pilot.scores` — one row per evaluation score

A `langfuse_staging` dataset is also created automatically by dlt as a temporary landing zone during the merge — this is expected and can be ignored.

---

## Columns loaded

### traces

| Column | Type | Notes |
|---|---|---|
| `id` | string | primary key |
| `timestamp` | datetime | business timestamp |
| `name` | string | |
| `user_id` | string | |
| `metadata` | string | JSON string |
| `release` | string | |
| `version` | string | |
| `project_id` | string | primary key |
| `public` | bool | |
| `bookmarked` | bool | |
| `tags` | string | ClickHouse array serialised as string |
| `input` | string | JSON string |
| `output` | string | JSON string |
| `session_id` | string | |
| `environment` | string | |
| `is_deleted` | int | always 0 (deleted rows excluded) |
| `created_at` | datetime | |
| `updated_at` | datetime | incremental cursor |
| `event_ts` | datetime | ClickHouse ingestion timestamp |

### scores

| Column | Type | Notes |
|---|---|---|
| `id` | string | primary key |
| `timestamp` | datetime | business timestamp |
| `project_id` | string | primary key |
| `trace_id` | string | |
| `observation_id` | string | |
| `name` | string | score name |
| `value` | float | numeric/boolean score value |
| `source` | string | |
| `comment` | string | |
| `author_user_id` | string | |
| `config_id` | string | |
| `data_type` | string | NUMERIC, BOOLEAN, or CATEGORICAL |
| `string_value` | string | label for CATEGORICAL scores |
| `long_string_value` | string | |
| `queue_id` | string | |
| `environment` | string | |
| `session_id` | string | |
| `dataset_run_id` | string | |
| `metadata` | string | JSON string |
| `is_deleted` | int | always 0 (deleted rows excluded) |
| `execution_trace_id` | string | |
| `created_at` | datetime | |
| `updated_at` | datetime | incremental cursor |
| `event_ts` | datetime | ClickHouse ingestion timestamp |

---

## Scheduling

Run on any scheduler at your desired cadence. Example cron for every 15 minutes:

```cron
*/15 * * * * cd /path/to/project && python clickhouse_bigquery_pipeline.py >> logs/pipeline.log 2>&1
```

---

## Resetting the pipeline

To do a full reload from scratch (e.g. after changing the initial date or column list):

```bash
# Delete dlt's stored state so the cursor resets
rm -rf .dlt/pipelines/langfuse_clickhouse_pipeline

# Then re-run
python clickhouse_bigquery_pipeline.py
```
