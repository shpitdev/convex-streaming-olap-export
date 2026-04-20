from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Optional

import requests
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


spark = SparkSession.builder.getOrCreate()

CDC_SOURCE_ID = "_cdc_source_id"
CDC_SOURCE_COMPONENT = "_cdc_source_component"
CDC_SOURCE_TABLE = "_cdc_source_table"
CDC_DOCUMENT_ID = "_cdc_document_id"
CDC_SEQUENCE_NUM = "_cdc_sequence_num"
CDC_IS_DELETED = "_cdc_is_deleted"
CDC_SCHEMA_FINGERPRINT = "_cdc_schema_fingerprint"
CDC_RAW_DOCUMENT_JSON = "_cdc_raw_document_json"
CDC_INGESTED_AT = "_cdc_ingested_at"
CDC_RUN_ID = "_cdc_run_id"
CDC_CREATION_TIME = "_cdc_creation_time"


def env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"missing required env var {name}")
    return value


def opt(value: Optional[str], env_name: str, default: Optional[str] = None) -> str:
    if value is not None:
        return value
    return env(env_name, default)


@dataclass
class Checkpoint:
    phase: str
    snapshot_ts: Optional[int] = None
    snapshot_cursor: Optional[str] = None
    delta_cursor: Optional[int] = None
    schema_hash: Optional[str] = None


class ConvexClient:
    def __init__(self, deployment_url: str, deploy_key: str):
        self.base_url = deployment_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Convex {deploy_key}"})

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        response = self.session.get(
            f"{self.base_url}/{path.lstrip('/')}",
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def json_schemas(self, *, delta_schema: bool = True) -> dict[str, Any]:
        params: dict[str, Any] = {"format": "json"}
        if delta_schema:
            params["deltaSchema"] = "true"
        return self._get("api/json_schemas", params)

    def list_snapshot(
        self,
        *,
        snapshot: Optional[int],
        cursor: Optional[str],
        table_name: Optional[str],
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"format": "json"}
        if snapshot is not None:
            params["snapshot"] = snapshot
        if cursor is not None:
            params["cursor"] = cursor
        if table_name:
            params["tableName"] = table_name
        return self._get("api/list_snapshot", params)

    def document_deltas(
        self,
        *,
        cursor: int,
        table_name: Optional[str],
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "format": "json",
            "cursor": cursor,
        }
        if table_name:
            params["tableName"] = table_name
        return self._get("api/document_deltas", params)


def sha256_json(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def schema_fingerprints(payload: dict[str, Any]) -> dict[str, str]:
    return {
        table_name: sha256_json(schema)
        for table_name, schema in payload.items()
        if not table_name.startswith("$")
    }


def qualify(catalog: Optional[str], schema: str, table: str) -> str:
    if catalog:
        return f"`{catalog}`.`{schema}`.`{table}`"
    return f"`{schema}`.`{table}`"


def ensure_schema(catalog: Optional[str], schema: str) -> None:
    if catalog:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    else:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")


def ensure_control_table(table_name: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          source_id STRING NOT NULL,
          phase STRING NOT NULL,
          snapshot_ts BIGINT,
          snapshot_cursor STRING,
          delta_cursor BIGINT,
          schema_hash STRING,
          run_id STRING NOT NULL,
          updated_at TIMESTAMP NOT NULL
        )
        USING DELTA
        """
    )


def load_checkpoint(source_id: str, control_table: str) -> Optional[Checkpoint]:
    if not spark.catalog.tableExists(control_table):
        return None

    rows = (
        spark.table(control_table)
        .where(F.col("source_id") == source_id)
        .orderBy(F.col("updated_at").desc(), F.col("run_id").desc())
        .limit(1)
        .collect()
    )
    if not rows:
        return None

    row = rows[0]
    return Checkpoint(
        phase=row["phase"],
        snapshot_ts=row["snapshot_ts"],
        snapshot_cursor=row["snapshot_cursor"],
        delta_cursor=row["delta_cursor"],
        schema_hash=row["schema_hash"],
    )


def save_checkpoint(source_id: str, control_table: str, checkpoint: Checkpoint, run_id: str) -> None:
    frame = build_checkpoint_frame(
        [
            {
                "source_id": source_id,
                "phase": checkpoint.phase,
                "snapshot_ts": checkpoint.snapshot_ts,
                "snapshot_cursor": checkpoint.snapshot_cursor,
                "delta_cursor": checkpoint.delta_cursor,
                "schema_hash": checkpoint.schema_hash,
                "run_id": run_id,
            }
        ]
    ).withColumn("updated_at", F.current_timestamp())

    frame.write.format("delta").mode("append").saveAsTable(control_table)


def sanitize_identifier(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_")
    if not sanitized:
        sanitized = "root"
    if sanitized[0].isdigit():
        sanitized = f"t_{sanitized}"
    return sanitized.lower()


def bronze_table_name(component_path: str, table_name: str) -> str:
    component = sanitize_identifier(component_path or "app")
    table = sanitize_identifier(table_name)
    return f"convex__{component}__{table}__cdc"


def normalize_field_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return value


def infer_spark_type(values: list[Any]):
    non_null = [value for value in values if value is not None]
    if not non_null:
        return StringType()

    seen_types = {type(value) for value in non_null}
    if seen_types == {bool}:
        return BooleanType()
    if seen_types.issubset({int, bool}):
        return LongType()
    if seen_types.issubset({int, float, bool}):
        return DoubleType()
    return StringType()


def build_explicit_frame(rows: list[dict[str, Any]]) -> DataFrame:
    field_names = sorted({key for row in rows for key in row.keys()})
    struct_fields = []
    normalized_rows = []

    for field_name in field_names:
        column_values = [row.get(field_name) for row in rows]
        struct_fields.append(StructField(field_name, infer_spark_type(column_values), True))

    schema = StructType(struct_fields)

    for row in rows:
        normalized = []
        for field in struct_fields:
            value = row.get(field.name)
            if value is None:
                normalized.append(None)
            elif isinstance(field.dataType, BooleanType):
                normalized.append(bool(value))
            elif isinstance(field.dataType, LongType):
                normalized.append(int(value))
            elif isinstance(field.dataType, DoubleType):
                normalized.append(float(value))
            else:
                normalized.append(str(value))
        normalized_rows.append(tuple(normalized))

    return spark.createDataFrame(normalized_rows, schema=schema)


def build_checkpoint_frame(rows: list[dict[str, Any]]) -> DataFrame:
    schema = StructType(
        [
            StructField("source_id", StringType(), False),
            StructField("phase", StringType(), False),
            StructField("snapshot_ts", LongType(), True),
            StructField("snapshot_cursor", StringType(), True),
            StructField("delta_cursor", LongType(), True),
            StructField("schema_hash", StringType(), True),
            StructField("run_id", StringType(), False),
        ]
    )

    normalized_rows = []
    for row in rows:
        normalized_rows.append(
            (
                str(row["source_id"]),
                str(row["phase"]),
                int(row["snapshot_ts"]) if row.get("snapshot_ts") is not None else None,
                str(row["snapshot_cursor"]) if row.get("snapshot_cursor") is not None else None,
                int(row["delta_cursor"]) if row.get("delta_cursor") is not None else None,
                str(row["schema_hash"]) if row.get("schema_hash") is not None else None,
                str(row["run_id"]),
            )
        )

    return spark.createDataFrame(normalized_rows, schema=schema)


def normalize_event(value: dict[str, Any], table_schema_fingerprint: Optional[str], source_id: str) -> dict[str, Any]:
    is_deleted = bool(value.get("_deleted", False))
    document = {
        key: field_value
        for key, field_value in value.items()
        if (not key.startswith("_")) or key == "_creationTime"
    }

    event: dict[str, Any] = {
        CDC_SOURCE_ID: source_id,
        CDC_SOURCE_COMPONENT: value["_component"],
        CDC_SOURCE_TABLE: value["_table"],
        CDC_DOCUMENT_ID: value["_id"],
        CDC_SEQUENCE_NUM: int(value["_ts"]),
        CDC_IS_DELETED: is_deleted,
        CDC_SCHEMA_FINGERPRINT: table_schema_fingerprint,
        CDC_RAW_DOCUMENT_JSON: None if is_deleted else json.dumps(document, sort_keys=True, separators=(",", ":")),
        CDC_INGESTED_AT: None,
        CDC_RUN_ID: None,
    }

    if "_creationTime" in document:
        event[CDC_CREATION_TIME] = document["_creationTime"]

    if not is_deleted:
        for key, field_value in document.items():
            if key == "_creationTime":
                continue
            event[key] = normalize_field_value(field_value)

    return event


def append_events(table_name: str, rows: list[dict[str, Any]], run_id: str, bronze_schema_name: str, catalog: Optional[str]) -> None:
    if not rows:
        return

    bronze_table = qualify(catalog, bronze_schema_name, table_name)
    frame = (
        build_explicit_frame(rows)
        .withColumn(CDC_INGESTED_AT, F.current_timestamp())
        .withColumn(CDC_RUN_ID, F.lit(run_id))
    )
    (
        frame.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(bronze_table)
    )


def append_grouped_events(
    values: list[dict[str, Any]],
    table_fingerprints: dict[str, str],
    source_id: str,
    run_id: str,
    bronze_schema_name: str,
    catalog: Optional[str],
) -> int:
    grouped: dict[str, list[dict[str, Any]]] = {}
    event_count = 0

    for value in values:
        normalized = normalize_event(
            value,
            table_fingerprints.get(value["_table"]),
            source_id,
        )
        target = bronze_table_name(value["_component"], value["_table"])
        grouped.setdefault(target, []).append(normalized)
        event_count += 1

    for target, rows in grouped.items():
        append_events(target, rows, run_id, bronze_schema_name, catalog)

    return event_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convex CDC Databricks extractor")
    parser.add_argument("--deployment-url")
    parser.add_argument("--deploy-key")
    parser.add_argument("--source-id")
    parser.add_argument("--table-name")
    parser.add_argument("--catalog")
    parser.add_argument("--control-schema")
    parser.add_argument("--bronze-schema")
    parser.add_argument("--checkpoint-table")
    args, _ = parser.parse_known_args()
    return args


def run_snapshot_until_delta(
    client: ConvexClient,
    *,
    initial_snapshot: Optional[int],
    initial_cursor: Optional[str],
    table_name: Optional[str],
    table_fingerprints: dict[str, str],
    source_id: str,
    run_id: str,
    bronze_schema: str,
    catalog: Optional[str],
    control_table: str,
    schema_hash: str,
) -> Checkpoint:
    snapshot = initial_snapshot
    cursor = initial_cursor

    while True:
        response = client.list_snapshot(
            snapshot=snapshot,
            cursor=cursor,
            table_name=table_name,
        )
        append_grouped_events(
            response["values"],
            table_fingerprints,
            source_id,
            run_id,
            bronze_schema,
            catalog,
        )

        if response["hasMore"]:
            checkpoint = Checkpoint(
                phase="initial_snapshot",
                snapshot_ts=response["snapshot"],
                snapshot_cursor=response["cursor"],
                schema_hash=schema_hash,
            )
            save_checkpoint(source_id, control_table, checkpoint, run_id)
            snapshot = response["snapshot"]
            cursor = response["cursor"]
            continue

        checkpoint = Checkpoint(
            phase="delta_tail",
            delta_cursor=response["snapshot"],
            schema_hash=schema_hash,
        )
        save_checkpoint(source_id, control_table, checkpoint, run_id)
        return checkpoint


def run_once() -> None:
    args = parse_args()

    deployment_url = opt(args.deployment_url, "CONVEX_DEPLOYMENT_URL")
    deploy_key = opt(args.deploy_key, "CONVEX_DEPLOY_KEY")
    source_id = opt(args.source_id, "CONVEX_SOURCE_ID", deployment_url)
    table_name = args.table_name if args.table_name is not None else os.getenv("CONVEX_TABLE_NAME")

    catalog = args.catalog if args.catalog is not None else os.getenv("DATABRICKS_CATALOG")
    control_schema = opt(args.control_schema, "DATABRICKS_CONTROL_SCHEMA", "control")
    bronze_schema = opt(args.bronze_schema, "DATABRICKS_BRONZE_SCHEMA", "bronze")
    checkpoint_table_name = opt(args.checkpoint_table, "DATABRICKS_CHECKPOINT_TABLE", "connector_checkpoint")
    control_table = qualify(catalog, control_schema, checkpoint_table_name)

    ensure_schema(catalog, control_schema)
    ensure_schema(catalog, bronze_schema)
    ensure_control_table(control_table)

    client = ConvexClient(deployment_url, deploy_key)
    schema_payload = client.json_schemas(delta_schema=True)
    table_fingerprints = schema_fingerprints(schema_payload)
    global_schema_hash = sha256_json(schema_payload)
    run_id = str(int(time.time() * 1000))

    checkpoint = load_checkpoint(source_id, control_table)

    if checkpoint is None:
        checkpoint = run_snapshot_until_delta(
            client,
            initial_snapshot=None,
            initial_cursor=None,
            table_name=table_name,
            table_fingerprints=table_fingerprints,
            source_id=source_id,
            run_id=run_id,
            bronze_schema=bronze_schema,
            catalog=catalog,
            control_table=control_table,
            schema_hash=global_schema_hash,
        )
    elif checkpoint.phase == "initial_snapshot":
        checkpoint = run_snapshot_until_delta(
            client,
            initial_snapshot=checkpoint.snapshot_ts,
            initial_cursor=checkpoint.snapshot_cursor,
            table_name=table_name,
            table_fingerprints=table_fingerprints,
            source_id=source_id,
            run_id=run_id,
            bronze_schema=bronze_schema,
            catalog=catalog,
            control_table=control_table,
            schema_hash=global_schema_hash,
        )

    assert checkpoint is not None
    if checkpoint.delta_cursor is None:
        raise RuntimeError("checkpoint missing delta cursor after snapshot handoff")

    cursor = checkpoint.delta_cursor

    while True:
        response = client.document_deltas(cursor=cursor, table_name=table_name)
        append_grouped_events(
            response["values"],
            table_fingerprints,
            source_id,
            run_id,
            bronze_schema,
            catalog,
        )

        cursor = int(response["cursor"])
        checkpoint = Checkpoint(
            phase="delta_tail",
            delta_cursor=cursor,
            schema_hash=global_schema_hash,
        )
        save_checkpoint(source_id, control_table, checkpoint, run_id)

        if not response["hasMore"]:
            break


if __name__ == "__main__":
    run_once()
