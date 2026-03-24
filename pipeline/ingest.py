import argparse
import gzip
import json
import os

import duckdb
import pandas as pd
from deltalake import write_deltalake
from pydantic import ValidationError

from .models import GitHubEvent


def get_paths(base_dir: str, day: int):
    source_file = os.path.join(base_dir, "data", "source", f"day_{day}.json.gz")
    bronze_path = os.path.join(base_dir, "data", "lakehouse", "bronze")
    silver_path = os.path.join(base_dir, "data", "lakehouse", "silver")
    return source_file, bronze_path, silver_path


def load_json_gz(filepath: str):
    with gzip.open(filepath, "rt", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            yield json.loads(line)


def normalize_record(event: GitHubEvent) -> dict:
    actor = event.actor.dict() if event.actor else {}
    repo = event.repo.dict() if event.repo else {}
    return {
        "id": event.id,
        "type": event.type,
        "created_at": event.created_at,
        "actor_id": actor.get("id"),
        "actor_login": actor.get("login"),
        "repo_id": repo.get("id"),
        "repo_name": repo.get("name"),
        "device_fingerprint": event.device_fingerprint,
    }


def bronze_ingest(source_file: str, bronze_path: str, day: int):
    records = []
    for raw in load_json_gz(source_file):
        try:
            if day == 5 and "actor" in raw and isinstance(raw["actor"], dict):
                login = raw["actor"].get("login")
                if login is not None:
                    raw["actor"]["login"] = f"corrupted_{hash(login)}"
            event = GitHubEvent.model_validate(raw)
            records.append(normalize_record(event))
        except ValidationError:
            continue

    if not records:
        print(f"No valid records for day {day}")
        return

    df = pd.DataFrame(records)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    for col in ["id", "type", "actor_login", "repo_name", "device_fingerprint"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    for col in ["actor_id", "repo_id"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    kwargs = {}
    if day >= 3:
        kwargs["schema_mode"] = "merge"

    mode = "append"
    if not os.path.exists(bronze_path):
        mode = "overwrite"

    os.makedirs(bronze_path, exist_ok=True)
    write_deltalake(bronze_path, df, mode=mode, **kwargs)
    print(f"Bronze ingestion completed for day {day}, rows={len(df)}")


def silver_upsert(bronze_path: str, silver_path: str):
    """
    Build Silver from scratch each time from the full Bronze table:
    - read all Bronze
    - deduplicate by id (keep last)
    - enforce fixed column order and dtypes
    - overwrite Silver table.
    This avoids schema mismatches between writes.[web:20][web:21]
    """
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    bronze_df = con.execute(
        "SELECT * FROM delta_scan(?)", [bronze_path]
    ).fetch_df()

    bronze_df = bronze_df.sort_values("created_at").drop_duplicates(
        subset=["id"], keep="last"
    )

    # Ensure consistent column order
    cols = [
        "id",
        "type",
        "created_at",
        "actor_id",
        "actor_login",
        "repo_id",
        "repo_name",
        "device_fingerprint",
    ]
    for col in cols:
        if col not in bronze_df.columns:
            bronze_df[col] = pd.NA

    bronze_df = bronze_df[cols]

    # Enforce dtypes again to avoid any Null type issues
    bronze_df["created_at"] = pd.to_datetime(bronze_df["created_at"], errors="coerce")
    for col in ["id", "type", "actor_login", "repo_name", "device_fingerprint"]:
        bronze_df[col] = bronze_df[col].astype("string")
    for col in ["actor_id", "repo_id"]:
        bronze_df[col] = pd.to_numeric(bronze_df[col], errors="coerce").astype("Int64")

    os.makedirs(silver_path, exist_ok=True)
    write_deltalake(silver_path, bronze_df, mode="overwrite")
    print(f"Silver upsert completed. rows={len(bronze_df)}")


def main():
    parser = argparse.ArgumentParser(
        description="Ingest GitHub Archive day into Bronze and Silver Delta tables."
    )
    parser.add_argument(
        "--day",
        type=int,
        required=True,
        help="Day number (1-5) to ingest.",
    )
    args = parser.parse_args()

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    source_file, bronze_path, silver_path = get_paths(base_dir, args.day)

    if not os.path.exists(source_file):
        raise FileNotFoundError(f"Source file not found: {source_file}")

    bronze_ingest(source_file, bronze_path, args.day)
    silver_upsert(bronze_path, silver_path)


if __name__ == "__main__":
    main()
