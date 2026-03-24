import argparse
import os

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from .ingest import load_json_gz, normalize_record
from .models import GitHubEvent


def get_paths(base_dir: str):
    bronze_path = os.path.join(base_dir, "data", "lakehouse", "bronze")
    silver_path = os.path.join(base_dir, "data", "lakehouse", "silver")
    silver_corrected_path = os.path.join(base_dir, "data", "lakehouse", "silver_corrected")
    day5_source = os.path.join(base_dir, "data", "source", "day_5.json.gz")
    return bronze_path, silver_path, silver_corrected_path, day5_source


def find_latest_version(table_path: str) -> int:
    dt = DeltaTable(table_path)
    history = dt.history()  # list of dicts in delta-rs Python[web:20][web:12]
    versions = [int(h["version"]) for h in history]
    return max(versions)


def correct_day5(day5_source: str):
    records = []
    for raw in load_json_gz(day5_source):
        try:
            event = GitHubEvent.model_validate(raw)
            records.append(normalize_record(event))
        except Exception:
            continue

    if not records:
        raise RuntimeError("No valid records in corrected Day 5")
    df = pd.DataFrame(records)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    return df


def main():
    parser = argparse.ArgumentParser(
        description="Create corrected Silver table using time travel."
    )
    parser.add_argument(
        "--pre_bad_version",
        type=int,
        help="Silver table version before bad batch (optional, auto-detect if omitted).",
    )
    args = parser.parse_args()

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    bronze_path, silver_path, silver_corrected_path, day5_source = get_paths(base_dir)

    if args.pre_bad_version is None:
        latest = find_latest_version(silver_path)
        pre_bad_version = latest - 1
    else:
        pre_bad_version = args.pre_bad_version

    # Time travel to pre-bad version
    dt = DeltaTable(silver_path, version=pre_bad_version)
    pre_bad_df = dt.to_pandas()

    # Correct Day 5
    day5_df = correct_day5(day5_source)

    combined = pd.concat([pre_bad_df, day5_df], ignore_index=True)
    combined = combined.sort_values("created_at").drop_duplicates(
        subset=["id"], keep="last"
    )

    os.makedirs(silver_corrected_path, exist_ok=True)
    write_deltalake(silver_corrected_path, combined, mode="overwrite")
    print(
        f"Wrote corrected silver table with {len(combined)} rows "
        f"at {silver_corrected_path}"
    )


if __name__ == "__main__":
    main()
