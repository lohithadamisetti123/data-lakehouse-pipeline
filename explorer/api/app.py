import json
import os
import pathlib
import tempfile

import duckdb
from deltalake import DeltaTable
from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

load_dotenv()

app = Flask(__name__)
CORS(app, origins=os.getenv("CORS_ORIGINS", "*"))

LAKEHOUSE_BASE_PATH = os.getenv("LAKEHOUSE_BASE_PATH", "/app/data/lakehouse")
UI_BUILD_PATH = pathlib.Path(__file__).resolve().parents[1] / "ui-build"


def get_table_path(table_name: str) -> str:
    if table_name not in {"bronze", "silver", "silver_corrected"}:
        raise ValueError("Unsupported table name")
    return os.path.join(LAKEHOUSE_BASE_PATH, table_name)


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/api/tables/<table_name>/versions", methods=["GET"])
def list_versions(table_name):
    path = get_table_path(table_name)
    dt = DeltaTable(path)
    history = dt.history()  # list[dict] in delta-rs[web:20][web:12]
    items = []
    for h in history:
        ts = h.get("timestamp")
        if hasattr(ts, "timestamp"):
            ts_ms = int(ts.timestamp() * 1000)
        else:
            ts_ms = int(ts)
        items.append(
            {
                "version": int(h["version"]),
                "timestamp": ts_ms,
                "operation": h.get("operation", "WRITE"),
            }
        )
    # newest first, like your current output
    items.sort(key=lambda x: x["version"], reverse=True)
    return jsonify(items), 200


def _version_to_parquet(path: str, version: int) -> str:
    dt = DeltaTable(path, version=version)
    df = dt.to_pandas()
    tmp_dir = tempfile.mkdtemp(prefix="delta_ver_")
    pq_path = os.path.join(tmp_dir, "snapshot.parquet")
    df.to_parquet(pq_path, index=False)
    return pq_path


@app.route("/api/tables/<table_name>/versions/<int:version_id>", methods=["GET"])
def version_metadata(table_name, version_id):
    path = get_table_path(table_name)
    pq_path = _version_to_parquet(path, version_id)

    con = duckdb.connect()
    # plain Parquet scan, no delta_scan or version param
    con.execute(f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet('{pq_path}')")

    row_count = con.execute("SELECT COUNT(*) AS c FROM t").fetchone()[0]
    schema_df = con.execute("DESCRIBE t").fetch_df()
    schema = {row["column_name"]: row["column_type"] for _, row in schema_df.iterrows()}

    return jsonify({"rowCount": int(row_count), "schema": schema}), 200


@app.route("/api/tables/<table_name>/query", methods=["POST"])
def execute_query(table_name):
    data = request.get_json(force=True)
    query = data.get("query")
    version = data.get("version")

    if not query:
        return jsonify({"error": "Query is required"}), 400
    if version is None:
        return jsonify({"error": "Version is required"}), 400

    path = get_table_path(table_name)
    version_int = int(version)
    pq_path = _version_to_parquet(path, version_int)

    con = duckdb.connect()
    con.execute(f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet('{pq_path}')")

    lowered = query.strip().lower()
    if any(word in lowered for word in ["insert", "update", "delete", "drop", "create"]):
        return jsonify({"error": "Only read-only queries are allowed"}), 400

    # replace placeholder 'table' with alias 't'
    safe_query = query.replace("table", "t")
    result_df = con.execute(safe_query).fetch_df()
    records = json.loads(result_df.to_json(orient="records"))
    return jsonify(records), 200


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve_ui(path):
    if not UI_BUILD_PATH.exists():
        return "UI not built", 500
    if path and (UI_BUILD_PATH / path).exists():
        return send_from_directory(UI_BUILD_PATH, path)
    return send_from_directory(UI_BUILD_PATH, "index.html")


if __name__ == "__main__":
    host = os.getenv("FLASK_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_PORT", "8000"))
    app.run(host=host, port=port)
