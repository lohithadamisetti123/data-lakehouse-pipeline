# Delta Lake Transaction Log Analysis

## Navigating the _delta_log

After running the pipeline for multiple days, the Silver table lives under `data/lakehouse/silver`. Inside it, the `_delta_log` directory contains a sequence of JSON files such as `00000000000000000000.json`, `00000000000000000001.json`, etc. Each file represents a committed transaction. Together with occasional checkpoint Parquet files, this ordered log is the single source of truth for the table state.[web:16][web:19]

When you open one of these JSON commit files, you will see one JSON object per line. Each line is an **action** describing part of the table’s state, for example a new data file being added, an old file being removed, or metadata being updated.[web:12]

## Deconstructing a Commit File

A typical commit file contains several top‑level keys across its lines:

- `commitInfo`: Describes the transaction itself (operation type, user, timestamp, job id). It records that “a write of type X happened at time T with options O”, making the log auditable over time.[web:16]
- `protocol`: Captures the reader and writer protocol versions required to interpret the table. This ensures that new writers cannot accidentally write incompatible data for older readers, and vice versa.[web:12]
- `metaData`: Defines table‑wide properties: schema (column names and types), partition columns, table name, and configuration options. When schema evolves (e.g., we add `device_fingerprint`), the new schema appears here in a later commit.[web:12]
- `add`: Represents a new data file added to the table. Each `add` action includes the file path, size, partition values, statistics (like min/max for columns), and a modification timestamp. Query engines reconstruct the current table by combining all active `add` actions that have not been superseded by later `remove` actions.[web:12][web:7]
- `remove`: Marks a file as no longer part of the latest snapshot. The physical file may remain in storage until vacuumed, but logically it is not visible at the latest version.[web:12]

By replaying all `add` and `remove` actions in version order, Delta Lake can reconstruct the table’s state at any version, which is what enables time travel.[web:16][web:19]

## How Delta Lake Provides ACID on Object Storage

### Atomicity

Each transaction writes its intent into a new log file with a monotonically increasing version number, for example `00000000000000000005.json`. To commit, the writer performs an atomic “put‑if‑absent” (or lease‑based) operation when creating this file. Either the log file appears completely, or not at all—there is no partial commit. If two writers race to claim version 5, only one succeeds, and the other must retry with a higher version after re‑reading the log.[web:19]

### Consistency

Because readers always reason over a concrete version number of the log, they see a self‑consistent snapshot. They may start from the latest version at the time they begin a query, or explicitly request an older version (time travel). The log guarantees that all actions needed to reconstruct that snapshot are present and ordered.[web:16]

Schema enforcement also contributes to consistency: writers that attempt to write data not matching the table schema will fail, unless schema evolution is explicitly requested (such as `schema_mode="merge"` in delta‑rs). This prevents “silent” corrupt writes that would otherwise break downstream queries.[web:12]

### Isolation

Delta Lake uses **optimistic concurrency control** rather than traditional locks. Writers read the current table state, plan their changes, and then attempt to append a new log version. If another transaction commits first, the second writer detects that the log has advanced (a conflict) and must re‑read and re‑plan. Readers operate on immutable snapshots (versions), so they never block writers and are never blocked by them.[web:19]

This snapshot‑isolation style behavior means that multiple readers and writers can operate concurrently on the same table without interfering with each other, while each reader always sees a stable view of the data.[web:16]

### Durability

Once a commit file and its referenced Parquet data files are written to durable object storage (local disk, S3, etc.), they persist until explicitly deleted (e.g., via vacuum). Because the transaction log is append‑only and versioned, recovering from crashes is as simple as re‑reading the highest available version and replaying its actions. Even if a compute node fails mid‑query, the committed versions remain intact.[web:12][web:19]

## Summary

The `_delta_log` directory encodes the full evolution of a Delta table as an ordered stream of actions. Commit files contain `commitInfo`, `protocol`, `metaData`, and `add`/`remove` records, which together let engines reconstruct any historical snapshot. Using an atomic, versioned log with optimistic concurrency control allows Delta Lake to provide ACID guarantees on top of simple object storage, without relying on heavyweight database locks.
