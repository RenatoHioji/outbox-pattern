# Outbox Pattern Study

## PostgreSQL Write-Ahead Log (`wal_level`): `minimal` vs. `replica` vs. `logical`

Before PostgreSQL modifies any table on disk, it first appends every change (`INSERT`, `UPDATE`, `DELETE`) to an internal transaction log called the **Write-Ahead Log (WAL)**. This guarantees durability if the server loses power mid-transaction.

In [docker-compose.yml](./docker-compose.yml), PostgreSQL is started with:

```yaml
command:
  - "postgres"
  - "-c"
  - "wal_level=logical"
```

The `wal_level` setting controls **how much context** PostgreSQL writes to the WAL stream:

### 1. `minimal` (Crash Recovery Only)
- **What it writes:** Only the bare minimum information needed to recover database consistency if the server crashes mid-transaction. For bulk operations (such as `CREATE TABLE AS`, `CLUSTER`, or `COPY` into a newly created table), it can skip writing WAL altogether and simply flush the data files to disk at commit time.
- **What gets recorded on `INSERT`:**
  ```text
  [Physical] Heap Page 42, Offset 3: write 128 bytes (crash recovery records only)
  ```
- **Write Overhead:** Baseline (lowest disk I/O).
- **Limitation:** You **cannot** use read-replicas (standby servers), WAL archiving, or Point-in-Time Recovery (PITR) backups.

### 2. `replica` (PostgreSQL Default)
- **What it writes:** Every **physical disk block change** (for example: *"In relation file `16389`, block `42`, byte offset `3`, write these raw tuple bytes"*).
- **What gets recorded on `INSERT`:**
  ```text
  [Physical] RelNode 16389, Block 42, Offset 3: raw tuple bytes 0x8f14...
  (knows physical disk addresses, NOT logical table/column names)
  ```
- **What uses it:** Read-replicas (physical standby servers) and Point-in-Time Recovery (PITR). Because a physical replica is an exact binary clone of the primary database, it simply copies those raw disk block changes to its own disk files.
- **Limitation:** External tools (such as Debezium / Kafka Connect) cannot decode raw physical disk block addresses into a meaningful JSON event without full access to PostgreSQL's internal catalog state.

### 3. `logical` (Required for Change Data Capture / CDC)
- **What it writes:** Everything included in `replica`, **plus extra catalog/tuple metadata** so PostgreSQL's built-in logical decoding plugin (`pgoutput`) can translate physical disk changes back into **row-level logical events** (such as *"In table `public.outbox_messages`, a row was `INSERT`ed with `id = '8f14...'`, `event_type = 'OrderCreated'`, and `payload = '{\"total\":99}'`"*).
- **What gets recorded on `INSERT`:**
  ```text
  [Physical] Heap Page 42, Offset 3: write 128 bytes
  [Logical]  TABLE public.outbox_messages: INSERT:
               id[uuid]:'8f14..'
               event_type[varchar]:'OrderCreated'
               payload[jsonb]:'{"total":99}'
  ```
- **What uses it:** Logical replication and Change Data Capture (CDC) tools like **Debezium**, which tail the WAL via a replication slot and stream row changes directly to **Apache Kafka**.
- **Trade-off:** Adds roughly **5–10% extra WAL disk I/O and storage overhead** on write-heavy workloads, in exchange for enabling real-time event streaming out of the database without polling `SELECT` queries.

### Summary Comparison

| `wal_level` | Crash Recovery | Physical Read-Replicas & PITR | Logical Decoding & CDC (Debezium) | WAL Write Overhead |
| :--- | :---: | :---: | :---: | :--- |
| **`minimal`** | Yes | No | No | Lowest |
| **`replica`** *(default)* | Yes | Yes | No | Standard |
| **`logical`** | Yes | Yes | **Yes** | +5–10% on heavy writes |
