# Solana DEX MEV Indexer — RUNBOOK & ARCHITECTURE GUIDE

> **Complete guide for local development and verification**
>
> Last updated: December 2025

---

## Table of Contents

- [A) QUICKSTART](#a-quickstart)
- [B) COMPONENT RUNBOOKS](#b-component-runbooks)
- [C) KAFKA TOPICS & MESSAGE SCHEMAS](#c-kafka-topics--message-schemas)
- [D) CLICKHOUSE SCHEMA GUIDE](#d-clickhouse-schema-guide)
- [E) ARCHITECTURE OVERVIEW](#e-architecture-overview)
- [F) TROUBLESHOOTING PLAYBOOK](#f-troubleshooting-playbook)

---

# A) QUICKSTART

## Prerequisites Checklist

| Requirement | Check Command | Expected Output |
|-------------|--------------|-----------------|
| **Rust toolchain** (stable) | `rustc --version` | `rustc 1.7x.x` or later |
| **Cargo** | `cargo --version` | `cargo 1.7x.x` or later |
| **Docker + Docker Compose** | `docker compose version` | v2.x.x or later |
| **WSL2** (Windows only) | `wsl --version` | WSL 2.x |
| **jq** (optional, for JSON parsing) | `jq --version` | 1.6+ |

### WSL Notes (Windows Users)
- Docker Desktop must be running and configured to use the WSL2 backend.
- Ports 9092, 19092, 8123, 9000, 2181 must be available.
- Use `localhost:19092` for Kafka access from within WSL.

---

## 1) Bring Up Infrastructure

```bash
# From the repo root:
cd /home/reda-37/solana-dex-mev-indexer

# Start Kafka (Zookeeper + Broker) + ClickHouse
./scripts/dev-up.sh
```

**What it does:** Runs `docker compose -f infra/docker-compose.yml up -d` to spin up:
- Zookeeper (port 2181)
- Kafka (ports 9092 internal, 19092 external)
- ClickHouse (ports 8123 HTTP, 9000 native)

**Success indicators:**
```
[*] Starting Kafka + ClickHouse + Prometheus + Grafana...
[*] Running containers:
NAMES               STATUS          PORTS
raydex-clickhouse   Up X seconds    0.0.0.0:8123->8123/tcp, 0.0.0.0:9000->9000/tcp
raydex-kafka        Up X seconds    0.0.0.0:9092->9092/tcp, 0.0.0.0:19092->19092/tcp
raydex-zookeeper    Up X seconds    0.0.0.0:2181->2181/tcp
```

---

## 2) Verify Infrastructure Health

### Check containers are running
```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```
**Success:** All 3 containers show `Up` status.

### Verify Kafka is reachable
```bash
docker exec raydex-kafka kafka-topics --bootstrap-server localhost:9092 --list
```
**Success:** Returns empty list or list of topics (no connection error).

### Verify ClickHouse is reachable
```bash
curl -s "http://localhost:8123/?query=SELECT%201"
```
**Success:** Returns `1`.

### Check ClickHouse database and tables exist
```bash
curl -s "http://localhost:8123/?query=SHOW%20TABLES%20FROM%20solana" | head -20
```
**Expected tables:**
```
dex_swaps
dex_swaps_v1
sol_balance_deltas
sol_balance_deltas_mv
sol_balance_deltas_queue
sol_raw_txs
sol_raw_txs_mv
sol_raw_txs_queue
sol_swaps_mv
sol_swaps_queue
sol_token_balance_deltas
sol_token_balance_deltas_mv
sol_token_balance_deltas_queue
```

---

## 3) Create Kafka Topics (if needed)

Topics are auto-created on first produce, but you can pre-create them:

```bash
# Create all required topics
docker exec raydex-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic sol_raw_txs --partitions 3 --replication-factor 1 --if-not-exists

docker exec raydex-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic sol_balance_deltas --partitions 3 --replication-factor 1 --if-not-exists

docker exec raydex-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic sol_token_balance_deltas --partitions 3 --replication-factor 1 --if-not-exists

docker exec raydex-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic sol_swaps --partitions 3 --replication-factor 1 --if-not-exists

docker exec raydex-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic sol_raw_txs_dlq --partitions 1 --replication-factor 1 --if-not-exists
```

**Verify topics:**
```bash
docker exec raydex-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

---

## 4) End-to-End Happy Path (Fastest Verification)

This uses the **backfill replay mode** to push pre-recorded transactions through the entire pipeline.

### Step 1: Build all binaries
```bash
cargo build --release -p backfill -p decoder -p indexer -p api
```
**Success:** No compilation errors, binaries in `target/release/`.

### Step 2: Replay historical data → Kafka (`sol_raw_txs`)
```bash
KAFKA_BROKER="localhost:19092" \
KAFKA_TOPIC="sol_raw_txs" \
cargo run --release -p backfill -- \
  --from-file data/raydium_amm_v4_mainnet_2k.jsonl
```

**What it does:** Reads the JSONL file containing ~2000 pre-fetched Raydium AMM v4 transactions and publishes `RawTxEvent` messages to `sol_raw_txs` topic.

**Expected output:**
```
[INFO] replay from data/raydium_amm_v4_mainnet_2k.jsonl
[INFO] 🔍 First RawTxEvent (replay) schema sample:
{
  "schema_version": 1,
  "chain": "solana-mainnet",
  "slot": 123456789,
  ...
}
[INFO] replay published 2000 events
```

### Step 3: Verify messages in Kafka
```bash
docker exec raydex-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic sol_raw_txs \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 5000
```
**Success:** Shows 3 JSON messages with `schema_version`, `signature`, `slot`, etc.

### Step 4: Run decoder → Kafka deltas/swaps
```bash
KAFKA_BROKER="localhost:19092" \
KAFKA_IN_TOPIC="sol_raw_txs" \
KAFKA_OUT_SOL_DELTAS_TOPIC="sol_balance_deltas" \
KAFKA_OUT_TOKEN_DELTAS_TOPIC="sol_token_balance_deltas" \
KAFKA_OUT_SWAPS_TOPIC="sol_swaps" \
KAFKA_GROUP="decoder_e2e_test" \
RPC_URL="https://api.mainnet-beta.solana.com" \
RAYDIUM_AMM_V4_PROGRAM_ID="675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" \
SWAPS_EXPLAIN="true" \
cargo run --release -p decoder
```

**What it does:** 
1. Consumes `RawTxEvent` from `sol_raw_txs`
2. Fetches full tx JSON from RPC
3. Extracts SOL deltas → publishes to `sol_balance_deltas`
4. Extracts token deltas → publishes to `sol_token_balance_deltas`
5. Detects Raydium v4 swaps → publishes to `sol_swaps`

**Expected log patterns:**
```
[INFO] decoder starting:
[INFO]   kafka_broker=localhost:19092
[INFO]   in_topic=sol_raw_txs
[INFO]   swap_detection=ENABLED
[INFO] 🔍 First RawTxEvent schema sample: ...
[INFO] 🔍 First SolBalanceDelta schema sample: ...
[INFO] 🔍 First SwapEvent schema sample: ...
[INFO] stats: processed=200 sol_deltas=... swaps_detected=...
```

Let it run for 30-60 seconds, then `Ctrl+C`.

### Step 5: Verify ClickHouse tables populated

```bash
# Raw transactions count
curl -s "http://localhost:8123/?query=SELECT%20count()%20FROM%20solana.sol_raw_txs"

# SOL balance deltas count
curl -s "http://localhost:8123/?query=SELECT%20count()%20FROM%20solana.sol_balance_deltas"

# Token balance deltas count  
curl -s "http://localhost:8123/?query=SELECT%20count()%20FROM%20solana.sol_token_balance_deltas"

# Swaps count
curl -s "http://localhost:8123/?query=SELECT%20count()%20FROM%20solana.dex_swaps_v1"
```

**Success:** Each query returns a number > 0.

### Step 6: Quick sanity query
```bash
curl -s "http://localhost:8123" -d "
SELECT 
  signature, 
  slot, 
  venue, 
  trader, 
  in_mint, 
  out_mint
FROM solana.dex_swaps_v1 
ORDER BY slot DESC 
LIMIT 5
FORMAT Pretty
"
```

---

## 5) Tear Down Infrastructure

```bash
./scripts/dev-down.sh
```
**What it does:** Runs `docker compose -f infra/docker-compose.yml down`.

To also remove volumes (full reset):
```bash
docker compose -f infra/docker-compose.yml down -v
```

---

# B) COMPONENT RUNBOOKS

## Streamer (`apps/streamer`)

### Purpose
Subscribes to a **Geyser/Yellowstone gRPC stream** and publishes `RawTxEvent` messages to Kafka in real-time.

### Inputs
| Input | Description |
|-------|-------------|
| **Geyser gRPC endpoint** | Yellowstone-compatible stream |
| **X-Token** (optional) | Auth token for private endpoints |

### Outputs
| Output | Kafka Topic | Format |
|--------|-------------|--------|
| Raw transactions | `sol_raw_txs` (configurable) | JSON (`RawTxEvent`) |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GEYSER_ENDPOINT` | **required** | Yellowstone gRPC endpoint URL |
| `GEYSER_X_TOKEN` | none | Auth token (if required) |
| `KAFKA_BROKER` | `localhost:19092` | Kafka bootstrap server |
| `KAFKA_TOPIC` | `sol_raw_txs` | Output topic for raw txs |
| `REQUIRED_ACCOUNTS` | `` (empty) | Comma-separated account pubkeys to filter |
| `INCLUDE_FAILED` | `false` | Include failed transactions |
| `COMMITMENT` | `processed` | `processed`, `confirmed`, or `finalized` |

### Example Commands

**Minimal run (local Geyser):**
```bash
GEYSER_ENDPOINT="http://127.0.0.1:10000" \
KAFKA_BROKER="localhost:19092" \
cargo run --release -p streamer
```

**Debug run (verbose logs):**
```bash
RUST_LOG=debug \
GEYSER_ENDPOINT="http://127.0.0.1:10000" \
KAFKA_BROKER="localhost:19092" \
cargo run --release -p streamer
```

**Filter to Raydium AMM only:**
```bash
GEYSER_ENDPOINT="http://your-geyser:10000" \
REQUIRED_ACCOUNTS="675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" \
cargo run --release -p streamer
```

### How to Validate It Works

1. **Check metrics log every 5s:**
   ```
   metrics tx_seen=100 kafka_ok=100 kafka_err=0 reconnects=1 connected=1
   ```

2. **Consume from Kafka:**
   ```bash
   docker exec raydex-kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic sol_raw_txs --max-messages 1
   ```

3. **Check ClickHouse raw table (if MV active):**
   ```bash
   curl -s "http://localhost:8123/?query=SELECT%20count()%20FROM%20solana.sol_raw_txs"
   ```

### Common Failures

| Symptom | Cause | Fix |
|---------|-------|-----|
| `GEYSER_ENDPOINT missing` | Env var not set | Set `GEYSER_ENDPOINT` |
| `transport error` | Geyser unreachable | Check endpoint, firewall |
| Reconnect loop | Stream disconnected | Normal behavior, auto-reconnects |

---

## Backfill (`apps/backfill`)

### Purpose
Historical data ingestion via RPC. Two modes:
1. **Backfill mode** (`--out`): Fetch tx history for an address, record to JSONL, publish to Kafka
2. **Replay mode** (`--from-file`): Replay recorded JSONL file to Kafka

### Inputs
| Mode | Input |
|------|-------|
| Backfill | Solana RPC + target address |
| Replay | JSONL file from previous backfill |

### Outputs
| Output | Kafka Topic | Format |
|--------|-------------|--------|
| Raw transactions | `sol_raw_txs` (configurable) | JSON (`RawTxEvent`) |
| DLQ events | `sol_raw_txs_dlq` | JSON (`DlqEvent`) |

### CLI Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--address` | `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8` | Address to fetch signatures for |
| `--limit` | `2000` | Max transactions to fetch |
| `--rpc-url` | `RPC_URL` env or mainnet-beta | Solana RPC endpoint |
| `--out` | none | JSONL output path (backfill mode) |
| `--from-file` | none | JSONL input path (replay mode) |
| `--concurrency` | `8` | Concurrent RPC calls |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RPC_URL` | `https://api.mainnet-beta.solana.com` | Solana RPC endpoint |
| `KAFKA_BROKER` | `127.0.0.1:19092` | Kafka bootstrap server |
| `KAFKA_TOPIC` | `sol_raw_txs` | Output topic |
| `KAFKA_DLQ_TOPIC` | `sol_raw_txs_dlq` | Dead letter queue topic |
| `CHAIN` | `solana-mainnet` | Chain identifier |

### Example Commands

**Backfill Raydium AMM v4 (2000 txs):**
```bash
KAFKA_BROKER="localhost:19092" \
cargo run --release -p backfill -- \
  --address 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8 \
  --limit 2000 \
  --out data/my_backfill.jsonl
```

**Replay existing file:**
```bash
KAFKA_BROKER="localhost:19092" \
cargo run --release -p backfill -- \
  --from-file data/raydium_amm_v4_mainnet_2k.jsonl
```

**Debug run:**
```bash
RUST_LOG=debug \
KAFKA_BROKER="localhost:19092" \
cargo run --release -p backfill -- --from-file data/raydium_amm_v4_mainnet_2k.jsonl
```

### How to Validate It Works

1. **Check progress logs:**
   ```
   progress fetched=100 ok=98 err=2 retries_429_total=5
   ```

2. **Check JSONL file created (backfill mode):**
   ```bash
   wc -l data/my_backfill.jsonl
   head -1 data/my_backfill.jsonl | jq .signature
   ```

3. **Verify Kafka messages:**
   ```bash
   docker exec raydex-kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic sol_raw_txs --from-beginning --max-messages 5
   ```

4. **Check DLQ for errors:**
   ```bash
   docker exec raydex-kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic sol_raw_txs_dlq --from-beginning --max-messages 5 --timeout-ms 3000
   ```

---

## Decoder (`apps/decoder`)

### Purpose
Consumes `RawTxEvent` from Kafka, fetches full transaction JSON via RPC, extracts:
- **SOL balance deltas** → publishes to `sol_balance_deltas`
- **Token balance deltas** → publishes to `sol_token_balance_deltas`
- **Swap events** (Raydium v4) → publishes to `sol_swaps`

### Inputs
| Input | Source |
|-------|--------|
| `RawTxEvent` messages | Kafka topic `sol_raw_txs` |
| Full transaction JSON | Solana RPC (getTransaction) |

### Outputs
| Output | Kafka Topic | Format |
|--------|-------------|--------|
| SOL balance deltas | `sol_balance_deltas` | JSON (`SolBalanceDelta`) |
| Token balance deltas | `sol_token_balance_deltas` | JSON (`TokenBalanceDelta`) |
| Swap events | `sol_swaps` | JSON (`SwapEvent`) |
| Failed messages | `KAFKA_DLQ_TOPIC` (optional) | JSON |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | `localhost:19092` | Kafka bootstrap server |
| `KAFKA_IN_TOPIC` | `sol_raw_txs` | Input topic |
| `KAFKA_OUT_SOL_DELTAS_TOPIC` | `sol_balance_deltas` | SOL deltas output |
| `KAFKA_OUT_TOKEN_DELTAS_TOPIC` | `sol_token_balance_deltas` | Token deltas output |
| `KAFKA_OUT_SWAPS_TOPIC` | `sol_swaps` | Swaps output |
| `KAFKA_DLQ_TOPIC` | none | DLQ topic (optional) |
| `KAFKA_GROUP` | `decoder_v1` | Consumer group ID |
| `RPC_PRIMARY_URL` / `RPC_URL` | `https://api.mainnet-beta.solana.com` | Primary RPC |
| `RPC_FALLBACK_URLS` | none | Comma-separated fallback RPCs |
| `RPC_CONCURRENCY` | `4` | Max concurrent RPC calls |
| `RPC_MIN_DELAY_MS` | `250` | Min delay between RPC calls |
| `RPC_MAX_TX_VERSION` | `1` | Max supported tx version |
| `RAYDIUM_AMM_V4_PROGRAM_ID` | `` (empty=disabled) | Enable swap detection |
| `SWAPS_EXPLAIN` | `false` | Include debug explain field |
| `SWAPS_EXPLAIN_LIMIT` | `20` | Max swaps with explain |
| `INCLUDE_FAILED` | `false` | Process failed transactions |

### Example Commands

**Minimal run (no swap detection):**
```bash
KAFKA_BROKER="localhost:19092" \
RPC_URL="https://api.mainnet-beta.solana.com" \
cargo run --release -p decoder
```

**Full run with swap detection:**
```bash
KAFKA_BROKER="localhost:19092" \
KAFKA_IN_TOPIC="sol_raw_txs" \
RPC_URL="https://api.mainnet-beta.solana.com" \
RAYDIUM_AMM_V4_PROGRAM_ID="675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" \
SWAPS_EXPLAIN="true" \
cargo run --release -p decoder
```

**Debug run:**
```bash
RUST_LOG=debug \
KAFKA_BROKER="localhost:19092" \
RAYDIUM_AMM_V4_PROGRAM_ID="675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" \
cargo run --release -p decoder
```

**With DLQ enabled:**
```bash
KAFKA_DLQ_TOPIC="decoder_dlq" \
KAFKA_BROKER="localhost:19092" \
RAYDIUM_AMM_V4_PROGRAM_ID="675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" \
cargo run --release -p decoder
```

### How to Validate It Works

1. **Check periodic stats log (every 200 messages):**
   ```
   stats: processed=200 sol_deltas=450 token_deltas=180 swaps_detected=42 swaps_emitted=42
   ```

2. **Consume SOL deltas from Kafka:**
   ```bash
   docker exec raydex-kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic sol_balance_deltas --max-messages 3
   ```

3. **Consume swaps from Kafka:**
   ```bash
   docker exec raydex-kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic sol_swaps --max-messages 3
   ```

4. **Check ClickHouse deltas table:**
   ```bash
   curl -s "http://localhost:8123/?query=SELECT%20count()%20FROM%20solana.sol_balance_deltas"
   ```

5. **Check ClickHouse swaps table:**
   ```bash
   curl -s "http://localhost:8123/?query=SELECT%20count()%20FROM%20solana.dex_swaps_v1"
   ```

---

## Indexer (`apps/indexer`)

### Purpose
Reads from ClickHouse and runs queries for verification/debugging. Currently a simple tool to query the `sol_raw_txs` table.

### Inputs
| Input | Source |
|-------|--------|
| ClickHouse tables | HTTP interface at `localhost:8123` |

### Example Command
```bash
cargo run --release -p indexer
```

**Expected output:**
```
Connecting to ClickHouse at http://localhost:8123 ...
Running query: SELECT ... FROM sol_raw_txs ORDER BY ts DESC LIMIT 20
Last 10 raw txs:
[2025-01-01 12:00:00] slot=123456 sig=abc... success=true fee=5000 ...
```

---

## API (`apps/api`)

### Purpose
HTTP API layer (currently a stub/placeholder).

### Status
**Not yet implemented** — prints "Hello, world!" only.

```bash
cargo run --release -p api
# Output: Hello, world!
```

---

# C) KAFKA TOPICS & MESSAGE SCHEMAS

## Topic Overview

| Topic | Key | Value Format | Producer | Consumer |
|-------|-----|--------------|----------|----------|
| `sol_raw_txs` | signature | JSON (`RawTxEvent`) | Streamer, Backfill | Decoder, ClickHouse MV |
| `sol_balance_deltas` | signature | JSON (`SolBalanceDelta`) | Decoder | ClickHouse MV |
| `sol_token_balance_deltas` | signature | JSON (`TokenBalanceDelta`) | Decoder | ClickHouse MV |
| `sol_swaps` | signature | JSON (`SwapEvent`) | Decoder | ClickHouse MV |
| `sol_raw_txs_dlq` | none/signature | JSON (`DlqEvent`) | Backfill, Decoder | Manual inspection |

---

## `sol_raw_txs` — Raw Transaction Events

**Producer:** `apps/streamer`, `apps/backfill`  
**Consumer:** `apps/decoder`, ClickHouse Kafka Engine (`solana.sol_raw_txs_queue`)

**Struct:** `RawTxEvent`  
**Location:** `apps/streamer/src/stream.rs`, `apps/backfill/src/types.rs`, `apps/decoder/src/types.rs`

**Schema:**
```json
{
  "schema_version": 1,
  "chain": "solana-mainnet",
  "slot": 319854752,
  "block_time": 1765817870,
  "signature": "y3yYtcSMAARYNSo5vhBFzAxA52ZXzETbh41Qp7RgL3sHUgC8XYMFkYZ5HbuVKidmdgCQwtuViyuopr8yiEP3dqE",
  "index_in_block": 0,
  "tx_version": null,
  "is_success": true,
  "fee_lamports": 6088,
  "compute_units_consumed": 15964,
  "main_program": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
  "program_ids": [
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  ]
}
```

---

## `sol_balance_deltas` — SOL Balance Changes

**Producer:** `apps/decoder`  
**Consumer:** ClickHouse Kafka Engine (`solana.sol_balance_deltas_queue`)

**Struct:** `SolBalanceDelta`  
**Location:** `apps/decoder/src/types.rs`

**Schema:**
```json
{
  "slot": 319854752,
  "block_time": 1765817870,
  "signature": "abc123...",
  "account": "7VtfL8fvgNfhz17qKRMjzQEXgbdpnHHHQRh54R9jP2RJ",
  "pre_balance": 1000000000,
  "post_balance": 999994912,
  "delta": -5088
}
```

---

## `sol_token_balance_deltas` — SPL Token Balance Changes

**Producer:** `apps/decoder`  
**Consumer:** ClickHouse Kafka Engine (`solana.sol_token_balance_deltas_queue`)

**Struct:** `TokenBalanceDelta`  
**Location:** `apps/decoder/src/types.rs`

**Schema:**
```json
{
  "slot": 319854752,
  "block_time": 1765817870,
  "signature": "abc123...",
  "account_index": 2,
  "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
  "decimals": 6,
  "pre_amount": 1000000,
  "post_amount": 950000,
  "delta": -50000
}
```

---

## `sol_swaps` — DEX Swap Events

**Producer:** `apps/decoder` (via Raydium v4 detector)  
**Consumer:** ClickHouse Kafka Engine (`solana.sol_swaps_queue`)

**Struct:** `SwapEvent`  
**Location:** `crates/schema/src/swap.rs`

**Schema:**
```json
{
  "schema_version": 1,
  "chain": "solana-mainnet",
  "slot": 319854752,
  "block_time": 1765817870,
  "signature": "abc123...",
  "index_in_tx": 0,
  "venue": "raydium",
  "market_or_pool": null,
  "trader": "7VtfL8fvgNfhz17qKRMjzQEXgbdpnHHHQRh54R9jP2RJ",
  "in_mint": "So11111111111111111111111111111111111111112",
  "in_amount": "1000000000",
  "out_mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
  "out_amount": "50000000",
  "fee_mint": null,
  "fee_amount": null,
  "route_id": null,
  "confidence": 80,
  "explain": "raydium_v4 gate=hit trader=7Vtf... in=So11... (-1000000000) out=EPjF... (+50000000)"
}
```

---

## `sol_raw_txs_dlq` — Dead Letter Queue

**Producer:** `apps/backfill`, `apps/decoder`  
**Consumer:** Manual debugging

**Struct:** `DlqEvent`  
**Location:** `apps/backfill/src/types.rs`

**Schema:**
```json
{
  "source": "backfill",
  "step": "getTransaction",
  "signature": "abc123...",
  "error": "rpc http error status=429"
}
```

**Decoder DLQ format (different):**
```json
{
  "reason": "rpc_getTransaction_failed",
  "attempts": 3,
  "error": "rate limited after 3 attempts",
  "signature": "abc123...",
  "slot": 319854752,
  "block_time": 1765817870,
  "chain": "solana-mainnet"
}
```

---

# D) CLICKHOUSE SCHEMA GUIDE

## Database: `solana`

All tables are created in the `solana` database via init scripts in `infra/clickhouse/` (mounted to `/docker-entrypoint-initdb.d`).

---

## Table Architecture Pattern

Each data stream follows this pattern:

```
Kafka Topic → Kafka Engine Table (_queue) → Materialized View (_mv) → MergeTree Table (storage)
```

- **Kafka Engine Table**: Consumes from Kafka, ephemeral (no data stored)
- **Materialized View**: Transforms and routes data to storage
- **MergeTree Table**: Persistent storage with proper ordering

---

## Tables Reference

### `solana.sol_raw_txs_queue` (Kafka Engine)

**Source:** `clickhouse/001_init_raw_txs.sql`  
**Purpose:** Consumes `RawTxEvent` from `sol_raw_txs` topic

```sql
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'sol_raw_txs',
  kafka_group_name = 'sol_dex_mev_indexer_v2',
  kafka_format = 'JSONEachRow',
  kafka_num_consumers = 1
```

**Columns:** `schema_version`, `chain`, `slot`, `block_time`, `signature`, `index_in_block`, `tx_version`, `is_success`, `fee_lamports`, `compute_units_consumed`, `main_program`, `program_ids`

---

### `solana.sol_raw_txs` (MergeTree)

**Purpose:** Persistent storage for raw transaction events

```sql
ENGINE = MergeTree
ORDER BY (slot, signature)
```

**ORDER BY rationale:**
- Primary queries filter by slot range
- Secondary lookups by signature
- Good compression due to slot locality

**Sanity queries:**
```sql
-- Row count
SELECT count() FROM solana.sol_raw_txs;

-- Latest 5 transactions
SELECT signature, slot, main_program, is_success 
FROM solana.sol_raw_txs 
ORDER BY slot DESC LIMIT 5;

-- Transactions per hour (lag check)
SELECT 
  toStartOfHour(ts) as hour,
  count() as txs
FROM solana.sol_raw_txs
WHERE ts > now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour DESC;
```

---

### `solana.sol_balance_deltas_queue` (Kafka Engine)

**Source:** `clickhouse/003_decoder_tables.sql`  
**Topic:** `sol_balance_deltas`  
**Consumer group:** `sol_dex_mev_indexer_decoder_v1`

---

### `solana.sol_balance_deltas` (MergeTree)

```sql
ENGINE = MergeTree
ORDER BY (account, slot, signature)
```

**ORDER BY rationale:**
- Primary queries filter by account pubkey
- Secondary by slot for time-range queries

**Sanity queries:**
```sql
-- Row count
SELECT count() FROM solana.sol_balance_deltas;

-- Top accounts by delta volume
SELECT account, sum(abs(delta)) as volume
FROM solana.sol_balance_deltas
GROUP BY account
ORDER BY volume DESC
LIMIT 10;
```

---

### `solana.sol_token_balance_deltas_queue` (Kafka Engine)

**Source:** `clickhouse/003_decoder_tables.sql`  
**Topic:** `sol_token_balance_deltas`  
**Consumer group:** `sol_dex_mev_indexer_decoder_v1`

---

### `solana.sol_token_balance_deltas` (MergeTree)

```sql
ENGINE = MergeTree
ORDER BY (mint, slot, signature, account_index)
```

**ORDER BY rationale:**
- Primary queries filter by mint (token type)
- Secondary by slot for time-range queries

**Sanity queries:**
```sql
-- Row count
SELECT count() FROM solana.sol_token_balance_deltas;

-- Unique mints
SELECT uniq(mint) FROM solana.sol_token_balance_deltas;

-- Top mints by transfer count
SELECT mint, count() as transfers
FROM solana.sol_token_balance_deltas
GROUP BY mint
ORDER BY transfers DESC
LIMIT 10;
```

---

### `solana.sol_swaps_queue` (Kafka Engine)

**Source:** `clickhouse/004_swaps.sql`  
**Topic:** `sol_swaps`  
**Consumer group:** `sol_swaps_v1`

---

### `solana.dex_swaps_v1` (ReplacingMergeTree)

```sql
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMMDD(fromUnixTimestamp(coalesce(block_time, 0)))
ORDER BY (signature, index_in_tx)
```

**Engine choice:** `ReplacingMergeTree` with `version` column
- Allows deduplication of duplicate swap events
- Latest version (by `toUnixTimestamp(now())`) wins

**PARTITION BY rationale:**
- Partition by day based on block_time
- Enables efficient date-range queries and TTL

**ORDER BY rationale:**
- Primary key is `(signature, index_in_tx)` — unique per swap
- Multiple swaps per tx possible (index_in_tx differentiates)

**Dedup behavior:**
- Rows with same `(signature, index_in_tx)` are deduplicated
- Higher `version` value wins
- Dedup happens during merges (not immediately)
- Force dedup: `OPTIMIZE TABLE solana.dex_swaps_v1 FINAL`

**Sanity queries:**
```sql
-- Row count
SELECT count() FROM solana.dex_swaps_v1;

-- Unique swaps (accounting for potential dups)
SELECT uniq(signature, index_in_tx) FROM solana.dex_swaps_v1;

-- Swaps by venue
SELECT venue, count() as swaps
FROM solana.dex_swaps_v1
GROUP BY venue;

-- Latest 5 swaps
SELECT signature, trader, venue, in_mint, out_mint, confidence
FROM solana.dex_swaps_v1
ORDER BY slot DESC
LIMIT 5;

-- Swaps per day
SELECT 
  toDate(fromUnixTimestamp(coalesce(block_time, 0))) as day,
  count() as swaps
FROM solana.dex_swaps_v1
GROUP BY day
ORDER BY day DESC
LIMIT 7;
```

---

### `solana.dex_swaps` (Legacy MergeTree)

**Source:** `clickhouse/002_dex_swaps.sql`  
**Purpose:** Older schema, may not be actively used

```sql
ENGINE = MergeTree
ORDER BY (ts, slot, signature, index_in_block)
```

---

## Complete Sanity Check Script

Run all health checks at once:

```bash
curl -s "http://localhost:8123" -d "
SELECT 'sol_raw_txs' as table, count() as rows FROM solana.sol_raw_txs
UNION ALL
SELECT 'sol_balance_deltas', count() FROM solana.sol_balance_deltas
UNION ALL
SELECT 'sol_token_balance_deltas', count() FROM solana.sol_token_balance_deltas
UNION ALL
SELECT 'dex_swaps_v1', count() FROM solana.dex_swaps_v1
FORMAT Pretty
"
```

---

# E) ARCHITECTURE OVERVIEW

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              INGESTION LAYER                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌──────────────────┐         ┌───────────────────┐                             │
│  │  Geyser/Yellow-  │         │   Solana RPC      │                             │
│  │  stone gRPC      │         │   (mainnet-beta)  │                             │
│  └────────┬─────────┘         └─────────┬─────────┘                             │
│           │                             │                                        │
│           ▼                             ▼                                        │
│  ┌──────────────────┐         ┌───────────────────┐                             │
│  │    STREAMER      │         │    BACKFILL       │                             │
│  │  (real-time)     │         │ (historical/batch)│                             │
│  └────────┬─────────┘         └─────────┬─────────┘                             │
│           │                             │                                        │
│           └──────────┬──────────────────┘                                        │
│                      │                                                           │
│                      ▼                                                           │
│           ┌─────────────────────┐                                               │
│           │  Kafka Topic:       │                                               │
│           │  sol_raw_txs        │◄─── RawTxEvent (JSON)                         │
│           └─────────┬───────────┘                                               │
│                     │                                                           │
└─────────────────────┼───────────────────────────────────────────────────────────┘
                      │
┌─────────────────────┼───────────────────────────────────────────────────────────┐
│                     │            PROCESSING LAYER                                │
├─────────────────────┼───────────────────────────────────────────────────────────┤
│                     ▼                                                           │
│           ┌───────────────────┐                                                 │
│           │      DECODER      │◄─── Fetches full tx via RPC                     │
│           │                   │                                                 │
│           │  • SOL deltas     │                                                 │
│           │  • Token deltas   │                                                 │
│           │  • Swap detection │                                                 │
│           └────────┬──────────┘                                                 │
│                    │                                                            │
│        ┌───────────┼───────────┬───────────────────┐                            │
│        ▼           ▼           ▼                   ▼                            │
│  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌─────────────┐                      │
│  │ sol_      │ │ sol_token_│ │ sol_swaps │ │ DLQ topic   │                      │
│  │ balance_  │ │ balance_  │ │           │ │(poison pills│                      │
│  │ deltas    │ │ deltas    │ │           │ │ & failures) │                      │
│  └─────┬─────┘ └─────┬─────┘ └─────┬─────┘ └─────────────┘                      │
│        │             │             │                                            │
└────────┼─────────────┼─────────────┼────────────────────────────────────────────┘
         │             │             │
┌────────┼─────────────┼─────────────┼────────────────────────────────────────────┐
│        │             │             │         STORAGE LAYER (ClickHouse)          │
├────────┼─────────────┼─────────────┼────────────────────────────────────────────┤
│        ▼             ▼             ▼                                            │
│  ┌───────────┐ ┌───────────┐ ┌───────────┐                                      │
│  │ Kafka     │ │ Kafka     │ │ Kafka     │   ◄── Kafka Engine Tables            │
│  │ Engine    │ │ Engine    │ │ Engine    │       (ephemeral consumers)           │
│  │ _queue    │ │ _queue    │ │ _queue    │                                       │
│  └─────┬─────┘ └─────┬─────┘ └─────┬─────┘                                      │
│        │             │             │                                            │
│        ▼             ▼             ▼                                            │
│  ┌───────────┐ ┌───────────┐ ┌───────────┐                                      │
│  │ Material- │ │ Material- │ │ Material- │   ◄── Materialized Views              │
│  │ ized View │ │ ized View │ │ ized View │       (transform & route)             │
│  │ _mv       │ │ _mv       │ │ _mv       │                                       │
│  └─────┬─────┘ └─────┬─────┘ └─────┬─────┘                                      │
│        │             │             │                                            │
│        ▼             ▼             ▼                                            │
│  ┌───────────┐ ┌───────────┐ ┌───────────┐                                      │
│  │ sol_      │ │ sol_token_│ │ dex_      │   ◄── MergeTree Tables                │
│  │ balance_  │ │ balance_  │ │ swaps_v1  │       (persistent storage)            │
│  │ deltas    │ │ deltas    │ │ (Replacing│                                       │
│  │ (Merge)   │ │ (Merge)   │ │ MergeTree)│                                       │
│  └───────────┘ └───────────┘ └───────────┘                                      │
│                                                                                 │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │ sol_raw_txs (MergeTree) ◄── Direct Kafka consumption of raw events       │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              QUERY LAYER                                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ┌───────────────┐    ┌───────────────┐                                         │
│  │   INDEXER     │    │     API       │                                         │
│  │ (query tool)  │    │   (stub)      │                                         │
│  └───────────────┘    └───────────────┘                                         │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Semantics & Guarantees

### At-Least-Once Delivery

- **Streamer → Kafka:** Idempotent producer (`enable.idempotence=true`)
- **Decoder consumer:** Manual commit only after successful processing
- **Outcome:** Messages may be processed multiple times on failure, but never lost

### Retry Strategy (Decoder)

```
Attempt 1: Process message
           ↓ (RPC failure)
Attempt 2: Wait 200ms, retry
           ↓ (RPC failure)
Attempt 3: Wait 400ms, retry
           ↓ (RPC failure, MAX_ATTEMPTS reached)
           → Send to DLQ (if configured)
           → Commit offset (unblock consumer)
```

**Constants (from decoder):**
- `MAX_ATTEMPTS = 3`
- `BASE_BACKOFF_MS = 200`
- `MAX_FAILURE_MAP_SIZE = 10000` (prevents unbounded memory)

### Poison Pill Handling

1. **JSON parse failure:** Log error, commit immediately (don't retry garbage)
2. **RPC transient failure:** Retry with backoff up to MAX_ATTEMPTS
3. **RPC permanent failure:** Send to DLQ, commit, continue

### DLQ Usage

Enable with `KAFKA_DLQ_TOPIC` env var. DLQ messages contain:
- Original signature
- Error reason
- Attempt count
- Slot and chain context

**Inspect DLQ:**
```bash
docker exec raydex-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic decoder_dlq --from-beginning
```

### Deduplication

| Layer | Mechanism |
|-------|-----------|
| Kafka Producer | Idempotent producer (exactly-once per partition) |
| ClickHouse dex_swaps_v1 | ReplacingMergeTree (version column) |
| Other tables | No built-in dedup (idempotent by design) |

**Force dedup in ClickHouse:**
```sql
OPTIMIZE TABLE solana.dex_swaps_v1 FINAL;
```

---

# F) TROUBLESHOOTING PLAYBOOK

## Issue: Kafka Not Reachable

### Symptoms
```
Kafka delivery error: BrokerTransportFailure
connection refused
```

### Diagnosis
```bash
# Check if Kafka container is running
docker ps | grep kafka

# Check Kafka logs
docker logs raydex-kafka --tail 50

# Test connectivity from host
nc -zv localhost 19092

# Test connectivity from container
docker exec raydex-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Common Causes

| Cause | Fix |
|-------|-----|
| Wrong port (9092 vs 19092) | Use `localhost:19092` from host/WSL, `kafka:9092` from containers |
| Container not started | Run `./scripts/dev-up.sh` |
| Zookeeper not ready | Wait 30s after starting, check `docker logs raydex-zookeeper` |

---

## Issue: Topics Missing

### Symptoms
```
Topic not present in metadata
```

### Diagnosis
```bash
docker exec raydex-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Fix
Topics auto-create on first produce, or create manually:
```bash
docker exec raydex-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic sol_raw_txs --partitions 3 --replication-factor 1
```

---

## Issue: Decoder Stuck on Poison Pill

### Symptoms
- Decoder logs show repeated errors for same signature
- No progress (processed count not increasing)
- High `pending_retries` count

### Diagnosis
```bash
# Check decoder logs for repeated signature
RUST_LOG=debug cargo run --release -p decoder 2>&1 | grep -E "(sig=|poison)"
```

### Fix
1. Enable DLQ to unblock:
   ```bash
   KAFKA_DLQ_TOPIC="decoder_dlq" cargo run --release -p decoder
   ```

2. After 3 attempts, poison pills go to DLQ automatically

3. Inspect DLQ:
   ```bash
   docker exec raydex-kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic decoder_dlq --from-beginning
   ```

---

## Issue: RPC 429 Rate Limiting

### Symptoms
```
RPC 429 rate limit, backing off
rate-limited (429)
```

### Diagnosis
Check how many 429s in logs:
```bash
cargo run --release -p decoder 2>&1 | grep -c "429"
```

### Fixes

| Fix | How |
|-----|-----|
| Lower concurrency | `RPC_CONCURRENCY=2` |
| Increase delay | `RPC_MIN_DELAY_MS=500` |
| Use private RPC | `RPC_URL="https://your-private-rpc.com"` |
| Add fallbacks | `RPC_FALLBACK_URLS="https://rpc2.com,https://rpc3.com"` |

---

## Issue: ClickHouse MV Not Inserting

### Symptoms
- Kafka topics have messages
- ClickHouse tables remain empty

### Diagnosis

```bash
# Check if Kafka Engine tables exist
curl -s "http://localhost:8123/?query=SHOW%20TABLES%20FROM%20solana%20LIKE%20'%25queue%25'"

# Check Kafka Engine table consumer lag
curl -s "http://localhost:8123" -d "
SELECT 
  database, 
  table, 
  last_exception,
  last_exception_time
FROM system.kafka_consumers
FORMAT Pretty
"

# Check system errors
curl -s "http://localhost:8123/?query=SELECT%20*%20FROM%20system.errors%20ORDER%20BY%20last_error_time%20DESC%20LIMIT%2010%20FORMAT%20Pretty"
```

### Common Causes

| Cause | Fix |
|-------|-----|
| Wrong Kafka broker | ClickHouse uses `kafka:9092` (internal), not `localhost:19092` |
| Schema mismatch | JSON field names must match exactly |
| MV not created | Re-run init SQL: `cat clickhouse/*.sql \| docker exec -i raydex-clickhouse clickhouse-client --multiquery` |

### Force recreation of tables:
```bash
# Connect to ClickHouse
docker exec -it raydex-clickhouse clickhouse-client

# Drop and recreate (destructive!)
DROP DATABASE solana;
# Then re-run init scripts
```

---

## Issue: Empty Swaps (Detection Not Working)

### Symptoms
- `swaps_detected=0` in decoder logs
- `sol_swaps` topic empty
- `dex_swaps_v1` table empty

### Diagnosis

1. **Check if swap detection is enabled:**
   ```bash
   cargo run --release -p decoder 2>&1 | grep "swap_detection"
   # Should show: swap_detection=ENABLED
   ```

2. **Check program ID:**
   ```bash
   # Must be set correctly
   echo $RAYDIUM_AMM_V4_PROGRAM_ID
   # Expected: 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8
   ```

3. **Check if transactions contain Raydium:**
   ```bash
   docker exec raydex-kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic sol_raw_txs --from-beginning --max-messages 10 \
     | grep -c "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
   ```

### Common Causes

| Cause | Fix |
|-------|-----|
| `RAYDIUM_AMM_V4_PROGRAM_ID` not set | Set to `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8` |
| Input transactions aren't Raydium swaps | Use `REQUIRED_ACCOUNTS` in streamer to filter |
| Multi-hop swaps (>2 tokens) | Currently not supported, logged silently |

---

## Issue: Inspecting One Signature End-to-End

### Step-by-step trace for signature `abc123...`

```bash
SIG="y3yYtcSMAARYNSo5vhBFzAxA52ZXzETbh41Qp7RgL3sHUgC8XYMFkYZ5HbuVKidmdgCQwtuViyuopr8yiEP3dqE"

# 1. Check if in raw_txs topic
docker exec raydex-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic sol_raw_txs --from-beginning \
  --timeout-ms 30000 | grep "$SIG" | head -1

# 2. Check if in sol_raw_txs table
curl -s "http://localhost:8123" -d "
SELECT * FROM solana.sol_raw_txs WHERE signature = '$SIG' FORMAT Pretty
"

# 3. Check if SOL deltas exist
curl -s "http://localhost:8123" -d "
SELECT * FROM solana.sol_balance_deltas WHERE signature = '$SIG' FORMAT Pretty
"

# 4. Check if token deltas exist
curl -s "http://localhost:8123" -d "
SELECT * FROM solana.sol_token_balance_deltas WHERE signature = '$SIG' FORMAT Pretty
"

# 5. Check if swap was detected
curl -s "http://localhost:8123" -d "
SELECT * FROM solana.dex_swaps_v1 WHERE signature = '$SIG' FORMAT Pretty
"

# 6. Check DLQ for errors
docker exec raydex-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic sol_raw_txs_dlq --from-beginning \
  --timeout-ms 10000 | grep "$SIG"
```

---

## Quick Health Check Script

Save as `scripts/health-check.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

echo "=== Docker Containers ==="
docker ps --format "table {{.Names}}\t{{.Status}}"

echo -e "\n=== Kafka Topics ==="
docker exec raydex-kafka kafka-topics --bootstrap-server localhost:9092 --list

echo -e "\n=== Kafka Consumer Groups ==="
docker exec raydex-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list

echo -e "\n=== ClickHouse Tables ==="
curl -s "http://localhost:8123/?query=SHOW%20TABLES%20FROM%20solana"

echo -e "\n=== ClickHouse Row Counts ==="
curl -s "http://localhost:8123" -d "
SELECT 'sol_raw_txs' as tbl, count() as rows FROM solana.sol_raw_txs
UNION ALL SELECT 'sol_balance_deltas', count() FROM solana.sol_balance_deltas
UNION ALL SELECT 'sol_token_balance_deltas', count() FROM solana.sol_token_balance_deltas  
UNION ALL SELECT 'dex_swaps_v1', count() FROM solana.dex_swaps_v1
FORMAT Pretty
"

echo -e "\n=== Latest Swap ==="
curl -s "http://localhost:8123" -d "
SELECT signature, slot, trader, venue, in_mint, out_mint 
FROM solana.dex_swaps_v1 
ORDER BY slot DESC LIMIT 1
FORMAT Pretty
"
```

Make executable and run:
```bash
chmod +x scripts/health-check.sh
./scripts/health-check.sh
```

---

## Summary Command Reference

| Task | Command |
|------|---------|
| Start infra | `./scripts/dev-up.sh` |
| Stop infra | `./scripts/dev-down.sh` |
| List Kafka topics | `docker exec raydex-kafka kafka-topics --bootstrap-server localhost:9092 --list` |
| Consume from topic | `docker exec raydex-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic TOPIC --from-beginning --max-messages N` |
| Check consumer lag | `docker exec raydex-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group GROUP --describe` |
| ClickHouse query | `curl -s "http://localhost:8123" -d "SELECT ..."` |
| ClickHouse CLI | `docker exec -it raydex-clickhouse clickhouse-client` |
| Build all | `cargo build --release` |
| Run decoder (full) | `KAFKA_BROKER=localhost:19092 RAYDIUM_AMM_V4_PROGRAM_ID=675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8 cargo run --release -p decoder` |
| Run backfill replay | `KAFKA_BROKER=localhost:19092 cargo run --release -p backfill -- --from-file data/raydium_amm_v4_mainnet_2k.jsonl` |

---

*End of Runbook*
