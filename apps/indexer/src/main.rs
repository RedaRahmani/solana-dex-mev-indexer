use anyhow::Result;
use clickhouse::{Client, Row};
use serde::Deserialize;



#[derive(Debug, Deserialize, Row)]
struct RawTxRow {
    ts: String,              // we'll convert DateTime -> String in SQL
    slot: u64,
    signature: String,
    tx_version: Option<u8>,
    is_success: bool,
    fee_lamports: u64,
    main_program: Option<String>,
    program_ids: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    println!("Connecting to ClickHouse at http://localhost:8123 ...");

    let client = Client::default()
        .with_url("http://localhost:8123")
        .with_database("solana"); // we created this DB earlier

    let query = r#"
        SELECT
            toString(ts) AS ts,   -- convert DateTime -> String, alias to `ts`
            slot,
            signature,
            tx_version,
            is_success,
            fee_lamports,
            main_program,
            program_ids
        FROM sol_raw_txs
        ORDER BY ts DESC
        LIMIT 20
    "#;

    println!("Running query:\n{query}");

    let mut cursor = client
        .query(query)
        .fetch::<RawTxRow>()?;

    println!("\nLast 10 raw txs:\n");

    while let Some(row) = cursor.next().await? {
        println!(
            "[{}] slot={} sig={} tx_v={:?} success={} fee={} main_prog={:?} programs={:?}",
            row.ts,
            row.slot,
            row.signature,
            row.tx_version,
            row.is_success,
            row.fee_lamports,
            row.main_program,
            row.program_ids,
        );
    }

    println!("\nDone.");
    Ok(())
}
