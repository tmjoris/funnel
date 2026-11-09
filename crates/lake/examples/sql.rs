//! Runs one SQL statement against a lake and prints the rows as JSON.
//!
//! cargo run --release -p funnel-lake --example sql -- DIR TENANT "SELECT ..."

use std::path::Path;

use funnel_lake::Lake;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let lake = Lake::open(Path::new(&args[1])).await?;
    let out = lake.query(&args[3], &args[2]).await?;
    println!("{}", String::from_utf8(out.json)?);
    println!("rollup seconds: {:?}", out.rollup_seconds);
    Ok(())
}
