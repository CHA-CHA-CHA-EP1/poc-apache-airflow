pub async fn run(channel: &str, date: &str, missing_record: i64) -> anyhow::Result<()> {
    tracing::info!("insert missing: channel={channel} date={date} missing_record={missing_record}");
    if missing_record > 0 {
        tracing::info!("insert missing: processing -> missing_record={missing_record}");
    }

    Ok(())
}
