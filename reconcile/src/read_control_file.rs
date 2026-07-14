pub async fn run(channel: &str, date: &str) -> anyhow::Result<()> {
    tracing::info!("reading control file: channel={channel} date={date}");
    // println!("{}", 20000);
    let result = serde_json::json!({
        "channel": channel,
        "date": date,
        "total_records": 20000
    });

    crate::xcom::push(&result)?;
    Ok(())
}
