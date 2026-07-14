pub async fn run(channel: &str, date: &str) -> anyhow::Result<()> {
    tracing::info!("count document: channel={channel} date={date}");
    let result = serde_json::json!({
        "channel": channel,
        "date": date,
        "document_count": 20000
    });

    crate::xcom::push(&result)?;
    Ok(())
}
