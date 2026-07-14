pub async fn run(
    channel: &str,
    date: &str,
    control_count: i64,
    document_count: i64,
) -> anyhow::Result<()> {
    tracing::info!(
        "compare: channel={channel} date={date} control_count={control_count} document_count={document_count}"
    );

    if control_count == document_count {
        println!(
            "{}",
            serde_json::json!({"status": "success", "mismatch": 0})
        );
        return Ok(());
    }

    println!("{}", serde_json::json!({"status": "fail", "mismatch": 10}));

    Ok(())
}
