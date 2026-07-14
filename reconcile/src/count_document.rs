pub async fn run(channel: &str, date: &str) -> anyhow::Result<()> {
    tracing::info!("count document: channel={channel} date={date}");
    println!("{}", 20000);
    Ok(())
}
