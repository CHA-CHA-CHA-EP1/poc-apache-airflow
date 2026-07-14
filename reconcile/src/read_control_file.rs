pub async fn run(channel: &str, date: &str) -> anyhow::Result<()> {
    tracing::info!("reading control file: channel={channel} date={date}");
    println!("{}", 20000);
    Ok(())
}
