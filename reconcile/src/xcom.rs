use std::fs;

use serde::Serialize;

pub fn push<T: Serialize>(value: &T) -> anyhow::Result<()> {
    fs::create_dir_all("/airflow/xcom")?;
    fs::write("/airflow/xcom/return.json", serde_json::to_string(value)?)?;
    Ok(())
}
