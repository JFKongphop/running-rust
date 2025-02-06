use std::{env, error::Error};
use reqwest;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct FileInfo {
  download_url: String,
}

pub async fn fetch_text_csv() -> Result<Vec<String>, Box<dyn Error>> {
  let folder = env::var("FOLDER")
    .map_err(|e| format!("Missing FOLDER env variable: {}", e))?;
  
  let github_repo = format!("https://api.github.com/repos/{}", folder);
  let client = reqwest::Client::new();
  
  let resp = client
    .get(&github_repo)
    .header("User-Agent", "github")
    .send()
    .await?
    .error_for_status()?
    .json::<Vec<FileInfo>>()
    .await?;

  let csv_links: Vec<String> = resp
    .into_iter()
    .map(|file| file.download_url)
    .collect();

  if csv_links.is_empty() {
    Err("No CSV files found in the repository".into())
  } else {
    Ok(csv_links)
  }
}
