use futures::future;
use polars::prelude::*;
use reqwest;
use serde::Deserialize;
use std::{env, error::Error, io::Cursor};
use tokio::sync::Mutex;
use redis::Commands;

#[derive(Deserialize, Debug)]
struct FileInfo {
  download_url: String,
}

async fn fetch_folder() -> Result<Vec<String>, Box<dyn Error>> {
  let folder = env::var("FOLDER").map_err(|e| format!("Missing FOLDER env variable: {}", e))?;

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

  let csv_links: Vec<String> = resp.into_iter().map(|file| file.download_url).collect();

  if csv_links.is_empty() {
    Err("No CSV files found in the repository".into())
  } else {
    Ok(csv_links)
  }
}

#[allow(dependency_on_unit_never_type_fallback)]
pub async fn fetch_text_csv() -> Result<DataFrame, Box<dyn Error>> {
  let redis_url = env::var("REDIS_KEY").map_err(|e| format!("Missing REDIS_KEY env variable: {}", e))?;
  let mut con = redis::Client::open(redis_url)?;
  let key = "DATAFRAME";
  let redis_dataframe: Option<String> = con.get(key)?;
  
  let running_df: DataFrame;

  if let Some(dataframe) = redis_dataframe {
    let data_bytes = dataframe.as_bytes();
    let cursor = Cursor::new(data_bytes);
  
    running_df = CsvReader::new(cursor)
      .finish()
      .expect("CSV reading should not fail");

    println!("redis");
  } else {
    let csv_data: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let header = String::from("Date,Energy (kcal),Activity,Distance(km),Duration(min),Pace(min),Heart rate: Average(min),Heart rate: Maximum(min)\n");
  
    let csv_links = fetch_folder().await?;
  
    let tasks: Vec<_> = csv_links
      .into_iter()
      .map(|link| {
        let csv_data = Arc::clone(&csv_data);
        let header = header.clone();
  
        tokio::spawn(async move {
          match reqwest::get(&link).await {
            Ok(response) => match response.text().await {
              Ok(text) => {
                let mut csv_data = csv_data.lock().await;
                let data_bytes = text.as_bytes();
                let cursor = Cursor::new(data_bytes);
  
                match CsvReader::new(cursor).finish() {
                  Ok(_df) => {
                    csv_data.push(text.replace(header.as_str(), ""));
                  }
                  Err(e) => eprintln!("Error reading CSV from {}: {}", link, e),
                }
              }
              Err(e) => eprintln!("Error fetching CSV from {}: {}", link, e),
            },
            Err(e) => eprintln!("Error downloading from {}: {}", link, e),
          }
        })
      })
      .collect();
  
    future::join_all(tasks).await;
  
    let joined_csv_data = {
      let csv_data = csv_data.lock().await;
      csv_data.join("\n")
    };
  
    let new_data = format!("{}{}", header, joined_csv_data);
    con.set_ex(key, new_data.clone(), 30)?;
  
    let data_bytes = new_data.as_bytes();
    let cursor = Cursor::new(data_bytes);
  
    running_df = CsvReader::new(cursor)
      .finish()
      .expect("CSV reading should not fail");
  
    println!("api");
  }

  Ok(running_df)
}
