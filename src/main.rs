use std::error::Error;
use running_rust::utils::{
  fetch_data::fetch_text_csv, 
  filter_column::{
    month_filter,
    null_filter
  }
};
use dotenv::dotenv;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  dotenv().ok();

  let running_df = fetch_text_csv().await?;
  let running_df = null_filter(&running_df)?
    .sort(["Date"], Default::default())?;

  let jan_2025 = month_filter(&running_df, "2568-01")?;

  


  println!("{:?}", jan_2025);

  Ok(())
}