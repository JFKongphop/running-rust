use std::error::Error;
use running_rust::utils::{
  apply_column::activity_to_type, 
  fetch_data::fetch_text_csv, 
  filter_column::{
    activity_filter, distance_filter, null_filter
  }
};
use dotenv::dotenv;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  dotenv().ok();

  let running_df = fetch_text_csv().await?;
  let mut running_df = null_filter(&running_df)?.sort(["Date"], Default::default())?;
  let running_df = running_df.apply("Activity", activity_to_type)?;

  let _indoor = activity_filter(&running_df, "indoor")?;
  let _outdoor = activity_filter(&running_df, "outdoor")?;

  let _400m = distance_filter(&running_df, 0.39, 0.43)?;
  let _1000m = distance_filter(&running_df, 1.0, 1.1)?;
  let _1200m = distance_filter(&running_df, 1.2, 1.35)?;
  let _2000m = distance_filter(&running_df, 2.0, 2.25)?;


  println!("{:?}", _400m);
  println!("{:?}", _1000m);
  println!("{:?}", _1200m);
  println!("{:?}", _2000m);

  Ok(())
}