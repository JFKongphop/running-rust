use std::error::Error;
use running_rust::utils::{
  agg_data::{
    group_sum, 
    sort_ascending, 
    sum_distance
  }, 
  apply_column::{
    activity_to_type, 
    only_year_month_column
  }, 
  fetch_data::fetch_text_csv, 
  filter_column::{
    activity_filter, 
    distance_filter, 
    null_filter, year_filter
  }, times::fill_missing_months
};
use dotenv::dotenv;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  dotenv().ok();

  let df = fetch_text_csv().await?;
  let not_null_df = null_filter(&df)?;
  let mut sort_date_df = sort_ascending(&not_null_df, "Date")?;
  let running_df = sort_date_df.apply("Activity", activity_to_type)?;

  let _indoor = activity_filter(&running_df, "indoor")?;
  let _outdoor = activity_filter(&running_df, "outdoor")?;

  let _400m = distance_filter(&running_df, 0.39, 0.43)?;
  let _1000m = distance_filter(&running_df, 1.0, 1.1)?;
  let _1200m = distance_filter(&running_df, 1.2, 1.35)?;
  let _2000m = distance_filter(&running_df, 2.0, 2.25)?;
  let _5000m = distance_filter(&running_df, 5.0, 5.1)?;
  let _7000m = distance_filter(&running_df, 7.0, 7.1)?;
  let _10000m = distance_filter(&running_df, 10.0, 10.1)?;
  let _longrun = distance_filter(&running_df, 10.1, 20.0)?;

  // println!("{:?}", sum_distance(&_400m));
  // println!("{:?}", sum_distance(&_1000m));
  // println!("{:?}", sum_distance(&_1200m));
  // println!("{:?}", sum_distance(&_2000m));
  // println!("{:?}", sum_distance(&_5000m));
  // println!("{:?}", sum_distance(&_7000m));
  // println!("{:?}", sum_distance(&_10000m));
  // println!("{:?}", sum_distance(&_longrun));
  // let only_date_df = running_df.clone()
  //   .apply("Date", only_year_month_column)?
  //   .sort([timestamp_col], Default::default())?;

  // println!("{:?}", only_date_df);
  let mut only_2024_df = year_filter(&running_df, "2567")?;
  let only_year_month = only_2024_df.apply("Date", only_year_month_column)?; 

  
  let month_distance_sum_2024_df = group_sum(
    &only_year_month, 
    "Date", 
    "Distance(km)"
  )?;
  let fill_missing_month_2024 = fill_missing_months(&month_distance_sum_2024_df)?;
  let _monthly_distances_2024 = sort_ascending(&fill_missing_month_2024, "Date")?;



  Ok(())
}