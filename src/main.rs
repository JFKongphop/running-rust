use dotenv::dotenv;
use running_rust::utils::{
  agg_data::{count_day, count_running, group_sum, sort_ascending, sum_distance},
  apply_column::{activity_to_type, only_date_column, only_year_month_column},
  fetch_data::fetch_text_csv,
  filter_column::{
    activity_filter, date_filter, distance_filter, month_filter, month_range_filter, null_filter,
    year_filter,
  },
  times::{fill_missing_days, fill_missing_months},
  vector_column::{date_distance_vector, date_vector},
};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  dotenv().ok();

  let df = fetch_text_csv().await?;
  let not_null_df = null_filter(&df)?;
  let mut sort_date_df = sort_ascending(&not_null_df, "Date")?;
  let running_df = sort_date_df.apply("Activity", activity_to_type)?;
  let all_distance = sum_distance(&running_df)?;
  let running_date = running_df.apply("Date", only_date_column)?;
  let all_day = count_day(&running_date)?;
  let all_running = count_running(&running_df);

  println!("Total Distance {}", all_distance);
  println!("Total Day {}", all_day);
  println!("Total Running {}", all_running);

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

  let _400m_sum = sum_distance(&_400m)?;
  let _1000m_sum = sum_distance(&_1000m)?;
  let _1200m_sum = sum_distance(&_1200m)?;
  let _2000m_sum = sum_distance(&_2000m)?;
  let _5000m_sum = sum_distance(&_5000m)?;
  let _7000m_sum = sum_distance(&_7000m)?;
  let _10000m_sum = sum_distance(&_10000m)?;
  let _longrun_sum = sum_distance(&_longrun)?;

  let year = "2567";
  let mut only_2024_df = year_filter(&running_df, &year)?;
  let only_year_month = only_2024_df.apply("Date", only_year_month_column)?;
  let _year_2024_sum_distance = sum_distance(&only_year_month)?;
  // println!("Year {} Distance {}", year, _year_2024_sum_distance);

  let month_distance_sum_2024_df = group_sum(&only_year_month, "Date", "Distance(km)")?;
  let fill_missing_month_2024 = fill_missing_months(&month_distance_sum_2024_df)?;
  let _monthly_distances_2024 = sort_ascending(&fill_missing_month_2024, "Date")?;
  // let _vec_monthly_distances_2024 = date_distance_vector(&_monthly_distances_2024);

  // println!("{:#?}", _vec_monthly_distances_2024);

  let jan_2025_df = month_filter(&running_df, "2568-01")?;
  let jan_2025_day_sum_df = group_sum(&jan_2025_df, "Date", "Distance(km)")?;
  let mut jan_2025_sorted = sort_ascending(&jan_2025_day_sum_df, "Date")?;
  let date_col_jan_2025 = jan_2025_sorted.column("Date")?;
  let distance_col_jan_2025 = jan_2025_sorted.column("Distance(km)_sum")?;
  let _vec_jan_2025_sorted = date_distance_vector(date_col_jan_2025, distance_col_jan_2025)?;
  // println!("jan 2025 {:#?}", _vec_jan_2025_sorted/*.select(["Date", "Distance(km)", "Pace(min)"])*/);
  let _jan_2025_sorted = jan_2025_sorted.rename("Distance(km)_sum", "Distance(km)".into())?;
  // println!("{:#?}", jan_2025_sorted);

  let _jan_14_2025 = date_filter(&running_df, "2568-01-14")?;

  let _oct_dec_2024 = month_range_filter(&running_df, "2567-10", "2567-12")?;
  // println!("{}", _oct_dec_2024);

  // let sum_oct_dec_2024 = sum_distance(&_oct_dec_2024)?;
  // println!("{}", _sum_oct_dec_2024);

  // println!("{}", running_df);

  let date_col = running_df.column("Date")?;
  let _unique_date = date_vector(&date_col)?;
  // println!("{:#?}", _unique_date);

  //
  let jan_2025_day_sum_df = group_sum(&jan_2025_df, "Date", "Distance(km)")?;
  let jan_2025_sorted = sort_ascending(&jan_2025_day_sum_df, "Date")?;
  let a = fill_missing_days(&jan_2025_sorted)?;
  println!("{}", a);

  Ok(())
}
