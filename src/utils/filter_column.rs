use polars::prelude::*;
use std::ops::BitAnd;

use super::times::convert_date_timestamp;

pub fn activity_filter(df: &DataFrame, activity: &str) -> PolarsResult<DataFrame> {
  let activity_column = df.column("Activity")?.str()?;
  let mask = activity_column.equal(activity);
  
  df.filter(&mask)
}

pub fn year_filter(df: &DataFrame, year: &str) -> PolarsResult<DataFrame> {
  let date_column = df.column("Date")?.str()?;
  let mask = date_column
    .into_iter()    
    .map(|opt_val| opt_val.map(|val| val.starts_with(year)))
    .collect::<BooleanChunked>();

  df.filter(&mask)
}

pub fn distance_filter(df: &DataFrame, min: f64, max: f64) -> PolarsResult<DataFrame> {
  let distance_column = df.column("Distance(km)")?.f64()?;
  let mask = distance_column
    .gt(min)
    .bitand(distance_column.lt(max));
  
  df.filter(&mask)
}

pub fn month_filter(df: &DataFrame, year_month: &str) -> PolarsResult<DataFrame> {
  let date_part: Vec<i64> = year_month
    .split('-')
    .filter_map(|part| part.parse::<i64>().ok()) 
    .collect();
  let (year, month) = (date_part[0], date_part[1]);

  let end_month = if month == 12 {
    format!("{}-{}", year + 1, 1)
  } else {
    format!("{}-{}", year, month + 1)
  };

  let start_date = format!("{}-01 00:00:00", year_month);
  let end_date = format!("{}-01 00:00:00", end_month);

  let start_timestamp = convert_date_timestamp(&start_date);
  let end_timestamp = convert_date_timestamp(&end_date);

  let distance_column = df.column("Timestamp")?.i64()?;
  let mask = distance_column
    .gt(start_timestamp)
    .bitand(distance_column.lt(end_timestamp));

  df.filter(&mask)
}