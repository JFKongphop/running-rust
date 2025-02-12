use polars::prelude::*;
use std::ops::{BitAnd, BitOr};

use crate::types::polars_type::PolarsFrame;

use super::apply_column::create_timestamp_column;

pub fn activity_filter(df: &DataFrame, activity: &str) -> PolarsFrame {
  let activity_column = df.column("Activity")?.str()?;
  let mask = activity_column.equal(activity);
  
  df.filter(&mask)
}

pub fn year_filter(df: &DataFrame, year: &str) -> PolarsFrame {
  let date_column = df.column("Date")?.str()?;
  let mask = date_column
    .into_iter()    
    .map(|opt_val| opt_val.map(|val| val.starts_with(year)))
    .collect::<BooleanChunked>();

  df.filter(&mask)
}

pub fn month_filter(df: &DataFrame, year_month: &str) -> PolarsFrame {
  let date_column = df.column("Date")?.str()?;
  let mask = date_column
    .into_iter()
    .map(|date| date.and_then(|d| Some(d.starts_with(year_month))))
    .collect::<BooleanChunked>();

  df.filter(&mask)
}

pub fn date_filter(df: &DataFrame, full_date: &str) -> PolarsFrame {
  let date_column = df.column("Date")?.str()?;
  let mask = date_column
    .into_iter()
    .map(|date| date.and_then(|d| Some(d.starts_with(full_date))))
    .collect::<BooleanChunked>();
  
  df.filter(&mask)
}

pub fn month_range_filter(df: &DataFrame, start_month: &str, end_month: &str) -> PolarsFrame {
  let parts: Vec<&str> = end_month.split('-').collect();
  let mut year: i32 = parts[0].parse().unwrap();
  let mut month: i32 = parts[1].parse().unwrap();

  if month > 10 {
    month = (month + 1) - 12;
    year += 1
  }
  else {
    month += 1
  }

  let end_month = format!("{}-{:02}", year, month);
  let end_month = end_month.as_str();

  let date_column = df.column("Date")?.str()?;
  let mask = date_column
    .gt_eq(start_month)
    .bitand(date_column.lt_eq(end_month));

  df.filter(&mask)
}

pub fn distance_filter(df: &DataFrame, min: f64, max: f64) -> PolarsFrame {
  let distance_column = df.column("Distance(km)")?.f64()?;
  let mask = distance_column
    .gt(min)
    .bitand(distance_column.lt(max));
  
  df.filter(&mask)
}

pub fn null_filter(df: &DataFrame) -> PolarsFrame {
  df.filter(&df.column("Date")?.is_not_null())
}