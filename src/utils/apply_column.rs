use std::collections::HashMap;

use polars::prelude::*;

use crate::types::polars_type::PolarsFrame;

use super::times::{date_to_timestamp, number_to_month};

pub fn activity_to_type(activity_col: &Column) -> Column {
  activity_col
    .str()
    .unwrap()
    .into_iter()
    .map(|opt_name: Option<&str>| match opt_name {
      Some(val) if val.contains("indoor") => Some("indoor"),
      _ => Some("outdoor"),
    })
    .collect::<StringChunked>()
    .into_column()
}

pub fn only_year_month_column(date_col: &Column) -> Column {
  date_col
    .str()
    .unwrap()
    .into_iter()
    .map(|d| d.and_then(|dd| Some(&dd[..7])))
    .collect::<StringChunked>()
    .into_column()
}

pub fn only_date_column(date_col: &Column) -> Column {
  date_col
    .str()
    .unwrap()
    .into_iter()
    .map(|d| d.and_then(|dd| Some(&dd[..10])))
    .collect::<StringChunked>()
    .into_column()
}

pub fn convert_date_month(str_val: &Column) -> Column {
  str_val
    .str()
    .unwrap()
    .into_iter()
    .map(|d| {
      d.and_then(|dd| {
        let month_str = &dd[5..7];
        month_str
          .parse::<u32>()
          .ok()
          .and_then(|month_num| number_to_month(month_num))
      })
    })
    .collect::<StringChunked>()
    .into_column()
}

pub fn create_timestamp_column(running_df: &DataFrame) -> PolarsFrame {
  let timestamp_col = running_df
    .column("Date")?
    .str()?
    .into_iter()
    .map(|date_opt| date_opt.and_then(date_to_timestamp))
    .collect::<Int64Chunked>()
    .into_series()
    .with_name("Timestamp".into());

  let mut new_df = running_df.clone();
  new_df.with_column(timestamp_col)?;

  Ok(new_df)
}

fn grouping_pace(pace: &str) -> Option<String> {
  let pace_map: HashMap<char, &str> = [
    ('2', "2:00-3:00"),
    ('3', "3:00-4:00"),
    ('4', "4:00-5:00"),
    ('5', "5:00-6:00"),
    ('6', "6:00-7:00"),
    ('7', "7:00-8:00"),
    ('8', "8:00-9:00"),
  ]
  .into_iter()
  .collect();

  pace
    .chars()
    .next()
    .and_then(|c| pace_map.get(&c).map(|&s| s.to_string()))
    .or(Some("Other".to_string()))
}

pub fn create_pace_column(running_df: &DataFrame) -> PolarsFrame {
  let pace_range_column = running_df
    .column("Pace(min)")?
    .str()?
    .into_iter()
    .map(|date_opt| date_opt.and_then(grouping_pace))
    .collect::<StringChunked>()
    .into_series()
    .with_name("Pace Group".into());

  let mut new_df = running_df.clone();
  new_df.with_column(pace_range_column)?;

  Ok(new_df)
}
