use polars::prelude::*;
use chrono::{
  NaiveDateTime, 
  NaiveDate
};

use crate::types::polars_type::PolarsFrame;

pub fn number_to_month(num: u32) -> Option<String> {
  NaiveDate::from_ymd_opt(2000, num, 1)
    .map(|d| d.format("%B").to_string())
}

pub fn date_to_timestamp(full_date: &str) -> Option<i64> {
  let format = "%Y-%m-%d %H:%M:%S";
  let only_start = &full_date[..19];
  let (db_year, date_time) = only_start.split_at(4);
  let year = db_year.parse::<i32>().ok()? - 543;
  let date_str = format!("{}{}", year, date_time);

  NaiveDateTime::parse_from_str(&date_str, format)
    .ok()
    .map(|dt| dt.and_utc().timestamp())
}

pub fn convert_date_timestamp(date: &str) ->i64 {
  let format = "%Y-%m-%d %H:%M:%S";
  let date = NaiveDateTime::parse_from_str(&date, format)
    .ok()
    .expect("Invalid date");

  date.and_utc().timestamp()
}

pub fn fill_missing_months(df: &DataFrame) -> PolarsFrame {
  let first_row = df.column("Date")?.str()?;

  let mut year = "";
  if let Some(date) = first_row.get(0) {
    year = &date[..4];
    println!("Year: {}", year);
  }

  let months: Vec<String> = (1..=12)
    .map(|m| format!("{}-{:02}", year, m)) 
    .collect();
  
  let full_months_df = df!("Date" => &months)?;

  let result = full_months_df
    .left_join(df, ["Date"], ["Date"])?
    .fill_null(FillNullStrategy::Zero)?;

  Ok(result)
}