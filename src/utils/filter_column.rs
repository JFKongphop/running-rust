use polars::prelude::*;
use std::ops::BitAnd;

type PolarsFrame = PolarsResult<DataFrame>;

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