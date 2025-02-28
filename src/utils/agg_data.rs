use polars::prelude::*;

use crate::types::polars_type::{PolarsFrame, PolarsGenType};

pub fn sum_distance(df: &DataFrame) -> PolarsGenType<f64> {
  let distance_column = df.column("Distance(km)")?.f64()?;
  Ok(distance_column.sum().unwrap_or(0.0))
}

pub fn sort_ascending(df: &DataFrame, column: &str) -> PolarsResult<DataFrame> {
  df.sort([column], Default::default())
}

#[allow(deprecated)]
pub fn group_sum(df: &DataFrame, group_column: &str, sum_column: &str) -> PolarsFrame {
  df.group_by([group_column])?.select([sum_column]).sum()
}

pub fn group_count(df: &DataFrame, group_column: &str, sort_column: &str) -> PolarsFrame {
  let group_counted = df.group_by([group_column])?.count()?.sort([sort_column], Default::default())?;
  let mut new_df = group_counted.select(["Pace Group", "Date_count"])?;

  new_df.rename("Date_count", "Activity".into()).cloned()
}

pub fn count_running(df: &DataFrame) -> usize {
  df.height()
}

pub fn count_day(df: &DataFrame) -> PolarsGenType<usize> {
  df.column("Date")?.n_unique()
}

pub fn join_df(left_df: &DataFrame, right_df: &DataFrame, left_column: &str, right_column: &str) -> PolarsFrame {
  left_df
    .left_join(
      right_df, 
      [left_column], 
      [right_column]
    )?
    .fill_null(FillNullStrategy::Zero)
}