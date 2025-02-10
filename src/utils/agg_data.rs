use polars::prelude::*;

use crate::types::polars_type::{
  PolarsFrame, 
  PolarsGenType
};

pub fn sum_distance(df: &DataFrame) -> PolarsGenType<f64> {
  let distance_column = df.column("Distance(km)")?.f64()?;
  Ok(distance_column.sum().unwrap_or(0.0))
}

pub fn sort_ascending(df: &DataFrame, column: &str) -> PolarsResult<DataFrame> {
  df.sort([column], Default::default())
}

#[allow(deprecated)]
pub fn group_sum(df: &DataFrame, group_column: &str, sum_column: &str) -> PolarsFrame {
  df
    .group_by([group_column])?
    .select([sum_column])
    .sum()
}

pub fn count_running(df: &DataFrame) -> usize {
  df.height()
}

pub fn count_day(df: &DataFrame) -> PolarsGenType<usize> {
  df.column("Date")?.n_unique()
}