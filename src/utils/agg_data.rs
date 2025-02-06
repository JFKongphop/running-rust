use polars::prelude::*;

pub fn sum_distance(df: &DataFrame) -> PolarsResult<f64> {
  let distance_column = df.column("Distance(km)")?.f64()?;
  Ok(distance_column.sum().unwrap_or(0.0))
}