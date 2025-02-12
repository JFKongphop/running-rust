use polars::prelude::*;
use crate::types::polars_type::PolarsGenType;

#[allow(dead_code)]
#[derive(Debug)]
pub struct MonthlyDistance {
  date: String,
  distance: f64,
}

pub fn dataframe_to_struct_vec(df: &DataFrame) -> PolarsGenType<Vec<MonthlyDistance>> {
  let dates = df.column("Date")?.str()?;
  let distances = df.column("Distance(km)_sum")?.f64()?;

  let struct_vec: Vec<MonthlyDistance> = dates
    .into_no_null_iter()
    .zip(distances.into_no_null_iter())
    .map(|(date, distance)| MonthlyDistance {
      date: date.to_string(),
      distance,
    })
    .collect();

  Ok(struct_vec)
}

pub fn date_vector(date_col: &Column) -> PolarsGenType<Vec<String>> {
  let mut unique_date: Vec<String> = date_col
    .str()?
    .unique()?
    .into_iter()
    .flatten()
    .map(|s| s.to_string())
    .collect();

  unique_date.sort();
  
  Ok(unique_date)
}