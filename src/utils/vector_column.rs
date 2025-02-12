use polars::prelude::*;

#[allow(dead_code)]
#[derive(Debug)]
pub struct MonthlyDistance {
  date: String,
  distance: f64,
}

pub fn dataframe_to_struct_vec(df: &DataFrame) -> PolarsResult<Vec<MonthlyDistance>> {
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
