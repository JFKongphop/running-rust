use polars::prelude::*;
use crate::types::polars_type::PolarsGenType;
use itertools::izip;

#[allow(dead_code)]
#[derive(Debug)]
pub struct DateDistance {
  date: String,
  distance: f64,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct DateDistancePaceHr {
  date: String,
  distance: f64,
  pace: String,
  hr: f64
}

pub fn date_distance_vector(
  date_col: &Column, 
  distance_col: &Column
) -> PolarsGenType<Vec<DateDistance>> {
  let dates = date_col.str()?;
  let distances = distance_col.f64()?;

  let vec: Vec<DateDistance> = dates
    .into_no_null_iter()
    .zip(distances.into_no_null_iter())
    .map(|(date, distance)| DateDistance {
      date: date.to_string(),
      distance,
    })
    .collect();

  Ok(vec)
}

pub fn date_distance_pace_hr_vector(
  date_col: &Column, 
  distance_col: &Column,
  pace_col: &Column,
  hr_col: &Column
) -> PolarsGenType<Vec<DateDistancePaceHr>> {
  let dates = date_col.str()?;
  let distances = distance_col.f64()?;
  let paces = pace_col.str()?;
  let hrs = hr_col.f64()?;

  let vec: Vec<DateDistancePaceHr> = izip!(
    dates.into_no_null_iter(),
    distances.into_no_null_iter(),
    paces.into_no_null_iter(),
    hrs.into_no_null_iter()
  )
  .map(|(date, distance, pace, hr)| DateDistancePaceHr {
    date: date.to_string(),
    distance,
    pace: pace.to_string(),
    hr,
  })
  .collect();

  Ok(vec)
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