use polars::{
  error::PolarsResult, 
  frame::DataFrame
};

pub type PolarsFrame = PolarsResult<DataFrame>;
pub type PolarsGenType<T> = PolarsResult<T>;
