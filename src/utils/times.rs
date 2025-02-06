use chrono::{NaiveDateTime, NaiveDate};

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