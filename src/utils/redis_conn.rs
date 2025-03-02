use std::{env, error::Error};

use redis::Client;

pub fn redis_connect() -> Result<Client, Box<dyn Error>> {
  let redis_url = env::var("REDIS_KEY").map_err(|e| format!("Missing REDIS_KEY env variable: {}", e))?;
  
  Ok(redis::Client::open(redis_url)?)    
} 
