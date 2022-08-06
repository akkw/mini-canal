use std::time::{Duration, UNIX_EPOCH};
use chrono::{DateTime, Utc};

pub fn timestamp_to_time(second: u64) -> String {
    let d = UNIX_EPOCH + Duration::from_secs(second);
    // Create DateTime from SystemTime
    let datetime = DateTime::<Utc>::from(d);
    // Formats the combined date and time with the specified format string.
    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}