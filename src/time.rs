use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Europe::Berlin;

pub fn normalize_timestamp(ts: i64) -> i64 {
    if ts < 1_000_000_000_000 {
        ts * 1000
    } else {
        ts
    }
}

pub fn normalize_timestamp_to_second(ts: i64) -> i64 {
    if ts > 1_000_000_000_000 {
        ts / 1000
    } else {
        ts
    }
}

pub fn convert_to_berlin_time(timestamp: i64) -> String {
    let ts_ms = normalize_timestamp(timestamp);
    let timestamp = ts_ms / 1000;
    let datetime = Utc.timestamp_opt(timestamp, 0).unwrap();
    let berlin_time = datetime.with_timezone(&Berlin);
    let time_only = berlin_time.format("%H:%M").to_string();
    
    time_only
}

pub fn convert_to_normal_time(total_seconds: i64) -> String {
    let total_minutes = (total_seconds + 59) / 60;
    let hours = total_minutes / 60;
    let minutes = total_minutes % 60;

    let hour_str = match hours {
        1 => "час",
        2..4 => "часа",
        _ if hours >= 0 => "часов",
        _ => ""
    };
    
    let minute_str = match minutes {
        1 => "минута",
        2..4 => "минуты",
        _ if minutes >= 0 => "минут",
        _ => ""
    };

    format!("{} {} {} {}", hours, hour_str, minutes, minute_str)
}

pub fn convert_to_minutes(total_seconds: i64) -> String {
    let total_minutes = (total_seconds + 59) / 60;
    let minutes = total_minutes % 60;

    let minute_str = match minutes {
        1 => "минута",
        2..4 => "минуты",
        _ if minutes >= 0 => "минут",
        _ => ""
    };

    format!("{} {}", minutes, minute_str)
}

pub fn convert_datetime_str_to_unix(datetime: String) -> i64 {
    let datetime: DateTime<Utc> = datetime.parse().unwrap();
    let timestamp = datetime.timestamp();

    timestamp
}