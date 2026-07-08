use std::time::Duration;

pub(crate) fn humanize_rows(rows: u64) -> String {
    if rows < 1_000 {
        return rows.to_string();
    }
    let (divisor, suffix) = if rows < 1_000_000 {
        (1_000.0, "k")
    } else if rows < 1_000_000_000 {
        (1_000_000.0, "M")
    } else {
        (1_000_000_000.0, "B")
    };
    format_compact(rows as f64 / divisor, suffix)
}

pub(crate) fn humanize_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format_decimal(value, UNITS[unit])
}

pub(crate) fn humanize_rate(bytes_per_second: f64) -> String {
    if bytes_per_second < 1024.0 {
        return format!("{} B/s", bytes_per_second.round() as u64);
    }
    let bytes = humanize_bytes(bytes_per_second.round() as u64);
    format!("{bytes}/s")
}

pub(crate) fn humanize_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    if seconds < 60 {
        return format!("{seconds}s");
    }
    let minutes = seconds / 60;
    let seconds = seconds % 60;
    if minutes < 60 {
        return format!("{minutes}m {seconds:02}s");
    }
    let hours = minutes / 60;
    let minutes = minutes % 60;
    format!("{hours}h {minutes:02}m")
}

fn format_decimal(value: f64, suffix: &str) -> String {
    if value >= 10.0 || value.fract() == 0.0 {
        format!("{value:.0} {suffix}")
    } else {
        format!("{value:.1} {suffix}")
    }
    .replace("  ", " ")
}

fn format_compact(value: f64, suffix: &str) -> String {
    if value >= 100.0 || value.fract() == 0.0 {
        format!("{value:.0}{suffix}")
    } else {
        format!("{value:.1}{suffix}")
    }
}
