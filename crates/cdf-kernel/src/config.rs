use crate::{CdfError, Result};

pub fn parse_human_byte_size(label: &str, value: &str) -> Result<u64> {
    let value = value.trim();
    if value.is_empty() {
        return Err(CdfError::contract(format!("{label} requires a byte size")));
    }
    let split = value
        .find(|character: char| !(character.is_ascii_digit() || character == '_'))
        .unwrap_or(value.len());
    let (digits, suffix) = value.split_at(split);
    if digits.is_empty() {
        return Err(CdfError::contract(format!(
            "{label} must start with an integer byte count"
        )));
    }
    let number = digits.replace('_', "").parse::<u64>().map_err(|_| {
        CdfError::contract(format!(
            "{label} must be an integer byte count with an optional suffix"
        ))
    })?;
    if number == 0 {
        return Err(CdfError::contract(format!(
            "{label} must be greater than zero"
        )));
    }
    let multiplier = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024_u64.pow(2),
        "g" | "gb" | "gib" => 1024_u64.pow(3),
        "t" | "tb" | "tib" => 1024_u64.pow(4),
        _ => {
            return Err(CdfError::contract(format!(
                "{label} suffix must be one of B, KiB, MiB, GiB, or TiB"
            )));
        }
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| CdfError::contract(format!("{label} byte size exceeds u64")))
}

#[cfg(test)]
mod tests {
    use super::parse_human_byte_size;

    #[test]
    fn human_byte_size_accepts_binary_suffixes_and_underscores() {
        assert_eq!(parse_human_byte_size("budget", "512").unwrap(), 512);
        assert_eq!(
            parse_human_byte_size("budget", "1_024KiB").unwrap(),
            1024 * 1024
        );
        assert_eq!(
            parse_human_byte_size("budget", "2 GiB").unwrap(),
            2 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn human_byte_size_rejects_zero_and_unknown_suffixes() {
        let zero = parse_human_byte_size("budget", "0").unwrap_err();
        assert!(zero.message.contains("greater than zero"));

        let suffix = parse_human_byte_size("budget", "1XB").unwrap_err();
        assert!(suffix.message.contains("suffix must be one of"));
    }
}
