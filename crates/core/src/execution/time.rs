use serde::{Deserialize, Deserializer, Serializer};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn serialize<S>(time: &SystemTime, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let duration = time
        .duration_since(UNIX_EPOCH)
        .map_err(serde::ser::Error::custom)?;

    serializer.serialize_u64(duration.as_millis() as u64)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<SystemTime, D::Error>
where
    D: Deserializer<'de>,
{
    let millis = u64::deserialize(deserializer)?;
    Ok(UNIX_EPOCH + std::time::Duration::from_millis(millis))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;

    #[derive(Serialize, Deserialize)]
    struct Wrapper(#[serde(with = "crate::execution::time")] SystemTime);

    #[test]
    fn round_trips_through_millis_since_epoch() {
        let t = UNIX_EPOCH + Duration::from_millis(1_700_000_000_123);
        let json = serde_json::to_string(&Wrapper(t)).unwrap();

        assert_eq!(json, "1700000000123");

        let back: Wrapper = serde_json::from_str(&json).unwrap();
        assert_eq!(back.0, t);
    }

    #[test]
    fn zero_deserializes_to_unix_epoch() {
        let back: Wrapper = serde_json::from_str("0").unwrap();
        assert_eq!(back.0, UNIX_EPOCH);
    }

    #[test]
    fn serialize_truncates_sub_millisecond_precision() {
        let t = UNIX_EPOCH + Duration::from_micros(1_500);
        let json = serde_json::to_string(&Wrapper(t)).unwrap();

        assert_eq!(json, "1");
    }
}
