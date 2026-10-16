//! Phase 10.2: pure-function upcaster registry for payload schema evolution.
//!
//! Upcasters transform a historical event payload one version step at a time
//! ($V_k \to V_{k+1}$). The registry resolves and applies a contiguous chain
//! of steps ($V_{\text{src}} \to \dots \to V_{\text{target}}$); a broken chain
//! fails fast before any transformation is applied.

use core::fmt;
use std::collections::HashMap;

use serde_json::Value;

/// Side-effect-free transformation of an event payload from `source_version()`
/// to `source_version() + 1`.
///
/// Implementations MUST NOT perform I/O, database reads, or clock access —
/// upcasting must be deterministic and reproducible from the payload alone.
/// The signature only exposes the payload `Value`, so envelope fields
/// (`event_id`, `timestamp`, headers) are structurally immutable to upcasters.
pub trait Upcaster: Send + Sync {
    /// Event type this upcaster applies to.
    fn event_type(&self) -> &str;
    /// Version this upcaster reads from; it always produces `source_version() + 1`.
    fn source_version(&self) -> u32;
    /// Transform the payload. Returns `Err(reason)` on failure.
    fn upcast(&self, payload: Value) -> Result<Value, String>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpcastError {
    /// An upcaster's transform function failed.
    UpcastFailed {
        event_type: String,
        source_version: u32,
        reason: String,
    },
    /// No unbroken chain of upcasters exists from `from_version` to `to_version`;
    /// `missing_at` is the first source version with no registered upcaster.
    MissingLink {
        event_type: String,
        from_version: u32,
        to_version: u32,
        missing_at: u32,
    },
    /// `to_version < from_version`: downcasting is strictly forbidden.
    Downcast {
        event_type: String,
        from_version: u32,
        to_version: u32,
    },
}

impl fmt::Display for UpcastError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UpcastError::UpcastFailed {
                event_type,
                source_version,
                reason,
            } => write!(
                f,
                "upcast failed for event {:?} at version {}: {}",
                event_type, source_version, reason
            ),
            UpcastError::MissingLink {
                event_type,
                from_version,
                to_version,
                missing_at,
            } => write!(
                f,
                "no upcaster chain for event {:?} from v{} to v{}: missing link at v{}",
                event_type, from_version, to_version, missing_at
            ),
            UpcastError::Downcast {
                event_type,
                from_version,
                to_version,
            } => write!(
                f,
                "cannot downcast event {:?} from v{} to v{}",
                event_type, from_version, to_version
            ),
        }
    }
}

impl std::error::Error for UpcastError {}

/// Upcasters indexed by `(event_type, source_version)`. At most one upcaster
/// per step; registering a second one for the same key replaces the first.
#[derive(Default)]
pub struct UpcasterRegistry {
    upcasters: HashMap<(String, u32), Box<dyn Upcaster>>,
}

impl UpcasterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, upcaster: Box<dyn Upcaster>) {
        let key = (upcaster.event_type().to_string(), upcaster.source_version());
        self.upcasters.insert(key, upcaster);
    }

    /// Resolve the ordered source versions to step through from `from_version`
    /// to `to_version`, failing fast if any step in the chain is missing.
    pub fn resolve_chain(
        &self,
        event_type: &str,
        from_version: u32,
        to_version: u32,
    ) -> Result<Vec<u32>, UpcastError> {
        if to_version < from_version {
            return Err(UpcastError::Downcast {
                event_type: event_type.to_string(),
                from_version,
                to_version,
            });
        }
        let mut steps = Vec::with_capacity((to_version - from_version) as usize);
        let mut v = from_version;
        while v < to_version {
            if !self.upcasters.contains_key(&(event_type.to_string(), v)) {
                return Err(UpcastError::MissingLink {
                    event_type: event_type.to_string(),
                    from_version,
                    to_version,
                    missing_at: v,
                });
            }
            steps.push(v);
            v += 1;
        }
        Ok(steps)
    }

    /// Upcast `payload` for `event_type` from `from_version` to `to_version`.
    /// Resolves the full chain before applying any step (fail fast on a broken chain).
    pub fn upcast(
        &self,
        event_type: &str,
        payload: Value,
        from_version: u32,
        to_version: u32,
    ) -> Result<Value, UpcastError> {
        let steps = self.resolve_chain(event_type, from_version, to_version)?;
        let mut value = payload;
        for source_version in steps {
            let upcaster = &self.upcasters[&(event_type.to_string(), source_version)];
            value = upcaster
                .upcast(value)
                .map_err(|reason| UpcastError::UpcastFailed {
                    event_type: event_type.to_string(),
                    source_version,
                    reason,
                })?;
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct AddField {
        version: u32,
        key: &'static str,
        value: Value,
    }

    impl Upcaster for AddField {
        fn event_type(&self) -> &str {
            "UserCreated"
        }
        fn source_version(&self) -> u32 {
            self.version
        }
        fn upcast(&self, payload: Value) -> Result<Value, String> {
            let mut obj = payload
                .as_object()
                .cloned()
                .ok_or_else(|| "payload must be an object".to_string())?;
            obj.insert(self.key.to_string(), self.value.clone());
            Ok(Value::Object(obj))
        }
    }

    struct AlwaysFails;

    impl Upcaster for AlwaysFails {
        fn event_type(&self) -> &str {
            "UserCreated"
        }
        fn source_version(&self) -> u32 {
            1
        }
        fn upcast(&self, _payload: Value) -> Result<Value, String> {
            Err("boom".to_string())
        }
    }

    #[test]
    fn same_version_passthrough_returns_payload_unchanged() {
        let registry = UpcasterRegistry::new();
        let payload = json!({ "id": "u1" });
        let out = registry
            .upcast("UserCreated", payload.clone(), 3, 3)
            .unwrap();
        assert_eq!(out, payload);
    }

    #[test]
    fn multi_step_chain_applies_in_order() {
        let mut registry = UpcasterRegistry::new();
        registry.register(Box::new(AddField {
            version: 1,
            key: "v2_field",
            value: json!(true),
        }));
        registry.register(Box::new(AddField {
            version: 2,
            key: "v3_field",
            value: json!(true),
        }));

        let payload = json!({ "id": "u1" });
        let out = registry.upcast("UserCreated", payload, 1, 3).unwrap();

        assert_eq!(out["id"], json!("u1"));
        assert_eq!(out["v2_field"], json!(true));
        assert_eq!(out["v3_field"], json!(true));
    }

    #[test]
    fn broken_chain_fails_fast_before_applying_any_step() {
        let mut registry = UpcasterRegistry::new();
        // Only v1 -> v2 registered; v2 -> v3 is missing.
        registry.register(Box::new(AddField {
            version: 1,
            key: "v2_field",
            value: json!(true),
        }));

        let payload = json!({ "id": "u1" });
        let err = registry.upcast("UserCreated", payload, 1, 3).unwrap_err();

        match err {
            UpcastError::MissingLink { missing_at, .. } => assert_eq!(missing_at, 2),
            other => panic!("expected MissingLink, got {:?}", other),
        }
    }

    #[test]
    fn downcast_is_rejected() {
        let registry = UpcasterRegistry::new();
        let payload = json!({ "id": "u1" });
        let err = registry.upcast("UserCreated", payload, 2, 1).unwrap_err();
        assert!(matches!(err, UpcastError::Downcast { .. }));
    }

    #[test]
    fn upcaster_failure_is_reported_with_context() {
        let mut registry = UpcasterRegistry::new();
        registry.register(Box::new(AlwaysFails));

        let payload = json!({ "id": "u1" });
        let err = registry.upcast("UserCreated", payload, 1, 2).unwrap_err();

        match err {
            UpcastError::UpcastFailed {
                source_version,
                reason,
                ..
            } => {
                assert_eq!(source_version, 1);
                assert_eq!(reason, "boom");
            }
            other => panic!("expected UpcastFailed, got {:?}", other),
        }
    }

    #[test]
    fn resolve_chain_returns_ordered_steps() {
        let mut registry = UpcasterRegistry::new();
        registry.register(Box::new(AddField {
            version: 1,
            key: "a",
            value: json!(1),
        }));
        registry.register(Box::new(AddField {
            version: 2,
            key: "b",
            value: json!(2),
        }));

        let steps = registry.resolve_chain("UserCreated", 1, 3).unwrap();
        assert_eq!(steps, vec![1, 2]);
    }
}
