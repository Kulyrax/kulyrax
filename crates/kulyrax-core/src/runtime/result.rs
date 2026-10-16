use crate::adapter::AdapterError;
use crate::routing::StorageKind;

pub struct ExecutionResult {
    pub results: Vec<(StorageKind, Result<(), AdapterError>)>, // Vec of (adapter_kind, success/failure)
                                                               // Fields here...
}

impl ExecutionResult {
    pub fn new(results: Vec<(StorageKind, Result<(), AdapterError>)>) -> Self {
        Self { results }
    }
    // Method to check if all succeeded
    pub fn all_succeeded(&self) -> bool {
        self.results.iter().all(|(_, result)| result.is_ok())
    }

    // Method to check if any succeeded
    pub fn any_succeeded(&self) -> bool {
        self.results.iter().any(|(_, result)| result.is_ok())
    }

    // Method to check if any failed
    pub fn any_failed(&self) -> bool {
        self.results.iter().any(|(_, result)| result.is_err())
    }
}
