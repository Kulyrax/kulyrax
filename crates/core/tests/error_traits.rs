//! Every public error in kulyrax-core is `std::error::Error + Send + Sync +
//! 'static` (decision 2026-11-26-error-chains). A field added later that is
//! not `Send` fails this build. `source()` returns the typed cause where it
//! is a YAML, JSON or I/O error.

use std::error::Error;

use kulyrax_core::adapter::AdapterError;
use kulyrax_core::adapter::document::adapter::DocumentError;
use kulyrax_core::adapter::graph::adapter::GraphError;
use kulyrax_core::adapter::keyvalue::adapter::KvError;
use kulyrax_core::adapter::sql::adapter::SqlError;
use kulyrax_core::format::{FormatError, FormatProblem};
use kulyrax_core::pipeline::{InitError, LoadError, PipelineError, load_config, load_mappings};
use kulyrax_core::rebuild::RebuildError;
use kulyrax_core::replay::ReplayLoadError;
use kulyrax_core::runtime::config::ConfigError;
use kulyrax_core::runtime::dependency_graph::DependencyOrderError;
use kulyrax_core::source::SourceError;
use kulyrax_core::upcast::UpcastError;
use kulyrax_core::yaml::YamlError;

fn is_error<E: Error + Send + Sync + 'static>() {}

#[test]
fn every_public_error_is_send_sync_static() {
    is_error::<AdapterError>();
    is_error::<SqlError>();
    is_error::<DocumentError>();
    is_error::<KvError>();
    is_error::<GraphError>();
    is_error::<PipelineError>();
    is_error::<LoadError>();
    is_error::<InitError>();
    is_error::<ReplayLoadError>();
    is_error::<ConfigError>();
    is_error::<DependencyOrderError>();
    is_error::<SourceError>();
    is_error::<UpcastError>();
    is_error::<YamlError>();
    is_error::<RebuildError>();
    is_error::<FormatError>();
}

#[test]
fn a_format_refusal_chains_to_the_format_error() {
    let refused = AdapterError::from(FormatError::new(
        "store",
        FormatProblem::Newer {
            found: 3,
            supported: 2,
        },
    ));
    let cause = refused.source().expect("a source");
    assert!(cause.downcast_ref::<FormatError>().is_some(), "{cause:?}");
    assert!(refused.to_string().contains("format 3"), "{refused}");
}

#[test]
fn a_config_parse_error_chains_to_the_yaml_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("config.yaml");
    std::fs::write(&path, "version: one\n").expect("write");
    let err = load_config(&path).expect_err("refused");
    let cause = err.source().expect("a source");
    assert!(cause.downcast_ref::<YamlError>().is_some(), "{cause:?}");
}

#[test]
fn a_mapping_parse_error_chains_to_the_yaml_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    for k in ["sql", "document"] {
        std::fs::create_dir_all(dir.path().join(k)).expect("mkdir");
    }
    std::fs::write(dir.path().join("sql").join("m.yaml"), "event: [\n").expect("write");
    let err = load_mappings(dir.path()).expect_err("refused");
    let text = err.to_string();
    assert!(text.starts_with("Invalid SQL mapping: "), "{text}");
    let load = err
        .source()
        .and_then(|e| e.downcast_ref::<LoadError>())
        .expect("a LoadError");
    let yaml = load.source().expect("a source");
    assert!(yaml.downcast_ref::<YamlError>().is_some(), "{yaml:?}");
}

#[test]
fn a_missing_mapping_dir_chains_to_the_io_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let err = load_mappings(&dir.path().join("none")).expect_err("refused");
    let io = err
        .source()
        .and_then(|e| e.source())
        .and_then(|e| e.downcast_ref::<std::io::Error>());
    assert!(io.is_some(), "{err}");
}

#[test]
fn a_dependency_order_error_reads_like_the_dispatch_report() {
    let err = DependencyOrderError::Cycle {
        adapters: vec!["a".into(), "b".into()],
    };
    assert_eq!(err.to_string(), "cyclic adapter dependencies among: a, b");
}

#[test]
fn a_backend_refusal_stays_a_refusal() {
    use kulyrax_core::adapter::sql::adapter::SqlError;
    let refused = RebuildError::from(SqlError::RebuildRefused("another writer".into()));
    assert!(matches!(refused, RebuildError::Refused(_)), "{refused:?}");
    let failed = RebuildError::from(SqlError::ExecutionFailed("disk full".into()));
    assert!(matches!(failed, RebuildError::Failed(_)), "{failed:?}");
}
