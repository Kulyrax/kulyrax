---
status: completed
---

# Prebuilt release binaries

## Objective

Installing Kulyrax required a Rust toolchain and a source build, even though using it only means writing YAML. The owner asked for release binaries. A version tag should produce downloadable `kulyrax` binaries for the common platforms.

## Acceptance criteria

- [x] Pushing a `v*` tag builds `kulyrax` for Linux x86_64, macOS arm64, macOS x86_64 and Windows x86_64.
- [x] Each build is archived (`.tar.gz`, or `.zip` on Windows) with both licenses and the README, plus a SHA-256 file.
- [x] A GitHub release is created with all archives, using `RELEASES/<tag>.md` as notes when it exists.
- [x] A tag that doesn't match the workspace version fails before anything is built.
- [x] `kulyrax --version` reports the version (it was unsupported, though `CONTRIBUTING.md` asks bug reporters for it).
- [x] The release process is documented.

## Constraints and assumptions

- Constraint: reuse the actions `ci.yml` already trusts (`actions/checkout@v4`, `dtolnay/rust-toolchain@stable`, `Swatinem/rust-cache@v2`), plus GitHub's own artifact actions and the preinstalled `gh` CLI. No new third-party release action.
- Assumption: `7z` is preinstalled on GitHub's `windows-latest` runner image. It is not on this machine, so the zip step was tested with a stand-in.
- Assumption: the `x86_64-apple-darwin` cross-build on the Apple Silicon `macos-latest` runner succeeds, including the crates that compile C code, using the runner's clang. Not verifiable locally.

## Decisions

- Build macOS Intel by cross-compiling on `macos-latest` rather than using an Intel runner, since GitHub is retiring those.
- Build with `--locked`, so a release uses exactly the committed `Cargo.lock`.
- Branch from `upstream/main`, independent of the other feature branches. The only code touched is one attribute in `crates/cli/src/main.rs`.

## Verification

| Criterion | Check | Observed result |
|---|---|---|
| Version gate | the workflow's check script on the real `Cargo.toml` with `GITHUB_REF_NAME=v0.8.0` and `v0.9.0` | `v0.8.0: matches 0.8.0`; `v0.9.0: MISMATCH with 0.8.0` (exit 1) |
| Release build | `cargo build --release --locked -p kulyrax-cli --target x86_64-pc-windows-msvc` | succeeds in 6m32s; 25 MB `kulyrax.exe` |
| Packaging, zip | the workflow's Package script, extracted from the YAML, with `7z` replaced by Windows `tar -a` | exit 0; `kulyrax-v0.8.0-x86_64-pc-windows-msvc.zip` holds `kulyrax.exe`, both licenses and the README; `sha256sum -c` OK |
| Packaging, tar.gz | the same script with `TARGET=x86_64-unknown-linux-gnu`, `ARCHIVE=tar.gz` (Windows binary copied in as a placeholder) | exit 0; same layout; `shasum -a 256 -c` OK |
| Workflow structure | parse with `serde_yaml` | jobs `check-version` → `build` → `release`; 4 matrix entries with the intended OS, target and archive; `on.push.tags: [v*]`; `permissions.contents: write` |
| `--version` | `kulyrax --version` | before: `error: unexpected argument '--version'`; after: `kulyrax 0.8.0` |
| Gates | `cargo fmt --check`; `cargo clippy -p kulyrax-cli -D warnings` | clean |

Not run:
- The workflow itself on GitHub, because that needs a tag pushed to `Kulyrax/kulyrax`. The first real tag is the end-to-end test, and a failure there creates no release, because `release` only runs after every build succeeds.
- The two macOS builds and the Linux build, which need those runners.

## Risks and open questions

- The README Install section still says "build from source". Once the first tagged release exists, add the download link there; saying so earlier would point at an empty Releases page.
- Binaries are unsigned. macOS Gatekeeper and Windows SmartScreen will warn on first run. Signing needs certificates the owner would have to provide.

## Outcome

Tagging `v<version>` on `main` publishes a GitHub release with `kulyrax` binaries for Linux, macOS (both architectures) and Windows, each with a checksum. `kulyrax --version` now works.
