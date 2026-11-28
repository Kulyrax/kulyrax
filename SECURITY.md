# Security policy

## Reporting a vulnerability

Please **do not** open a public issue, pull request or discussion for a
security problem.

Report it privately through GitHub:
[**Report a vulnerability**](https://github.com/Kulyrax/kulyrax/security/advisories/new)
(the repository's *Security* tab, then *Report a vulnerability*). Only the
maintainers can read it.

Include what you can:
- the Kulyrax version (`kulyrax --version`) and how you run it (CLI, `ingest`,
  embedded);
- the backends and sources involved;
- what an attacker can do, and what they need first (access to the event log,
  to the config, to a database);
- the steps or a test that reproduce it.

## What happens next

- We acknowledge the report within 7 days.
- We confirm or rule out the problem, and agree a timeline with you. We aim to
  ship a fix within 90 days.
- We publish a GitHub security advisory with the fix, and request a CVE for
  it. Unless you ask otherwise, we credit you in it.

Until the advisory is published, please keep the details private.

## Supported versions

| Version | Security fixes |
|---|---|
| 1.0.x (from 1.0.0-rc.1) | yes |
| 0.12.x | until 1.0.0 is released; upgrade to 0.12.2 or later |
| 0.11.x and earlier | no; upgrade |

Fixes go into the latest release of each supported line. Versions up to and
including 0.12.0 are affected by
[GHSA-6chh-gx4c-448g](https://github.com/Kulyrax/kulyrax/security/advisories/GHSA-6chh-gx4c-448g)
and
[GHSA-x8vc-9h5c-gh55](https://github.com/Kulyrax/kulyrax/security/advisories/GHSA-x8vc-9h5c-gh55);
see [`RELEASES/v0.12.1.md`](RELEASES/v0.12.1.md).

## Scope

Kulyrax reads an event log and writes projections into databases you
configure. Of particular interest:
- writes outside a store's root, or into another store's data;
- credentials appearing in errors, logs or reports;
- a crafted event, mapping or config that makes Kulyrax lose or corrupt
  projected data, or skip events silently;
- the gRPC ingest server (`kulyrax ingest`) accepting what it should refuse.

Out of scope: problems that need control of the config, the mappings or the
database already, and the dev-only file stores (see the README's known
limits) under workloads they are not meant for.
