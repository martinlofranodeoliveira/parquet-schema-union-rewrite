# Changelog

## 0.1.0 — Unreleased development version

- Explicit bounded union of bool, signed int32/int64 and ordinary UTF-8 strings, including integer-indexed string dictionaries.
- Observed-only UTF-8 dictionary domains, typed missing nulls and exact source-occurrence/row provenance.
- Local non-following file admission, global schema/row/group limits and sanitized constant errors.
- Real uncompressed PyArrow rewrite and independent restricted DuckDB row/type/null/aggregate verification.
- Marked same-filesystem staging and Linux atomic no-replace directory publication, including distinct post-commit synchronization failure.
- Public Python API, CLI, offline synthetic demo, semantic receipts and integrity inspection.
- Literal regression goldens, real footer/permission failures, adversarial admission tests, transaction fault injection and outside-checkout install gates.
- English/Portuguese documentation and bounded non-root container CI; no release or remote deployment workflow.
