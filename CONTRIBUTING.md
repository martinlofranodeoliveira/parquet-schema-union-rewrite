# Contributing

This is development version 0.1.0. Keep contributions original, small and within the documented synthetic-data contract. Do not submit private/client code, personal data, credentials or imported production fixtures.

Use the README's pinned preparation and non-root container commands. The required local commands after installation are:

```sh
python -m pytest -q
python -m parquet_union_replay demo --assert
```

CI also builds a real sdist and wheel, installs from both, and exercises them outside the source tree. Preparation may acquire the exact recorded wheels; tests/demo/library operations must never acquire packages, use the network or install/load DuckDB extensions. Do not widen hash pins or backend versions without actual platform qualification and revised records.

For a change, explain the semantic problem, add an independent literal regression before adjusting behavior, and update both READMEs and the contract when observable behavior changes. Include empty declarations, nulls, ordering and failure publication effects where relevant. Do not write tests whose expectations are simply calls into the code under test. Fault injection supplements real I/O failures; it does not replace the non-root permission and truncated-footer gates.

Preserve constant-code error sanitization, exact integer arithmetic, global limits, no-replace publication and cleanup limited to marked owned stages. Do not add permissive casts, arbitrary SQL, remote inputs, telemetry, release workflows or deployment automation. Code in this repository uses the MIT license with organization attribution and no author email.

Use repository issues for public non-sensitive defects. For vulnerabilities, follow SECURITY.md rather than posting details publicly. General contact is the [official homepage](https://lofranotecnologia.com.br).
