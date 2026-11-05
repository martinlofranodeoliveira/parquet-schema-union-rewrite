# Parquet Schema Union Rewrite

**Development version 0.1.0.** An original [Lofrano Tecnologia](https://lofranotecnologia.com.br) project for small, owned **synthetic** Parquet fixtures. It unifies an explicit schema subset, preserves record order, normalizes observed string dictionaries, and adds source provenance. PyArrow does real I/O; restricted DuckDB SQL independently checks the written artifact before publication.

This is not a general ingestion service, a security sandbox, a benchmark, or universal Parquet conformance. No model, paid API, telemetry, network access, extension installation, or external dataset is used by library operations, tests, or the demo.

## Install and validate

Target: **CPython 3.13, Linux x64**, PyArrow **26.0.1**, DuckDB **2.0.0**. Qualification used CPython 3.13.18. Exact wheel hashes and build/test versions are in `requirements.lock`; their platform scope and provenance are in [qualification](docs/qualification.md). Do dependency preparation separately from offline operation.

From an unpacked source directory, in a non-root Python environment:

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install --only-binary=:all: --require-hashes -r requirements.lock
python -m pip install --no-index --no-build-isolation --no-deps .
python -m pytest -q
python -m parquet_union_replay demo --assert
python -m parquet_union_replay demo --assert --json
```

Preparation can use a pre-provisioned wheelhouse instead of an index:

```sh
python -m pip install --no-index --find-links /opt/wheelhouse --only-binary=:all: --require-hashes -r requirements.lock
```

The hash list describes actual qualified wheels, not all platforms. A different wheel fails closed rather than silently changing the environment. Do not regenerate pins merely to bypass that failure.

### Public-base container, no host mounts

The Dockerfile explicitly prepares dependencies and builds an sdist, a wheel, and a source installation. It then selects UID 1000. Network access is needed only for image/dependency preparation, not execution. No private image is required.

```sh
docker build --platform linux/amd64 -t parquet-union-local .
docker run --rm --network none --cap-drop ALL --security-opt no-new-privileges --pids-limit 256 --memory 1g --cpus 2 --read-only --tmpfs /tmp:rw,nosuid,nodev,size=256m parquet-union-local python -m pytest -q
docker run --rm --network none --cap-drop ALL --security-opt no-new-privileges --pids-limit 256 --memory 1g --cpus 2 --read-only --tmpfs /tmp:rw,nosuid,nodev,size=256m --workdir /tmp parquet-union-local python -I -m parquet_union_replay demo --assert --json
docker run --rm --network none --cap-drop ALL --security-opt no-new-privileges --pids-limit 256 --memory 1g --cpus 2 --read-only --tmpfs /tmp:rw,nosuid,nodev,size=256m --workdir /tmp --env PYTHONPATH=/opt/source-install parquet-union-local python -m parquet_union_replay demo --assert --json
```

The last two commands use installed packages outside the checkout. The permission regression requires non-root execution and fails when tests are run as root. There are no host mounts, credentials or privileged containers.

## Deterministic synthetic demo

```text
Synthetic union: rows=4; sum(id)=10; non-null note=1
Observed color dictionary: green, red
Independent verification completed; JSON follows.
```

The following JSON line includes actual runtime versions. `--json` suppresses the three human-readable lines. Expected semantic fields are `rows=4`, `id_sum=10`, `note_nonnull=1`, dictionary `["green","red"]`, groups green=1/red=2/null=1 and provenance `[[0,0],[0,1],[1,0],[1,1]]`. No timestamps or temporary paths appear.

Sources A/B contain literal records `(1,red)`, `(2,null)`, `(3,green,x)`, `(4,red,null)`. A's unused dictionary value `blue` is removed. Output columns are `color,id,note,__source_file,__source_row`, with `id` promoted to int64. Demo source row groups have size 1; output row groups have size 2, compression is NONE, and `color` must have real dictionary encoding. Temporary fixtures are removed after the demo.

## Public Python API

```python
from pathlib import Path
from parquet_union_replay import UnionError, plan_union, rewrite, inspect_receipt

manifest = [Path("fixtures/a.parquet"), Path("fixtures/b.parquet")]
plan = plan_union(manifest)  # bounded bytes + metadata, no row decoding
try:
    receipt = rewrite(manifest, "artifacts/union", row_group_size=2)
except UnionError as error:
    print(error.code)  # constant code only
else:
    assert inspect_receipt("artifacts/union") == receipt
```

Parents must already exist. `rewrite` replans; a plan is information, not an executable snapshot. Existing destinations, even empty directories, are never overwritten. The module functions share one serialized instance; `UnionRewriter()` provides an independent instance. Separate instances must still use private, immutable input workspaces.

## CLI

A manifest is a JSON array of 1–8 explicit local file paths. Relative entries are relative to the manifest file; API paths are relative to the working directory. Repeated entries intentionally repeat rows.

```json
["a.parquet", "b.parquet"]
```

```sh
python -m parquet_union_replay plan fixtures/manifest.json
python -m parquet_union_replay rewrite fixtures/manifest.json artifacts/union --row-group-size 2
python -m parquet_union_replay inspect artifacts/union
```

Successful data commands emit one canonical JSON object. Failures emit only a constant code to stderr: exit 2 before publication, or exit 3 for `E_COMMITTED_IO` when rename succeeded but parent-directory synchronization failed. Exit 3 means the destination **exists**: inspect it, do not assume rollback or retry with overwrite.

## Contract in brief

- 8 input occurrences, 4096 total rows, 16 global source fields, 64 total source row groups; 16 MiB per input.
- Only top-level bool/int32/int64/ordinary UTF-8 string and integer-indexed UTF-8 dictionaries. Only int32→int64 promotion. Unsupported declarations fail even with zero rows.
- Exact UTF-8-byte field ordering; typed missing nulls; no sorting or deduplication of records. Provenance uses exact signed int64, starting at zero per manifest occurrence.
- Dictionary output is used if any declaration of that string field is dictionary-backed. Its observed non-null domain is UTF-8-byte sorted. No Unicode normalization.
- Duplicate names, ASCII-case ambiguities, reserved provenance names, control names and names over 128 UTF-8 bytes are rejected. Unicode case folding is not used.
- Local regular files only, no symlinks in any component, URLs, globs, backslashes, colons, control characters or `..` components. See the [precise contract](docs/contract.md), including additional output/receipt limits and error codes.

## Why this design, and limitations

Explicit policy avoids permissive schema unification. A bounded in-memory row oracle is practical for this fixture size. DuckDB checks exact rows, order, SQL types, null counts and provenance aggregates; additional fixed SQL checks sum of numeric `id` and grouping of string `color` when those exact fields exist. Arrow separately inspects schema, dictionaries, row groups and compression. Tests have literal independent row and aggregate goldens, including int64 extremes.

Receipts contain input hashes without paths, logical dictionaries, schema, semantic counts, output hash and actual dependency versions. `inspect_receipt` rechecks file hash, Arrow structure, DuckDB semantics and provenance shape, but cannot authenticate the supplied receipt or original sources. It does not establish that a claimed source hash belongs to someone else’s file.

Counters and byte admissions are **not** hard disk, decompression or RSS bounds. Native parsers can allocate before metadata is accepted. Use the external 1 GiB / 2 CPU controls; do not feed adversarial third-party datasets. Output is admitted at 128 MiB after writing and the receipt at 4 MiB. No spill is enabled in DuckDB. There is no promise of physical dictionary-page ordering, byte-identical Parquet across versions, universal compatibility, or crash recovery on every filesystem. Publication requires Linux `renameat2(RENAME_NOREPLACE)` and same-filesystem directory semantics.

See [design](docs/design.md), [contract and failures / contrato e falhas](docs/contract.md), [Português](README.pt-BR.md), and [security reporting](SECURITY.md). MIT, copyright 2026 Lofrano Tecnologia. General contact: [official homepage](https://lofranotecnologia.com.br).
