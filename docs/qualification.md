# Qualification record and dependency preparation

This record describes the native environment, independent fixture observations and dependency preparation used for this bounded development 0.1.0 implementation. The README documents reproducible regression and installed-package checks.

## Qualified environment

The pre-source qualification used CPython 3.13.18 on Linux x64, PyArrow 26.0.1, DuckDB 2.0.0, pytest 9.2.0, setuptools 84.2.1, wheel 0.48.0, packaging 26.3 and the resolved pytest dependencies recorded in `requirements.lock`.

The Dockerfile uses the public base `python:3.13-slim-bookworm` at digest `sha256:281d39ff02e3b389401586134ff9e6572954687822a58055ec07a60ded4e0b7f`. No separately prepared image is required.

All nine dependency hashes in `requirements.lock` identify actual resolved wheel artifacts for this ABI/platform. They are not a cross-platform lock. Exact build backend versions are also declared in pyproject.toml. The source distribution includes code, tests, documentation, configuration and the dependency lock through MANIFEST.in.

## Independent fixture observations

- A real uncompressed PyArrow fixture used A.id=int32[1,2], A.color dictionary indices int8[0,null], dictionary[red,blue]; blue was unused. B used id=int64[3,4], color indices int16[0,1], dictionary[green,red], note[x,null]. Source group size was 1.
- A literal output with normalized dictionary[green,red], int32 indices[1,null,0,1], int64 id and provenance was written with group size 2, no compression, `use_dictionary=['color']`, and stored Arrow schema. There were exactly two row groups, UNCOMPRESSED metadata and real RLE_DICTIONARY color encoding. Read chunks retained the logical domain.
- Independent DuckDB returned literal ordered tuples, count=4, sum(id)=10, count(note)=1, green=1/red=2/null=1. Signed int64 extrema summed exactly to -1.
- A real zero-row bool-declaration file had one row group. A real truncated footer raised ArrowInvalid. UID 1000 with a chmod0500 staging parent encountered PermissionError with no staging child.
- Connecting with both allowed_paths and external-access=false in the configuration failed because the flag was applied first. Connecting with extension safeguards first, then setting allowed_paths, then external-access=false allowed the candidate and denied a second unlisted file with PermissionException. Installation/autoload/external-access settings were confirmed false.
- DuckDB silently renamed fields A/a into A/a_1. This project rejects the ambiguity before writing instead of treating that as interoperability success.

These observations justify API choices and literal fixture expectations. CI and the documented container commands validate the implementation separately. There is no release or deployment automation.

## Explicit preparation

The Docker build performs dependency acquisition with `--only-binary=:all: --require-hashes`, then offline package builds with the pinned installed backend. Runtime container commands have no network and do not install packages. For local offline use, provision the same wheels in a wheelhouse during a separate preparation step and use the README's no-index command. A platform/hash mismatch is a preparation failure, not permission to accept an unrecorded artifact.

## Dependency licenses

License identifiers below describe dependencies, not an assertion that their code is part of this original implementation. Installed distributions carry their own notices and bundled-component notices; preserve those when redistributing wheels/images.

| Distribution | Version | Declared license family |
| --- | --- | --- |
| PyArrow | 26.0.1 | Apache-2.0; consult bundled notices |
| DuckDB | 2.0.0 | MIT; consult bundled notices |
| pytest | 9.2.0 | MIT |
| iniconfig | 2.3.1 | MIT |
| pluggy | 1.7.0 | MIT |
| packaging | 26.3 | Apache-2.0 OR BSD-2-Clause |
| Pygments | 2.21.0 | BSD-2-Clause |
| setuptools | 84.2.1 | MIT |
| wheel | 0.48.0 | MIT |

CPython uses the PSF license and includes its own notices. This project's original code is MIT, copyright 2026 Lofrano Tecnologia.

## Primary API references

- [Arrow ParquetFile](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html)
- [Arrow write_table](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html)
- [Arrow DictionaryArray](https://arrow.apache.org/docs/python/generated/pyarrow.DictionaryArray.html)
- [Arrow compute.cast](https://arrow.apache.org/docs/python/generated/pyarrow.compute.cast.html)
- [DuckDB Parquet overview](https://duckdb.org/docs/stable/data/parquet/overview)
- [DuckDB configuration](https://duckdb.org/docs/stable/configuration/overview)
- [Apache Parquet format](https://parquet.apache.org/docs/file-format/)

Installed APIs and the generated API documentation above support the qualified operations. These references do not turn this bounded subset into a broad standards-compliance claim.
