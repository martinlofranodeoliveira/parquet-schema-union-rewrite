# Design — development 0.1.0

## Scope

This project deliberately handles a small semantic rewrite, not a general schema evolution framework. Explicit type dispatch is easier to audit than a permissive type unifier. It rejects unsupported declarations before materializing any rows, including declarations in zero-row files. A global field-name policy prevents DuckDB's ASCII-case renaming hazard from silently changing the output contract.

Original authoring is split into local storage/publication, schema and job policy, independent SQL verification, synthetic fixtures and CLI presentation. No Parquet parser is implemented here. PyArrow's maintained ParquetFile, BufferReader, DictionaryArray, safe compute casts and write_table perform native operations.

## Data flow

1. Validate manifest shape and each local path. Walk real directory components using directory descriptors and O_NOFOLLOW. Stat each final regular file before a nonblocking open. Admit at most 16 MiB and snapshot bounded bytes with a SHA-256 digest.
2. Parse Arrow metadata from those bytes. Accumulate total rows, physical row groups and the global union. Enforce limits before row-group reads. Preserve supported declarations and discard source metadata.
3. Read each physical row group in manifest order with Arrow threading disabled on reads. Decode dictionary values to the admitted logical type with safe casts. Construct an ordered tuple oracle containing all rows and exact int64 provenance.
4. Build arrays from that oracle. Dictionary domains come only from observed strings and are UTF-8-byte sorted. This intentionally removes unused entries and merges plain/dictionary sources without changing values.
5. Create an owner-only marked sibling stage. Write uncompressed Parquet through an exclusive local file handle, flush and fsync it. The stage name is random solely for namespace isolation; it never enters semantic output or the receipt.
6. Inspect the actual written Arrow schema, dictionary domains, row-group sizes, compression and dictionary encodings. Verify every row and its order with DuckDB SQL. Schema/row/null/aggregate disagreements fail closed.
7. Write canonical semantic receipt, flush/fsync, reopen each source and recheck its digest. Close native readers/connections. Fsync the stage and publish by Linux no-replace directory rename. Fsync its parent. Distinguish post-commit sync failure from rollback.

The fixed ownership marker remains in the artifact. Cleanup examines only marked stages and known regular files; it never traverses an arbitrary tree. No crash-recovery scanner is included.

## Independence of verification

Arrow is both input interpreter and writer, so Arrow-only roundtripping would not be sufficient. DuckDB reads the written file independently and compares all exact row tuples, SQL names/types, all-column non-null counts, total rows and provenance sums. Additional fixed queries cover numeric `id` sums and string `color` groups. All SQL text is authored in the package and paths are parameters; no user SQL, field-name interpolation, external table, INSTALL or LOAD exists.

The DuckDB connection first sets allowed_paths to the exact candidate, then disables external access. Passing both settings at connect time is intentionally avoided because of the qualified DuckDB 2 ordering behavior. Extension installation, autoloading and unsigned extensions are disabled at connection creation. There is one DuckDB thread, a 256 MB memory limit and no spill directory. These settings restrict this verifier; they are not a hostile-code sandbox.

A common implementation mistake could affect a source-derived oracle and writer together. Regression tests therefore supply separate literal expected tuples, schemas, dictionary domains, provenance and SQL aggregates. The acceptance fixtures include an unused dictionary value, differing dictionary index widths, promoted IDs, missing fields and nulls. Tests also exercise reversed/repeated manifest blocks, empty declarations, int64 extrema, Unicode distinctions, global limits, real malformed footers, special files, real permissions and transaction races. Injected write/oracle/fsync faults cover otherwise difficult failure windows, not claims about actual hardware failure rates.

## Resource model

Input snapshots are at most 8 × 16 MiB, but Python/native overhead is additional. Row/field/group counters do not constrain decompression ratios, Arrow metadata allocations, native internal buffers or total RSS. Even a metadata parser may allocate before counters can be read. The 128 MiB output check is after writing and is not a disk quota; the receipt check is after constructing its JSON representation. Python MemoryError can be sanitized; an OOM kill cannot.

Run only owned synthetic inputs in the documented non-root, network-none, cap-drop-ALL, NNP, pids256, 1 GiB, 2 CPU container. The CI tmpfs is a further disk boundary, not a replacement for native parser caution. No performance or general adversarial-dataset safety claim follows from these counters.

## Determinism and identity

For fixed source bytes, manifest order, Python/dependency versions and group size, semantic rows, union ordering, dictionary domains and canonical receipts have no clock inputs. The receipt records actual file hashes and versions. Physical dictionary-page order and byte-for-byte output across library versions are expressly excluded. No numerical aggregate passes through floating point; DuckDB sums use HUGEINT and Python uses exact integers.

Inspection rechecks a self-contained artifact, not its historical truth. The receipt is unsigned and does not prove original-source ownership. If both data and receipt are maliciously replaced with another consistent pair, inspection can accept that pair. Input immutability and a private workspace are part of the calling contract.

## Publication limits

Linux x64 is explicit rather than hiding incompatible rename behavior behind a fallback. Unsupported rename flags or filesystems fail before publication. An existing destination wins even if created between verification and rename. Same-filesystem rename gives namespace atomicity, not universal power-loss recovery. A parent fsync failure after rename has a distinct committed error. Cross-process access to source workspaces, symlink-tree replacement and filesystem administrators are outside the supported model.

Future changes to limits, schema policy, naming or receipt structure require a documented contract revision and tests; this development version does not silently widen its accepted subset.
