"""Arrow layout inspection plus independent, restricted DuckDB SQL."""

from collections import Counter
from contextlib import contextmanager

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .errors import UnionError
from .storage import MAX_ARTIFACT_BYTES, digest, read_file


@contextmanager
def connection(candidate):
    con = None
    try:
        con = duckdb.connect(':memory:', config={
            'threads': 1,
            'memory_limit': '256MB',
            'temp_directory': '',
            'preserve_insertion_order': True,
            'autoinstall_known_extensions': False,
            'autoload_known_extensions': False,
            'allow_unsigned_extensions': False,
        })
        # Ordering matters in DuckDB 2: install the exception before disabling access.
        con.execute('SET allowed_paths = ?', [[str(candidate)]])
        con.execute('SET enable_external_access = false')
        yield con
    except duckdb.Error:
        raise UnionError('E_ORACLE') from None
    finally:
        if con is not None:
            try:
                con.close()
            except duckdb.Error:
                raise UnionError('E_ORACLE') from None


def _require(condition):
    if not condition:
        raise UnionError('E_ORACLE')


def verify(candidate, schema, dictionaries, row_group_size, expected_rows=None):
    """Return semantic metrics, file facts, and exact Arrow row tuples.

    expected_rows is supplied by rewrite, independently of the written file.
    Inspection without sources uses Arrow rows but still cross-checks DuckDB.
    """
    candidate, payload = read_file(candidate, MAX_ARTIFACT_BYTES)
    try:
        with pa.BufferReader(payload) as buffer:
            parquet = pq.ParquetFile(buffer)
            try:
                _require(parquet.schema_arrow.equals(schema, check_metadata=False))
                count = parquet.metadata.num_rows
                groups = parquet.metadata.num_row_groups
                _require(0 <= count <= 4096)
                _require(groups == max(1, (count + row_group_size - 1) // row_group_size))
                encoded = set()
                for group_index in range(groups):
                    group = parquet.metadata.row_group(group_index)
                    _require(group.num_rows <= row_group_size)
                    for column_index in range(group.num_columns):
                        column = group.column(column_index)
                        _require(column.compression == 'UNCOMPRESSED')
                        if 'RLE_DICTIONARY' in column.encodings:
                            encoded.add(schema.names[column_index])
                table = parquet.read(use_threads=False)
            finally:
                parquet.close()
        _require(table.num_rows == count)
        for name, values in dictionaries.items():
            chunks = table.column(name).chunks
            for chunk in chunks:
                _require(pa.types.is_dictionary(chunk.type))
                _require(chunk.dictionary.to_pylist() == values)
            observed = sorted({v for v in table.column(name).to_pylist() if v is not None},
                              key=lambda v: v.encode('utf-8'))
            _require(observed == values)
            if values:
                _require(name in encoded)
        columns = [column.to_pylist() for column in table.columns]
        rows = list(zip(*columns))
        _require(len(rows) == count)
        if expected_rows is not None:
            _require(rows == expected_rows)
    except pa.ArrowException:
        raise UnionError('E_ORACLE') from None

    names = schema.names
    sql_types = {'bool': 'BOOLEAN', 'int32': 'INTEGER', 'int64': 'BIGINT',
                 'string': 'VARCHAR'}
    types = []
    for field in schema:
        logical = 'string' if pa.types.is_dictionary(field.type) else str(field.type)
        types.append(sql_types[logical])
    nonnull = {name: sum(row[i] is not None for row in rows)
               for i, name in enumerate(names)}
    provenance_sums = {
        name: (sum(row[names.index(name)] for row in rows) if rows else None)
        for name in ('__source_file', '__source_row')
    }
    metrics = {'rows': count, 'nonnull': nonnull, 'provenance_sums': provenance_sums}
    with connection(candidate) as con:
        cursor = con.execute('SELECT * FROM read_parquet(?)', [str(candidate)])
        _require([item[0] for item in cursor.description] == names)
        _require([str(item[1]) for item in cursor.description] == types)
        _require(cursor.fetchmany(4097) == rows)
        actual_counts = con.execute(
            'SELECT count(COLUMNS(*)) FROM read_parquet(?)', [str(candidate)]
        ).fetchone()
        _require(list(actual_counts) == list(nonnull.values()))
        aggregate = con.execute(
            'SELECT count(*), sum(__source_file)::HUGEINT, '
            'sum(__source_row)::HUGEINT FROM read_parquet(?)', [str(candidate)]
        ).fetchone()
        _require(aggregate == (count, provenance_sums['__source_file'],
                               provenance_sums['__source_row']))
        if 'id' in names and types[names.index('id')] in ('INTEGER', 'BIGINT'):
            values = [row[names.index('id')] for row in rows
                      if row[names.index('id')] is not None]
            wanted = sum(values) if values else None
            actual = con.execute('SELECT sum(id)::HUGEINT FROM read_parquet(?)',
                                 [str(candidate)]).fetchone()[0]
            _require(actual == wanted)
            metrics['id_sum'] = actual
        if 'color' in names and types[names.index('color')] == 'VARCHAR':
            counts = Counter(row[names.index('color')] for row in rows)
            wanted = sorted(counts.items(), key=lambda item: (
                item[0] is None, b'' if item[0] is None else item[0].encode('utf-8')))
            actual = con.execute(
                'SELECT color, count(*) FROM read_parquet(?) '
                'GROUP BY color ORDER BY color NULLS LAST', [str(candidate)]
            ).fetchall()
            _require(actual == wanted)
            metrics['color_groups'] = [{'value': value, 'rows': amount}
                                       for value, amount in actual]
    return metrics, {'bytes': len(payload), 'sha256': digest(payload),
                     'row_groups': groups}, rows
