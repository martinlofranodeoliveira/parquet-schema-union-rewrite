"""Bounded schema policy and transactional public API."""

import functools
import json
import os
import platform
import re
import sys
import threading
import unicodedata

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .errors import UnionError
from . import oracle
from .storage import (MARKER, MAX_INPUT_BYTES, MAX_RECEIPT_BYTES, digest,
                      read_file, staging, write_bytes)

VERSION = '0.1.0'
RESERVED = ('__source_file', '__source_row')
LOGICAL = {'bool': pa.bool_(), 'int32': pa.int32(), 'int64': pa.int64(),
           'string': pa.string()}
ASCII_FOLD = str.maketrans('ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')


def canonical(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True,
                       separators=(',', ':'), allow_nan=False) + '\n').encode('utf-8')


def versions():
    return {'python': platform.python_version(), 'pyarrow': pa.__version__,
            'duckdb': duckdb.__version__, 'parquet_union_replay': VERSION}


def _runtime():
    if (sys.implementation.name != 'cpython' or sys.version_info[:2] != (3, 13)
            or sys.platform != 'linux' or platform.machine() not in ('x86_64', 'amd64')
            or pa.__version__ != '26.0.1' or duckdb.__version__ != '2.0.0'):
        raise UnionError('E_RUNTIME')


def _entry(function):
    @functools.wraps(function)
    def call(self, *args, **kwargs):
        try:
            with self._lock:
                _runtime()
                return function(self, *args, **kwargs)
        except UnionError:
            raise
        except PermissionError:
            raise UnionError('E_PERMISSION') from None
        except MemoryError:
            raise UnionError('E_RESOURCE') from None
        except OSError:
            raise UnionError('E_IO') from None
        except Exception:
            raise UnionError('E_INTERNAL') from None
    return call


def _name(name):
    if (not isinstance(name, str) or not name or len(name.encode('utf-8')) > 128
            or any(unicodedata.category(c) == 'Cc' for c in name)):
        raise UnionError('E_NAME')
    folded = name.translate(ASCII_FOLD)
    if folded in RESERVED:
        raise UnionError('E_RESERVED')
    return folded


def _type(data_type):
    if pa.types.is_dictionary(data_type):
        if data_type.value_type == pa.string() and pa.types.is_integer(data_type.index_type):
            return 'string', True
        raise UnionError('E_SCHEMA')
    for name, arrow_type in LOGICAL.items():
        if data_type == arrow_type:
            return name, False
    raise UnionError('E_SCHEMA')


def arrow_schema(fields):
    result = []
    for field in fields:
        data_type = LOGICAL[field['type']]
        if field['dictionary']:
            data_type = pa.dictionary(pa.int32(), pa.string())
        result.append(pa.field(field['name'], data_type, nullable=field['nullable']))
    return pa.schema(result)


def _metadata(payload):
    try:
        with pa.BufferReader(payload) as buffer:
            parquet = pq.ParquetFile(buffer)
            try:
                schema = parquet.schema_arrow
                rows = parquet.metadata.num_rows
                groups = parquet.metadata.num_row_groups
                if rows > 4096:
                    raise UnionError('E_LIMIT_ROWS')
                if groups > 64:
                    raise UnionError('E_LIMIT_GROUPS')
                if rows < 0 or groups < 0:
                    raise UnionError('E_PARQUET_READ')
                sizes = [parquet.metadata.row_group(i).num_rows for i in range(groups)]
                if sum(sizes) != rows or any(n < 0 for n in sizes):
                    raise UnionError('E_PARQUET_READ')
                return schema, rows, sizes
            finally:
                parquet.close()
    except (pa.ArrowException, OSError):
        raise UnionError('E_PARQUET_READ') from None


def _prepare(manifest):
    if not isinstance(manifest, (list, tuple)) or not manifest:
        raise UnionError('E_ARGUMENT')
    if len(manifest) > 8:
        raise UnionError('E_LIMIT_FILES')
    sources, union, folded_names = [], {}, {}
    total_rows = total_groups = 0
    for index, item in enumerate(manifest):
        path, payload = read_file(item)
        schema, rows, sizes = _metadata(payload)
        if len(schema) > 16:
            raise UnionError('E_LIMIT_COLUMNS')
        total_rows += rows
        total_groups += len(sizes)
        if total_rows > 4096:
            raise UnionError('E_LIMIT_ROWS')
        if total_groups > 64:
            raise UnionError('E_LIMIT_GROUPS')
        local_names = set()
        for field in schema:
            folded = _name(field.name)
            if field.name in local_names:
                raise UnionError('E_NAME_COLLISION')
            local_names.add(field.name)
            if folded in folded_names and folded_names[folded] != field.name:
                raise UnionError('E_NAME_COLLISION')
            folded_names[folded] = field.name
            logical, dictionary = _type(field.type)
            if field.name in union:
                previous, was_dictionary = union[field.name]
                if previous != logical:
                    if {previous, logical} == {'int32', 'int64'}:
                        logical = 'int64'
                    else:
                        raise UnionError('E_SCHEMA')
                dictionary = dictionary or was_dictionary
            union[field.name] = logical, dictionary
        if len(union) > 16:
            raise UnionError('E_LIMIT_COLUMNS')
        sources.append({'path': path, 'payload': payload, 'sizes': sizes,
                        'record': {'index': index, 'sha256': digest(payload),
                                   'bytes': len(payload), 'rows': rows,
                                   'row_groups': len(sizes)}})
    fields = [{'name': name, 'type': union[name][0], 'dictionary': union[name][1],
               'nullable': True}
              for name in sorted(union, key=lambda name: name.encode('utf-8'))]
    fields.extend({'name': name, 'type': 'int64', 'dictionary': False,
                   'nullable': False} for name in RESERVED)
    plan = {'schema': fields, 'dictionary_columns': [f['name'] for f in fields
                                                    if f['dictionary']],
            'sources': [s['record'] for s in sources], 'rows': total_rows,
            'row_groups': total_groups}
    return plan, sources


def _materialize(plan, sources):
    fields = plan['schema'][:-2]
    rows = []
    for source in sources:
        source_row = 0
        try:
            with pa.BufferReader(source['payload']) as buffer:
                parquet = pq.ParquetFile(buffer)
                try:
                    for group_index, size in enumerate(source['sizes']):
                        table = parquet.read_row_group(group_index, use_threads=False)
                        if table.num_rows != size:
                            raise UnionError('E_PARQUET_READ')
                        columns = []
                        for field in fields:
                            if field['name'] not in table.column_names:
                                columns.append([None] * size)
                            else:
                                try:
                                    column = pc.cast(table.column(field['name']),
                                                     LOGICAL[field['type']], safe=True)
                                    columns.append(column.to_pylist())
                                except pa.ArrowException:
                                    raise UnionError('E_CAST') from None
                        for offset in range(size):
                            rows.append(tuple(column[offset] for column in columns) +
                                        (source['record']['index'], source_row))
                            source_row += 1
                finally:
                    parquet.close()
        except (pa.ArrowException, OSError, UnicodeError):
            raise UnionError('E_PARQUET_READ') from None
        if source_row != source['record']['rows']:
            raise UnionError('E_PARQUET_READ')
    if len(rows) != plan['rows']:
        raise UnionError('E_PARQUET_READ')
    dictionaries, arrays = {}, []
    try:
        for index, field in enumerate(plan['schema']):
            values = [row[index] for row in rows]
            if field['dictionary']:
                domain = sorted({v for v in values if v is not None},
                                key=lambda v: v.encode('utf-8'))
                dictionaries[field['name']] = domain
                positions = {value: i for i, value in enumerate(domain)}
                indices = [None if value is None else positions[value] for value in values]
                arrays.append(pa.DictionaryArray.from_arrays(
                    pa.array(indices, type=pa.int32(), safe=True),
                    pa.array(domain, type=pa.string(), safe=True)))
            else:
                arrays.append(pa.array(values, type=LOGICAL[field['type']], safe=True))
        return pa.Table.from_arrays(arrays, schema=arrow_schema(plan['schema'])), rows, dictionaries
    except pa.ArrowException:
        raise UnionError('E_CAST') from None


def _write_table(table, path, row_group_size, dictionary_columns):
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            pq.write_table(table, stream, row_group_size=row_group_size,
                           compression='NONE', use_dictionary=dictionary_columns,
                           store_schema=True)
            stream.flush()
            os.fsync(stream.fileno())
    except (pa.ArrowException, OSError):
        raise UnionError('E_WRITE') from None


def _integer(value, low, high):
    return type(value) is int and low <= value <= high


def _need(condition):
    if not condition:
        raise UnionError('E_RECEIPT')


def _receipt_shape(receipt):
    """Validate untrusted receipt structure before invoking native readers."""
    _need(isinstance(receipt, dict))
    _need(set(receipt) == {'format', 'development_version', 'plan', 'dictionary_values',
                            'verification', 'artifact', 'versions'})
    _need(receipt['format'] == 'parquet-union-replay/1')
    _need(receipt['development_version'] == VERSION)
    plan = receipt['plan']
    _need(isinstance(plan, dict))
    _need(set(plan) == {'schema', 'dictionary_columns', 'sources', 'rows', 'row_groups'})
    _need(_integer(plan['rows'], 0, 4096) and _integer(plan['row_groups'], 0, 64))
    fields = plan['schema']
    _need(isinstance(fields, list) and 2 <= len(fields) <= 18)
    names, folded = [], set()
    for field in fields[:-2]:
        _need(isinstance(field, dict))
        _need(set(field) == {'name', 'type', 'dictionary', 'nullable'})
        name = field['name']
        key = _name(name)
        _need(key not in folded)
        folded.add(key)
        names.append(name)
        _need(field['type'] in LOGICAL and type(field['dictionary']) is bool)
        _need(field['nullable'] is True)
        _need(not field['dictionary'] or field['type'] == 'string')
    _need(names == sorted(names, key=lambda name: name.encode('utf-8')))
    _need(fields[-2:] == [{'name': name, 'type': 'int64', 'dictionary': False,
                           'nullable': False} for name in RESERVED])
    _need(plan['dictionary_columns'] == [f['name'] for f in fields if f['dictionary']])
    dictionaries = receipt['dictionary_values']
    _need(isinstance(dictionaries, dict))
    _need(set(dictionaries) == set(plan['dictionary_columns']))
    for values in dictionaries.values():
        _need(isinstance(values, list) and all(isinstance(v, str) for v in values))
        _need(values == sorted(set(values), key=lambda v: v.encode('utf-8')))
    sources = plan['sources']
    _need(isinstance(sources, list) and 1 <= len(sources) <= 8)
    for index, source in enumerate(sources):
        _need(isinstance(source, dict))
        _need(set(source) == {'index', 'sha256', 'bytes', 'rows', 'row_groups'})
        _need(type(source['index']) is int and source['index'] == index)
        _need(isinstance(source['sha256'], str) and
              re.fullmatch('[0-9a-f]{64}', source['sha256']) is not None)
        _need(_integer(source['bytes'], 1, MAX_INPUT_BYTES))
        _need(_integer(source['rows'], 0, 4096) and _integer(source['row_groups'], 0, 64))
    _need(sum(s['rows'] for s in sources) == plan['rows'])
    _need(sum(s['row_groups'] for s in sources) == plan['row_groups'])
    artifact = receipt['artifact']
    _need(isinstance(artifact, dict))
    _need(set(artifact) == {'name', 'bytes', 'sha256', 'row_groups',
                           'row_group_size', 'compression'})
    _need(artifact['name'] == 'data.parquet' and artifact['compression'] == 'NONE')
    _need(_integer(artifact['row_group_size'], 1, 4096))
    _need(_integer(artifact['row_groups'], 1, 4096))
    _need(type(artifact['bytes']) is int and artifact['bytes'] > 0)
    _need(isinstance(artifact['sha256'], str) and
          re.fullmatch('[0-9a-f]{64}', artifact['sha256']) is not None)
    _need(isinstance(receipt['versions'], dict))
    _need(set(receipt['versions']) == {'python', 'pyarrow', 'duckdb', 'parquet_union_replay'})
    _need(all(isinstance(v, str) and 1 <= len(v) <= 64
              for v in receipt['versions'].values()))
    _need(receipt['versions']['parquet_union_replay'] == VERSION)
    _need(isinstance(receipt['verification'], dict))


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise UnionError('E_RECEIPT')
        result[key] = value
    return result


class UnionRewriter:
    """One job at a time per instance. Methods return JSON-compatible dictionaries."""

    def __init__(self):
        self._lock = threading.Lock()

    @_entry
    def plan_union(self, manifest):
        """Read bounded source bytes and metadata; do not decode row data or write."""
        return _prepare(manifest)[0]

    @_entry
    def rewrite(self, manifest, destination, *, row_group_size=2):
        """Replan, normalize, verify, and atomically publish a new directory."""
        if not _integer(row_group_size, 1, 4096):
            raise UnionError('E_ARGUMENT')
        plan, sources = _prepare(manifest)
        table, expected, dictionaries = _materialize(plan, sources)
        with staging(destination) as stage:
            candidate = stage.path / 'data.parquet'
            _write_table(table, candidate, row_group_size, plan['dictionary_columns'])
            metrics, facts, _ = oracle.verify(candidate, table.schema, dictionaries,
                                               row_group_size, expected)
            receipt = {'format': 'parquet-union-replay/1', 'development_version': VERSION,
                       'plan': plan, 'dictionary_values': dictionaries,
                       'verification': metrics,
                       'artifact': {'name': 'data.parquet', **facts,
                                    'row_group_size': row_group_size, 'compression': 'NONE'},
                       'versions': versions()}
            encoded = canonical(receipt)
            if len(encoded) > MAX_RECEIPT_BYTES:
                raise UnionError('E_LIMIT_BYTES')
            write_bytes(stage.path / 'receipt.json', encoded)
            # Reopen non-following handles; never trust cached path metadata.
            for source in sources:
                try:
                    _, current = read_file(source['path'])
                except UnionError:
                    raise UnionError('E_SOURCE_CHANGED') from None
                if digest(current) != source['record']['sha256']:
                    raise UnionError('E_SOURCE_CHANGED')
            stage.publish()
            return receipt

    @_entry
    def inspect_receipt(self, artifact):
        """Recheck the artifact and receipt without access to original sources.

        This is an integrity/consistency check, not authentication of provenance.
        """
        from .storage import local_path, MAX_ARTIFACT_BYTES
        directory = local_path(artifact)
        _, marker = read_file(directory / '.union-owned', len(MARKER))
        if marker != MARKER:
            raise UnionError('E_RECEIPT')
        _, encoded = read_file(directory / 'receipt.json', MAX_RECEIPT_BYTES)
        try:
            receipt = json.loads(encoded, object_pairs_hook=_unique_object)
            _receipt_shape(receipt)
            _need(canonical(receipt) == encoded)
        except (UnionError, ValueError, TypeError, KeyError, UnicodeError, RecursionError):
            raise UnionError('E_RECEIPT') from None
        _, payload = read_file(directory / 'data.parquet', MAX_ARTIFACT_BYTES)
        _need(digest(payload) == receipt['artifact']['sha256'])
        _need(len(payload) == receipt['artifact']['bytes'])
        schema = arrow_schema(receipt['plan']['schema'])
        metrics, facts, rows = oracle.verify(
            directory / 'data.parquet', schema, receipt['dictionary_values'],
            receipt['artifact']['row_group_size'])
        _need(metrics == receipt['verification'])
        _need(len(rows) == receipt['plan']['rows'])
        for key, value in facts.items():
            _need(receipt['artifact'][key] == value)
        provenance = [(source['index'], row) for source in receipt['plan']['sources']
                      for row in range(source['rows'])]
        _need([row[-2:] for row in rows] == provenance)
        return receipt


_default = UnionRewriter()
plan_union = _default.plan_union
rewrite = _default.rewrite
inspect_receipt = _default.inspect_receipt
