import hashlib
import json
from pathlib import Path
import subprocess
import sys

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parquet_union_replay import UnionError, inspect_receipt, plan_union, rewrite
from parquet_union_replay.demo import create_sources
from parquet_union_replay.oracle import connection


GOLDEN = [('red', 1, None, 0, 0), (None, 2, None, 0, 1),
          ('green', 3, 'x', 1, 0), ('red', 4, None, 1, 1)]


def write(path, columns, group=2):
    pq.write_table(pa.table(columns), path, row_group_size=group,
                   compression='NONE', store_schema=True)
    return path


def rows(path):
    table = pq.read_table(path, use_threads=False)
    return list(zip(*(column.to_pylist() for column in table.columns)))


def test_literal_oracles_and_receipt(tmp_path):
    sources = create_sources(tmp_path)
    originals = [p.read_bytes() for p in sources]
    plan = plan_union(sources)
    assert not (tmp_path / 'out').exists()
    assert plan['rows'] == 4 and plan['row_groups'] == 4
    receipt = rewrite(sources, tmp_path / 'out')
    candidate = tmp_path / 'out' / 'data.parquet'
    assert rows(candidate) == GOLDEN
    assert [p.read_bytes() for p in sources] == originals
    assert receipt == inspect_receipt(tmp_path / 'out')
    assert receipt['dictionary_values'] == {'color': ['green', 'red']}
    assert receipt['artifact']['sha256'] == hashlib.sha256(candidate.read_bytes()).hexdigest()
    assert str(tmp_path) not in json.dumps(receipt)
    assert receipt['versions']['pyarrow'] == '26.0.1'
    assert receipt['versions']['duckdb'] == '2.0.0'
    with pq.ParquetFile(candidate) as parquet:
        assert parquet.schema_arrow.names == ['color', 'id', 'note',
                                              '__source_file', '__source_row']
        assert parquet.schema_arrow.field('id').type == pa.int64()
        for name in ['__source_file', '__source_row']:
            assert parquet.schema_arrow.field(name).type == pa.int64()
            assert not parquet.schema_arrow.field(name).nullable
        assert parquet.metadata.num_row_groups == 2
        for index in range(2):
            group = parquet.metadata.row_group(index)
            assert group.num_rows == 2
            assert 'RLE_DICTIONARY' in group.column(0).encodings
            assert all(group.column(i).compression == 'UNCOMPRESSED'
                       for i in range(group.num_columns))
        for chunk in parquet.read().column('color').chunks:
            assert chunk.dictionary.to_pylist() == ['green', 'red']
    # Separate literal SQL goldens, not metrics calculated by production code.
    with connection(candidate) as con:
        assert con.execute('SELECT * FROM read_parquet(?)', [str(candidate)]).fetchall() == GOLDEN
        assert con.execute('SELECT count(*), sum(id), count(note) FROM read_parquet(?)',
                           [str(candidate)]).fetchone() == (4, 10, 1)
        assert con.execute('SELECT color, count(*) FROM read_parquet(?) GROUP BY color '
                           'ORDER BY color NULLS LAST', [str(candidate)]).fetchall() == [
                               ('green', 1), ('red', 2), (None, 1)]


def test_reverse_repeated_files_and_row_groups(tmp_path):
    a, b = create_sources(tmp_path)
    rewrite([b, a, b], tmp_path / 'out', row_group_size=1)
    assert rows(tmp_path / 'out' / 'data.parquet') == [
        ('green', 3, 'x', 0, 0), ('red', 4, None, 0, 1),
        ('red', 1, None, 1, 0), (None, 2, None, 1, 1),
        ('green', 3, 'x', 2, 0), ('red', 4, None, 2, 1)]


def test_empty_supported_field_and_empty_job_rows(tmp_path):
    a, b = create_sources(tmp_path)
    empty = write(tmp_path / 'empty.parquet', {'active': pa.array([], pa.bool_())})
    plan = plan_union([empty, a, b])
    assert plan['sources'][0]['row_groups'] == 1
    rewrite([empty, a, b], tmp_path / 'out')
    result = pq.read_table(tmp_path / 'out' / 'data.parquet')
    assert result.schema.names[:4] == ['active', 'color', 'id', 'note']
    assert result.column('active').to_pylist() == [None] * 4
    assert result.column('__source_file').to_pylist() == [1, 1, 2, 2]
    receipt = rewrite([empty], tmp_path / 'empty-out')
    assert receipt['verification']['rows'] == 0
    assert receipt['verification']['provenance_sums']['__source_row'] is None
    assert inspect_receipt(tmp_path / 'empty-out') == receipt


def test_mixed_strings_unicode_and_exact_int64(tmp_path):
    a = write(tmp_path / 'a.parquet', {
        'id': pa.array([-(2**63), 2**63 - 1], pa.int64()),
        'color': pa.array(['é', 'e\u0301'], pa.string()),
        'Á': pa.array([True, None], pa.bool_()),
        'á': pa.array([False, True], pa.bool_()),
    })
    b = write(tmp_path / 'b.parquet', {
        'color': pa.DictionaryArray.from_arrays(pa.array([0, None], pa.int32()),
                                                pa.array(['é', 'unused']))})
    receipt = rewrite([a, b], tmp_path / 'out')
    assert receipt['dictionary_values']['color'] == ['e\u0301', 'é']
    assert receipt['verification']['id_sum'] == -1
    table = pq.read_table(tmp_path / 'out' / 'data.parquet')
    assert table.column('id').to_pylist() == [-(2**63), 2**63 - 1, None, None]
    assert table.column('color').to_pylist() == ['é', 'e\u0301', 'é', None]
    assert table.schema.names == ['color', 'id', 'Á', 'á', '__source_file', '__source_row']


def test_dictionary_all_null_has_empty_domain(tmp_path):
    path = write(tmp_path / 'a.parquet', {'color': pa.DictionaryArray.from_arrays(
        pa.array([None, None], pa.int32()), pa.array(['unobserved']))})
    receipt = rewrite([path], tmp_path / 'out')
    assert receipt['dictionary_values'] == {'color': []}
    assert inspect_receipt(tmp_path / 'out') == receipt


@pytest.mark.parametrize('data_type', [pa.float32(), pa.float64(), pa.uint32(),
    pa.timestamp('ns'), pa.list_(pa.int32()), pa.struct([('x', pa.int32())]),
    pa.large_string(), pa.binary(), pa.null()])
def test_unsupported_even_empty(tmp_path, data_type):
    path = write(tmp_path / 'bad.parquet', {'bad': pa.array([], type=data_type)})
    with pytest.raises(UnionError, match='^E_SCHEMA$'):
        rewrite([path], tmp_path / 'out')
    assert not (tmp_path / 'out').exists()


def test_string_integer_conflict(tmp_path):
    a = write(tmp_path / 'a.parquet', {'x': pa.array([1], pa.int32())})
    b = write(tmp_path / 'b.parquet', {'x': pa.array(['1'], pa.string())})
    with pytest.raises(UnionError, match='^E_SCHEMA$'):
        rewrite([a, b], tmp_path / 'out')
    assert not (tmp_path / 'out').exists()


@pytest.mark.parametrize('names,code', [
    (['A', 'a'], 'E_NAME_COLLISION'), (['x', 'x'], 'E_NAME_COLLISION'),
    (['__source_file'], 'E_RESERVED'), (['__SOURCE_ROW'], 'E_RESERVED'),
    (['line\nbreak'], 'E_NAME'), (['x\x00y'], 'E_NAME'),
    (['é' * 65], 'E_NAME'), ([''], 'E_NAME')])
def test_name_policy(tmp_path, names, code):
    table = pa.Table.from_arrays([pa.array([1], pa.int32()) for _ in names], names=names)
    path = tmp_path / 'bad.parquet'
    pq.write_table(table, path)
    with pytest.raises(UnionError, match='^' + code + '$'):
        rewrite([path], tmp_path / 'out')
    assert not (tmp_path / 'out').exists()


def test_cross_file_ascii_collision(tmp_path):
    a = write(tmp_path / 'a.parquet', {'A': pa.array([], pa.int32())})
    b = write(tmp_path / 'b.parquet', {'a': pa.array([], pa.int32())})
    with pytest.raises(UnionError, match='^E_NAME_COLLISION$'):
        plan_union([a, b])


def test_duckdb_restricted_settings_and_unlisted_file(tmp_path):
    a, b = create_sources(tmp_path)
    with connection(a) as con:
        actual = con.execute("SELECT current_setting('autoinstall_known_extensions'), "
                             "current_setting('autoload_known_extensions'), "
                             "current_setting('enable_external_access'), "
                             "current_setting('temp_directory')").fetchone()
        assert actual == (False, False, False, '')
        assert con.execute('SELECT count(*) FROM read_parquet(?)', [str(a)]).fetchone() == (2,)
        with pytest.raises(duckdb.PermissionException):
            con.execute('SELECT * FROM read_parquet(?)', [str(b)]).fetchall()


def test_cli_and_installed_wheel_demo(tmp_path):
    # CI installs the wheel. -I and an outside-checkout cwd exclude source imports.
    process = subprocess.run([sys.executable, '-I', '-m', 'parquet_union_replay',
                              'demo', '--assert', '--json'], cwd=tmp_path,
                             text=True, capture_output=True, timeout=45)
    assert process.returncode == 0, process.stderr
    result = json.loads(process.stdout)
    assert result['rows'] == 4 and result['id_sum'] == 10
    assert result['checked'] is True
    assert result['provenance'] == [[0, 0], [0, 1], [1, 0], [1, 1]]


def test_cli_manifest_and_sanitized_failure(tmp_path):
    create_sources(tmp_path)
    manifest = tmp_path / 'manifest.json'
    manifest.write_text('["a.parquet","b.parquet"]', encoding='utf-8')
    process = subprocess.run([sys.executable, '-m', 'parquet_union_replay',
                              'plan', str(manifest)], text=True, capture_output=True, timeout=30)
    assert process.returncode == 0
    assert json.loads(process.stdout)['rows'] == 4
    private = 'sensitive-value-123'
    process = subprocess.run([sys.executable, '-m', 'parquet_union_replay',
                              'rewrite', str(manifest), str(tmp_path / 'out'),
                              '--row-group-size', private],
                             text=True, capture_output=True, timeout=30)
    assert process.returncode == 2
    assert process.stderr == 'E_ARGUMENT\n' and process.stdout == ''


def test_numeric_dictionary_serializes_as_supported_logical_int32(tmp_path):
    # Parquet's reader exposes this as ordinary int32, not a dictionary type.
    values = pa.DictionaryArray.from_arrays(pa.array([0, 1, None], pa.int32()),
                                            pa.array([7, 9], pa.int32()))
    path = write(tmp_path / 'numeric.parquet', {'number': values})
    assert pq.ParquetFile(path).schema_arrow.field('number').type == pa.int32()
    receipt = rewrite([path], tmp_path / 'out')
    assert receipt['dictionary_values'] == {}
    assert pq.read_table(tmp_path / 'out' / 'data.parquet').column('number').to_pylist() == [7, 9, None]
    assert inspect_receipt(tmp_path / 'out') == receipt
