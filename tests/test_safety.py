import json
import os
from pathlib import Path
import threading
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parquet_union_replay import UnionError, UnionRewriter, inspect_receipt, plan_union, rewrite
from parquet_union_replay import core, oracle, storage
from parquet_union_replay.demo import create_sources


def fail(code, function, *args, **kwargs):
    with pytest.raises(UnionError) as caught:
        function(*args, **kwargs)
    assert caught.value.code == code
    assert str(caught.value) == code
    assert caught.value.args == (code,)


def no_stage(root):
    assert not list(root.glob('.parquet-union-*'))


def test_limit_boundaries_before_materialization(tmp_path, monkeypatch):
    def forbidden(*args):
        pytest.fail('bulk materialization must not run for a rejected plan')
    monkeypatch.setattr(core, '_materialize', forbidden)
    small = tmp_path / 'small.parquet'
    pq.write_table(pa.table({'x': pa.array([], pa.int32())}), small)
    fail('E_LIMIT_FILES', rewrite, [small] * 9, tmp_path / 'out')
    large = tmp_path / 'large.parquet'
    pq.write_table(pa.table({'x': pa.array(range(4097), pa.int32())}), large)
    fail('E_LIMIT_ROWS', rewrite, [large], tmp_path / 'out')
    a = tmp_path / 'a.parquet'
    b = tmp_path / 'b.parquet'
    pq.write_table(pa.table({'x': pa.array(range(2048), pa.int32())}), a)
    pq.write_table(pa.table({'x': pa.array(range(2049), pa.int32())}), b)
    fail('E_LIMIT_ROWS', rewrite, [a, b], tmp_path / 'out')
    wide = tmp_path / 'wide.parquet'
    pq.write_table(pa.table({f'x{i}': pa.array([], pa.bool_()) for i in range(17)}), wide)
    fail('E_LIMIT_COLUMNS', rewrite, [wide], tmp_path / 'out')
    pq.write_table(pa.table({f'x{i}': pa.array([], pa.bool_()) for i in range(9)}), a)
    pq.write_table(pa.table({f'y{i}': pa.array([], pa.bool_()) for i in range(8)}), b)
    fail('E_LIMIT_COLUMNS', rewrite, [a, b], tmp_path / 'out')
    pq.write_table(pa.table({'x': pa.array(range(65), pa.int32())}), large, row_group_size=1)
    fail('E_LIMIT_GROUPS', rewrite, [large], tmp_path / 'out')
    pq.write_table(pa.table({'x': pa.array(range(64), pa.int32())}), large, row_group_size=1)
    assert plan_union([large])['row_groups'] == 64
    fail('E_LIMIT_GROUPS', rewrite, [large, small], tmp_path / 'out')
    assert not (tmp_path / 'out').exists()
    no_stage(tmp_path)


def test_allowed_total_rows_and_global_columns(tmp_path):
    a = tmp_path / 'a.parquet'
    b = tmp_path / 'b.parquet'
    pq.write_table(pa.table({f'a{i}': pa.array([None] * 2048, pa.int32())
                             for i in range(8)}), a)
    pq.write_table(pa.table({f'b{i}': pa.array([None] * 2048, pa.int32())
                             for i in range(8)}), b)
    receipt = rewrite([a, b], tmp_path / 'out', row_group_size=4096)
    assert receipt['verification']['rows'] == 4096
    assert len(receipt['plan']['schema']) == 18
    assert receipt['verification']['provenance_sums']['__source_row'] == 4192256


def test_real_truncated_footer(tmp_path):
    a, _ = create_sources(tmp_path)
    original = a.read_bytes()
    a.write_bytes(original[:-9])
    fail('E_PARQUET_READ', rewrite, [a], tmp_path / 'out')
    assert a.read_bytes() == original[:-9]
    assert not (tmp_path / 'out').exists()
    no_stage(tmp_path)


def test_file_admission_and_nonblocking_special_files(tmp_path):
    large = tmp_path / 'large.parquet'
    with large.open('wb') as stream:
        stream.truncate(16 * 1024 * 1024 + 1)
    fail('E_LIMIT_BYTES', plan_union, [large])
    fifo = tmp_path / 'pipe'
    os.mkfifo(fifo)
    fail('E_INPUT_TYPE', plan_union, [fifo])
    fail('E_INPUT_TYPE', plan_union, [tmp_path])
    a, _ = create_sources(tmp_path)
    link = tmp_path / 'link.parquet'
    link.symlink_to(a)
    fail('E_INPUT_TYPE', plan_union, [link])
    linkdir = tmp_path / 'linked-directory'
    linkdir.symlink_to(tmp_path, target_is_directory=True)
    fail('E_PATH', plan_union, [linkdir / 'a.parquet'])
    fail('E_PATH', rewrite, [a], linkdir / 'out')
    for value in ['https://example.invalid/a.parquet', '*.parquet', '../owned.parquet',
                  'bad\x00name', 'a\\b']:
        fail('E_PATH', plan_union, [value])


def test_actual_unwritable_parent_and_input(tmp_path):
    assert os.geteuid() != 0, 'permission qualification requires non-root tests'
    a, b = create_sources(tmp_path)
    parent = tmp_path / 'locked'
    parent.mkdir()
    parent.chmod(0o500)
    try:
        fail('E_PERMISSION', rewrite, [a, b], parent / 'out')
        assert not (parent / 'out').exists()
        assert list(parent.iterdir()) == []
    finally:
        parent.chmod(0o700)
    a.chmod(0)
    try:
        fail('E_PERMISSION', plan_union, [a])
    finally:
        a.chmod(0o600)


def test_existing_destinations_and_atomic_race(tmp_path, monkeypatch):
    sources = create_sources(tmp_path)
    destination = tmp_path / 'out'
    destination.mkdir()
    fail('E_DEST_EXISTS', rewrite, sources, destination)
    sentinel = destination / 'sentinel'
    sentinel.write_bytes(b'keep')
    fail('E_DEST_EXISTS', rewrite, sources, destination)
    assert sentinel.read_bytes() == b'keep'
    other = tmp_path / 'raced'
    real_verify = oracle.verify
    def race(*args, **kwargs):
        result = real_verify(*args, **kwargs)
        other.mkdir()
        (other / 'sentinel').write_bytes(b'winner')
        return result
    monkeypatch.setattr(oracle, 'verify', race)
    fail('E_DEST_EXISTS', rewrite, sources, other)
    assert (other / 'sentinel').read_bytes() == b'winner'
    no_stage(tmp_path)


def test_oracle_failure_and_incomplete_write(tmp_path, monkeypatch):
    sources = create_sources(tmp_path)
    originals = [p.read_bytes() for p in sources]
    def reject(*args, **kwargs):
        raise UnionError('E_ORACLE')
    with monkeypatch.context() as patch:
        patch.setattr(oracle, 'verify', reject)
        fail('E_ORACLE', rewrite, sources, tmp_path / 'out')
    assert not (tmp_path / 'out').exists()
    no_stage(tmp_path)
    def partial(table, stream, **kwargs):
        stream.write(b'PAR1-incomplete')
        raise OSError('sensitive-native-error')
    monkeypatch.setattr(core.pq, 'write_table', partial)
    fail('E_WRITE', rewrite, sources, tmp_path / 'out')
    assert [p.read_bytes() for p in sources] == originals
    assert not (tmp_path / 'out').exists()
    no_stage(tmp_path)


def test_source_change_and_committed_error(tmp_path, monkeypatch):
    sources = create_sources(tmp_path)
    real_verify = oracle.verify
    def mutate(*args, **kwargs):
        result = real_verify(*args, **kwargs)
        with sources[0].open('ab') as stream:
            stream.write(b'changed')
        return result
    with monkeypatch.context() as patch:
        patch.setattr(oracle, 'verify', mutate)
        fail('E_SOURCE_CHANGED', rewrite, sources, tmp_path / 'out')
    assert not (tmp_path / 'out').exists()
    no_stage(tmp_path)
    sources = create_sources(tmp_path)
    def fail_sync(fd):
        raise OSError('post-commit failure')
    monkeypatch.setattr(storage, 'sync_parent', fail_sync)
    fail('E_COMMITTED_IO', rewrite, sources, tmp_path / 'out')
    assert (tmp_path / 'out' / 'data.parquet').exists()
    assert inspect_receipt(tmp_path / 'out')['verification']['rows'] == 4
    no_stage(tmp_path)


def test_inspection_tampering(tmp_path):
    sources = create_sources(tmp_path)
    rewrite(sources, tmp_path / 'out')
    receipt = tmp_path / 'out' / 'receipt.json'
    original = receipt.read_bytes()
    receipt.write_bytes(b'{"format":"secret","format":"other"}')
    fail('E_RECEIPT', inspect_receipt, tmp_path / 'out')
    record = json.loads(original)
    record['plan']['sources'][0]['rows'] = 99
    receipt.write_bytes(core.canonical(record))
    fail('E_RECEIPT', inspect_receipt, tmp_path / 'out')
    receipt.write_bytes(original)
    data = tmp_path / 'out' / 'data.parquet'
    with data.open('ab') as stream:
        stream.write(b'tamper')
    fail('E_RECEIPT', inspect_receipt, tmp_path / 'out')


def test_safe_cast_failure_is_sanitized(tmp_path, monkeypatch):
    sources = create_sources(tmp_path)
    def bad_cast(*args, **kwargs):
        raise pa.ArrowInvalid('private value and path')
    monkeypatch.setattr(core.pc, 'cast', bad_cast)
    fail('E_CAST', rewrite, sources, tmp_path / 'out')
    assert not (tmp_path / 'out').exists()


@pytest.mark.parametrize('target', [
    'root', 'plan', 'field', 'dictionary_values', 'source', 'artifact', 'versions'])
def test_receipt_records_reject_arrays_before_native_readers(tmp_path, monkeypatch, target):
    plain = tmp_path / 'plain.parquet'
    pq.write_table(pa.table({'x': pa.array([1], pa.int32())}), plain)
    record = rewrite([plain], tmp_path / 'out')
    if target == 'root':
        record = []
    elif target == 'field':
        record['plan']['schema'][0] = []
    elif target == 'source':
        record['plan']['sources'][0] = []
    elif target == 'versions':
        record['versions'] = list(record['versions'])
    else:
        record[target] = []
    (tmp_path / 'out' / 'receipt.json').write_bytes(core.canonical(record))

    def forbidden(*args, **kwargs):
        pytest.fail('malformed receipt must fail before native verification')

    monkeypatch.setattr(oracle, 'verify', forbidden)
    fail('E_RECEIPT', inspect_receipt, tmp_path / 'out')


def test_instance_serializes_jobs(tmp_path, monkeypatch):
    sources = create_sources(tmp_path)
    worker = UnionRewriter()
    real_prepare = core._prepare
    active = 0
    peak = 0
    mutex = threading.Lock()
    failures = []
    def slow_prepare(*args):
        nonlocal active, peak
        with mutex:
            active += 1
            peak = max(peak, active)
        try:
            time.sleep(0.02)
            return real_prepare(*args)
        finally:
            with mutex:
                active -= 1
    monkeypatch.setattr(core, '_prepare', slow_prepare)
    def job():
        try:
            worker.plan_union(sources)
        except Exception as exc:
            failures.append(type(exc).__name__)
    threads = [threading.Thread(target=job) for _ in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=15)
    assert not any(thread.is_alive() for thread in threads)
    assert failures == [] and peak == 1


def test_row_group_cap_precedes_per_group_metadata_iteration(tmp_path, monkeypatch):
    path = tmp_path / 'groups.parquet'
    pq.write_table(pa.table({'x': pa.array(range(65), pa.int32())}),
                   path, row_group_size=1)
    original = pq.ParquetFile

    class Metadata:
        def __init__(self, real):
            self.num_rows = real.num_rows
            self.num_row_groups = real.num_row_groups

        def row_group(self, index):
            pytest.fail('rejected row-group count must not be iterated')

    class Reader:
        def __init__(self, *args, **kwargs):
            self.real = original(*args, **kwargs)
            self.schema_arrow = self.real.schema_arrow
            self.metadata = Metadata(self.real.metadata)

        def close(self):
            self.real.close()

    monkeypatch.setattr(core.pq, 'ParquetFile', Reader)
    fail('E_LIMIT_GROUPS', plan_union, [path])
