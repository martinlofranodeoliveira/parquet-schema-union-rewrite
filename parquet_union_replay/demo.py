"""Original local synthetic fixtures; no network or imported datasets."""

from pathlib import Path
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from .core import UnionRewriter
from .errors import UnionError


def create_sources(directory):
    directory = Path(directory)
    a = pa.table({
        'id': pa.array([1, 2], type=pa.int32()),
        'color': pa.DictionaryArray.from_arrays(
            pa.array([0, None], type=pa.int8()), pa.array(['red', 'blue'])),
    })
    b = pa.table({
        'id': pa.array([3, 4], type=pa.int64()),
        'color': pa.DictionaryArray.from_arrays(
            pa.array([0, 1], type=pa.int16()), pa.array(['green', 'red'])),
        'note': pa.array(['x', None], type=pa.string()),
    })
    paths = [directory / 'a.parquet', directory / 'b.parquet']
    for path, table in zip(paths, [a, b]):
        pq.write_table(table, path, row_group_size=1, compression='NONE',
                       use_dictionary=['color'], store_schema=True)
    return paths


def run_demo(check=False):
    with tempfile.TemporaryDirectory(prefix='parquet-union-demo-') as temporary:
        root = Path(temporary)
        sources = create_sources(root)
        worker = UnionRewriter()
        receipt = worker.rewrite(sources, root / 'artifact')
        inspected = worker.inspect_receipt(root / 'artifact')
        table = pq.read_table(root / 'artifact' / 'data.parquet', use_threads=False)
        actual = list(zip(*(column.to_pylist() for column in table.columns)))
        golden = [('red', 1, None, 0, 0), (None, 2, None, 0, 1),
                  ('green', 3, 'x', 1, 0), ('red', 4, None, 1, 1)]
        if check:
            if (actual != golden or receipt != inspected
                    or table.schema.names != ['color', 'id', 'note',
                                               '__source_file', '__source_row']
                    or table.schema.field('id').type != pa.int64()
                    or receipt['dictionary_values'] != {'color': ['green', 'red']}
                    or receipt['verification']['id_sum'] != 10
                    or receipt['verification']['nonnull']['note'] != 1
                    or receipt['artifact']['row_groups'] != 2):
                raise UnionError('E_DEMO_CHECK')
        return {'rows': len(actual), 'id_sum': receipt['verification']['id_sum'],
                'note_nonnull': receipt['verification']['nonnull']['note'],
                'dictionary': receipt['dictionary_values']['color'],
                'color_groups': receipt['verification']['color_groups'],
                'provenance': [[row[-2], row[-1]] for row in actual],
                'checked': bool(check), 'versions': receipt['versions']}
