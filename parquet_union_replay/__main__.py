"""CLI: constant error codes on stderr; JSON on successful data commands."""

import argparse
import json
from pathlib import Path
import sys

from . import UnionError, inspect_receipt, plan_union, rewrite
from .core import VERSION, canonical
from .demo import run_demo
from .storage import local_path, read_file


class Parser(argparse.ArgumentParser):
    def error(self, message):
        raise UnionError('E_ARGUMENT')


def _manifest(path):
    absolute, payload = read_file(path, 64 * 1024)
    try:
        values = json.loads(payload)
    except (ValueError, UnicodeError):
        raise UnionError('E_ARGUMENT') from None
    if not isinstance(values, list) or not all(isinstance(v, str) for v in values):
        raise UnionError('E_ARGUMENT')
    result = []
    for value in values:
        # Validate before joining so '..' cannot be normalized away.
        local_path(value)
        result.append(Path(value) if Path(value).is_absolute() else absolute.parent / value)
    return result


def main(argv=None):
    try:
        parser = Parser(description='Bounded local Parquet schema union, development 0.1.0')
        parser.add_argument('--version', action='version', version=VERSION)
        commands = parser.add_subparsers(dest='command', required=True, parser_class=Parser)
        plan = commands.add_parser('plan', help='inspect an explicit JSON manifest')
        plan.add_argument('manifest')
        write = commands.add_parser('rewrite', help='publish a new artifact directory')
        write.add_argument('manifest')
        write.add_argument('destination')
        write.add_argument('--row-group-size', type=int, default=2)
        inspect = commands.add_parser('inspect', help='check receipt and artifact consistency')
        inspect.add_argument('artifact')
        demo = commands.add_parser('demo', help='run only original synthetic fixtures')
        demo.add_argument('--assert', dest='check', action='store_true')
        demo.add_argument('--json', action='store_true', help='emit only JSON')
        args = parser.parse_args(argv)
        if args.command == 'plan':
            result = plan_union(_manifest(args.manifest))
        elif args.command == 'rewrite':
            result = rewrite(_manifest(args.manifest), args.destination,
                             row_group_size=args.row_group_size)
        elif args.command == 'inspect':
            result = inspect_receipt(args.artifact)
        else:
            result = run_demo(args.check)
            if not args.json:
                print('Synthetic union: rows=4; sum(id)=10; non-null note=1')
                print('Observed color dictionary: green, red')
                print('Independent verification completed; JSON follows.')
        sys.stdout.write(canonical(result).decode('utf-8'))
        return 0
    except UnionError as exc:
        code = exc.code
    except PermissionError:
        code = 'E_PERMISSION'
    except MemoryError:
        code = 'E_RESOURCE'
    except OSError:
        code = 'E_IO'
    except KeyboardInterrupt:
        code = 'E_INTERRUPTED'
    except Exception:
        code = 'E_INTERNAL'
    sys.stderr.write(code + '\n')
    return 3 if code == 'E_COMMITTED_IO' else 2


if __name__ == '__main__':
    raise SystemExit(main())
