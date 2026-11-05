"""Public errors contain codes only, including their repr and arguments."""

CODES = frozenset({
    'E_ARGUMENT', 'E_PATH', 'E_INPUT_TYPE', 'E_PERMISSION', 'E_IO',
    'E_SOURCE_CHANGED', 'E_PARQUET_READ', 'E_SCHEMA', 'E_NAME',
    'E_NAME_COLLISION', 'E_RESERVED', 'E_LIMIT_FILES', 'E_LIMIT_BYTES',
    'E_LIMIT_ROWS', 'E_LIMIT_COLUMNS', 'E_LIMIT_GROUPS', 'E_CAST',
    'E_WRITE', 'E_ORACLE', 'E_RECEIPT', 'E_DEST_EXISTS', 'E_PLATFORM',
    'E_COMMITTED_IO', 'E_RUNTIME', 'E_RESOURCE', 'E_INTERNAL',
    'E_INTERRUPTED', 'E_DEMO_CHECK',
})


class UnionError(Exception):
    """A stable, sanitized failure. No paths or native exception text."""

    def __init__(self, code: str):
        self.code = code if code in CODES else 'E_INTERNAL'
        super().__init__(self.code)
