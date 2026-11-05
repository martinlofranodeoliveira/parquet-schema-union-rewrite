"""Local, non-following I/O and Linux no-replace publication."""

from contextlib import contextmanager
import ctypes
import errno
import hashlib
import os
from pathlib import Path
import stat
import unicodedata
import uuid

from .errors import UnionError

MAX_INPUT_BYTES = 16 * 1024 * 1024
MAX_ARTIFACT_BYTES = 128 * 1024 * 1024
MAX_RECEIPT_BYTES = 4 * 1024 * 1024
MARKER = b'parquet-union-replay owned directory v1\n'


def local_path(value) -> Path:
    raw = os.fspath(value)
    if not isinstance(raw, str):
        raise UnionError('E_PATH')
    if not raw or len(raw.encode('utf-8')) > 4096:
        raise UnionError('E_PATH')
    if any(unicodedata.category(c) == 'Cc' for c in raw):
        raise UnionError('E_PATH')
    if any(c in raw for c in '*?[]\\:') or '..' in raw.split('/'):
        raise UnionError('E_PATH')
    result = Path(os.path.abspath(raw))
    if result == Path('/'):
        raise UnionError('E_PATH')
    return result


@contextmanager
def parent_handle(value):
    path = local_path(value)
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parent.parts[1:]:
            try:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                dir_fd=fd)
            except OSError as exc:
                if exc.errno in (errno.ELOOP, errno.ENOTDIR):
                    raise UnionError('E_PATH') from None
                raise
            os.close(fd)
            fd = child
        yield path, fd, path.name
    finally:
        os.close(fd)


def _identity(st):
    return st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns


def read_file(value, limit=MAX_INPUT_BYTES):
    """Stat before open; never block opening a FIFO; never follow links."""
    with parent_handle(value) as (path, parent, name):
        before = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode):
            raise UnionError('E_INPUT_TYPE')
        if before.st_size > limit:
            raise UnionError('E_LIMIT_BYTES')
        try:
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                         dir_fd=parent)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise UnionError('E_INPUT_TYPE') from None
            raise
        with os.fdopen(fd, 'rb') as stream:
            opened = os.fstat(stream.fileno())
            if not stat.S_ISREG(opened.st_mode) or _identity(before) != _identity(opened):
                raise UnionError('E_SOURCE_CHANGED')
            data = stream.read(limit + 1)
            after = os.fstat(stream.fileno())
        if len(data) > limit:
            raise UnionError('E_LIMIT_BYTES')
        if _identity(opened) != _identity(after) or len(data) != after.st_size:
            raise UnionError('E_SOURCE_CHANGED')
    return path, data


def digest(data):
    return hashlib.sha256(data).hexdigest()


def write_bytes(path, data):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def rename_noreplace(parent, source, destination):
    """There is deliberately no racy exists()+rename fallback."""
    libc = ctypes.CDLL(None, use_errno=True)
    function = getattr(libc, 'renameat2', None)
    if function is None:
        raise UnionError('E_PLATFORM')
    function.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int,
                         ctypes.c_char_p, ctypes.c_uint]
    function.restype = ctypes.c_int
    result = function(parent, os.fsencode(source), parent,
                      os.fsencode(destination), 1)
    if result:
        error = ctypes.get_errno()
        if error in (errno.EEXIST, errno.ENOTEMPTY):
            raise UnionError('E_DEST_EXISTS')
        if error in (errno.ENOSYS, errno.EINVAL, errno.EOPNOTSUPP):
            raise UnionError('E_PLATFORM')
        raise OSError(error, 'publication failed')


def sync_parent(fd):
    os.fsync(fd)


class Stage:
    def __init__(self, path, parent, name, destination):
        self.path = path
        self.parent = parent
        self.name = name
        self.destination = destination
        self.committed = False

    def publish(self):
        fd = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        rename_noreplace(self.parent, self.name, self.destination)
        self.committed = True
        try:
            sync_parent(self.parent)
        except OSError:
            raise UnionError('E_COMMITTED_IO') from None

    def cleanup(self):
        # Only our fixed marked files; no recursive deletion, no symlink following.
        try:
            fd = os.open(self.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                         dir_fd=self.parent)
            try:
                marker = os.open('.union-owned', os.O_RDONLY | os.O_NOFOLLOW,
                                 dir_fd=fd)
                with os.fdopen(marker, 'rb') as stream:
                    if stream.read(len(MARKER) + 1) != MARKER:
                        return
                names = os.listdir(fd)
                if not set(names) <= {'data.parquet', 'receipt.json', '.union-owned'}:
                    return
                for name in names:
                    info = os.stat(name, dir_fd=fd, follow_symlinks=False)
                    if not stat.S_ISREG(info.st_mode):
                        return
                for name in names:
                    os.unlink(name, dir_fd=fd)
            finally:
                os.close(fd)
            os.rmdir(self.name, dir_fd=self.parent)
        except OSError:
            # A failed cleanup must not obscure the original job failure.
            # An unremoved marked stage is never a published destination.
            return


@contextmanager
def staging(destination):
    with parent_handle(destination) as (path, parent, name):
        try:
            os.stat(name, dir_fd=parent, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise UnionError('E_DEST_EXISTS')
        stage_name = '.parquet-union-' + uuid.uuid4().hex
        os.mkdir(stage_name, 0o700, dir_fd=parent)
        stage = Stage(path.parent / stage_name, parent, stage_name, name)
        try:
            write_bytes(stage.path / '.union-owned', MARKER)
        except BaseException:
            # If marker creation failed, only an empty directory is removable.
            try:
                os.rmdir(stage_name, dir_fd=parent)
            except OSError:
                pass
            raise
        try:
            yield stage
        finally:
            if not stage.committed:
                stage.cleanup()
