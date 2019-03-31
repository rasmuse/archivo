# -*- coding: utf-8 -*-

"""Main module."""

from __future__ import annotations
from pathlib import Path
import os
import json
import stat
import shutil
import tempfile
import hashlib
import datetime
import typing
from typing import (
    Union,
    Sequence,
    Dict,
    Generator,
    Tuple,
    Any,
    TypeVar,
    NewType,
    Callable,
    )
import attr

T = TypeVar('T')

path_like = Union[Path, str]

DEFAULT_HASH = 'sha256'

def read_file_meta(path):
    stat_result = os.stat(path)
    return {
        'mtime_ns': stat_result.st_mtime_ns,
        'atime_ns': stat_result.st_atime_ns,
        'mode': stat_result.st_mode,
        'size': stat_result.st_size,
    }

def set_mode(path, mode):
    os.chmod(path, mode)

def set_mtime_ns(path, mtime_ns):
    atime_ns = read_file_meta(path)['atime_ns']
    os.utime(path, ns=(atime_ns, mtime_ns))

_CHUNK_SIZE = 4096

def get_file_hexdigest(path: path_like, hash_name: str) -> str:
    m = hashlib.new(hash_name)
    with open(path, 'rb') as f:
        while True:
            data = f.read(_CHUNK_SIZE)
            if not data:
                break
            m.update(data)
    return m.hexdigest()

@attr.s(auto_attribs=True)
class FileSpec:
    name: str
    hash_name: str
    hexdigest: str
    mode: int
    mtime_ns: int
    size: int

@attr.s(auto_attribs=True)
class DirSpec:
    name: str
    hash_name: str
    contents: Sequence[FileOrDirSpec]
    mode: int
    mtime_ns: int


FileOrDirSpec = Union[FileSpec, DirSpec]

def _iter_paths_and_specs(spec, subdir, **kwargs):
    this_path = ensure_rel(subdir / spec.name)
    if isinstance(spec, FileSpec):
        if kwargs['files']:
            yield (this_path, spec)
    else:
        if kwargs['dirs']:
            yield (this_path, spec)
        for child in spec.contents:
            yield from _iter_paths_and_specs(child, this_path, **kwargs)


def iter_paths_and_specs(
    spec: FileOrDirSpec,
    dirs=True,
    files=True,
    ) -> Generator[Tuple[RelPath, FileSpec]]:
    start_dir = Path('.')
    return _iter_paths_and_specs(spec, start_dir, dirs=dirs, files=files)


def get_apparent_name(path: Path) -> str:
    # Doing path.resolve() on a symlink would give the name of what
    # the symlink points to, but we want the name of the symlink
    # to represent the file or directory of what the symlink points to.
    if path.is_symlink():
        return path.name

    # For concrete files and directories (including '.', and '..' etc)
    # we get the right name by doing path.resolve()
    else:
        return path.resolve().name


def read_spec(path: path_like, hash_name: str) -> FileOrDirSpec:
    path = Path(path)
    # Doing stat first of all to get it before any possible modification
    # by following operations
    file_meta = read_file_meta(path)

    if path.is_dir():
        type_ = DirSpec
    elif path.is_file():
        type_ = FileSpec
    else:
        raise ValueError(f'path {path} has unsupported type')

    kwargs = {}
    kwargs['name'] = get_apparent_name(path)
    kwargs['hash_name'] = hash_name
    kwargs['mode'] = file_meta['mode']
    kwargs['mtime_ns'] = file_meta['mtime_ns']

    if type_ == FileSpec:
        kwargs['hexdigest'] = get_file_hexdigest(path, hash_name)
        kwargs['size'] = file_meta['size']

    if type_ == DirSpec:
        kwargs['contents'] = [
            read_spec(child, hash_name)
            for child in path.iterdir()
            ]

    return type_(**kwargs)


AbsPath = NewType('AbsPath', Path)
RelPath = NewType('RelPath', Path)

def ensure_abs(path: path_like) -> AbsPath:
    resolved = Path(path).resolve()
    return AbsPath(resolved)

def ensure_rel(path: path_like) -> RelPath:
    path = Path(path)
    if path.is_absolute():
        raise ValueError(f'path {path} is not relative')
    return RelPath(path)

def write_json_file(data, path):
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)
        f.write('\n')

def now_to_text():
    return datetime.datetime.utcnow().isoformat()


class CollisionError(Exception):
    def __init__(self, message, desired, existing):
        self.message = message
        self.desired = desired
        self.existing = existing

    def __str__(self):
        return self.message

class RestoreError(Exception):
    def __init__(self, message, desired, restored):
        self.message = message
        self.desired = desired
        self.restored = restored

    def __str__(self):
        return self.message


def _copy_into(src_path: Path, dst_dir: Path) -> Path:
    name = get_apparent_name(src_path)
    dst_path = dst_dir / name

    if src_path.is_dir():
        shutil.copytree(src_path, dst_path)
    elif src_path.is_file():
        shutil.copy2(src_path, dst_path)
    else:
        raise ValueError(f'path {src_path} is of unsupported type')

    return dst_path


@attr.s(auto_attribs=True)
class Storage:
    path: AbsPath = attr.ib(converter=ensure_abs)
    meta: Dict[str, Any] = attr.ib(init=False, repr=False)
    hash_name: str = attr.ib(init=False, repr=False)

    def _get_storage_path(self, file_spec):
        return self.path / f'{file_spec.hash_name}/{file_spec.hexdigest}'

    def __attrs_post_init__(self):
        with open(Storage._get_meta_path(self.path), 'r') as f:
            self.meta = json.load(f)
            self.hash_name = self.meta['hash_name']

    @staticmethod
    def _get_meta_path(storage_path: AbsPath) -> AbsPath:
        return storage_path / '.archivo-storage'

    @staticmethod
    def create(path: path_like, hash_name=DEFAULT_HASH) -> Storage:
        path = ensure_abs(path)
        os.makedirs(path, exist_ok=False)
        metadata = {
            'created': now_to_text(),
            'hash_name': hash_name,
        }
        write_json_file(metadata, Storage._get_meta_path(path))
        return Storage(path)


    def has_file(self, file_spec: FileSpec) -> bool:
        return self._get_storage_path(file_spec).exists()


    def store(self, src_path: path_like) -> FileOrDirSpec:
        src_path = Path(src_path)
        with tempfile.TemporaryDirectory(dir=self.path) as tmp_dir:
            tmp_dir = Path(tmp_dir)
            tmp_path = _copy_into(src_path, tmp_dir)

            spec = read_spec(tmp_path, self.hash_name)

            for rel_path, file_spec in iter_paths_and_specs(spec, dirs=False):
                if self.has_file(file_spec):
                    continue

                tmp_path = tmp_dir / rel_path
                dst_path = self._get_storage_path(file_spec)
                os.makedirs(dst_path.parent, exist_ok=True)
                os.rename(tmp_path, dst_path)

        return spec

    def _restore_metadata(self, spec, dst_path):
        set_mode(dst_path, spec.mode)
        set_mtime_ns(dst_path, spec.mtime_ns)

    def _restore_into(self, root_spec, dst_dir):

        root_dst_path = dst_dir / root_spec.name

        # Create all the directories
        for rel_path, dir_spec in iter_paths_and_specs(root_spec, files=False):
            os.makedirs(dst_dir / rel_path, exist_ok=True)

        # Copy all the files
        for rel_path, file_spec in iter_paths_and_specs(root_spec, dirs=False):
            src_path = self._get_storage_path(file_spec)
            shutil.copy2(src_path, dst_dir / rel_path)

        # Restore metadata
        for rel_path, spec in iter_paths_and_specs(root_spec):
            self._restore_metadata(spec, dst_dir / rel_path)

        # Check that results are correct
        restored_spec = read_spec(root_dst_path, root_spec.hash_name)

        if not restored_spec == root_spec:
            raise RestoreError(
                message='Restoration resulted in the wrong spec.',
                desired=root_spec,
                restored=restored_spec
                )

    def restore(self, spec: FileOrDirSpec, dst_dir: path_like) -> None:
        dst_dir = Path(dst_dir)
        dst_path = dst_dir / spec.name

        if dst_path.exists():
            existing_spec = read_spec(dst_path, spec.hash_name)
            if existing_spec != spec:
                raise CollisionError(
                    f'Another file or directory is on path {dst_path}',
                    desired=spec,
                    existing=existing_spec
                    )

            # All fine; no restore needed
            return

        with tempfile.TemporaryDirectory(dir=self.path) as tmp_dir:
            tmp_dir = Path(tmp_dir)
            tmp_path = tmp_dir / spec.name
            self._restore_into(spec, tmp_dir)
            os.rename(tmp_path, dst_path)
