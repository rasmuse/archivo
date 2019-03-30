# -*- coding: utf-8 -*-

"""Main module."""

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
    )
import attr

DEFAULT_HASH = 'sha256'



def _get_stat_data(file):
    stat_result = os.stat(file.fileno())
    return {
        'mode': stat_result.st_mode,
        'size': stat_result.st_size,
        'ctime_ns': stat_result.st_ctime_ns,
        'atime_ns': stat_result.st_atime_ns,
        'mtime_ns': stat_result.st_mtime_ns,
    }

def _restore_stat_data(path, file_spec):
    os.chmod(path, file_spec.mode)
    os.utime(path, ns=(file_spec.atime_ns, file_spec.mtime_ns))


_EQUIV_ATTRIBUTES = [
    'hash_name',
    'hexdigest',
    'mode',
    'mtime_ns',
    'size',
]

@attr.s(auto_attribs=True)
class FileSpec:
    hash_name: str
    hexdigest: str
    mode: int
    atime_ns: int
    mtime_ns: int
    size: int
    ctime_ns: int


    def _equiv_dict(self):
        return {k: getattr(self, k) for k in _EQUIV_ATTRIBUTES}

    def is_equivalent(self, other):
        return self._equiv_dict() == other._equiv_dict()

FileList = Dict[Path, FileSpec]


def write_json_file(data, path):
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)
        f.write('\n')

def now_to_text():
    return datetime.datetime.utcnow().isoformat()

def datetime_from_text(s):
    return datetime.datetime.fromisoformat(s)

path_like = Union[Path, str]

def _to_abs_path(path_like: path_like) -> Path:
    return Path(path_like).resolve()

def format_datetime(dt, repository):
    return dt.strftime(repository.datetime_format)


_CHUNK_SIZE = 8096

def make_file_spec(path: Path, hash_name: str) -> FileSpec:
    if not path.is_file():
        raise ValueError(f'the path {path} is not a file')

    m = hashlib.new(hash_name)
    with open(path, 'rb') as f:
        stat_data = _get_stat_data(f)
        while True:
            data = f.read(_CHUNK_SIZE)
            if not data:
                break
            m.update(data)

    return FileSpec(hash_name, m.hexdigest(), **stat_data)


def _generate_relative_file_paths(
    current_dir: Path,
    root_dir=None
    ) -> Generator[Path, None, None]:

    assert current_dir.is_dir(), current_dir
    assert current_dir.is_absolute()

    if not root_dir:
        root_dir = current_dir

    for child in current_dir.iterdir():
        assert child.is_absolute()
        if child.is_file():
            yield child.relative_to(root_dir)
        else:
            yield from _generate_relative_file_paths(child, root_dir=root_dir)

def make_file_list(the_dir: Path, hash_name: str) -> FileList:
    if not the_dir.is_dir():
        raise ValueError(f'the path {the_dir} is not a directory')

    files = {
        child: make_file_spec(child, hash_name)
        for child in _generate_relative_file_paths(the_dir)
        }

    return files


def _move_files(moves):
    for src_path, dst_path in moves.items():
        os.makedirs(dst_path.parent, exist_ok=True)
        os.rename(src_path, dst_path)


class CollisionError(Exception):
    pass


class RestoreError(Exception):
    pass


def _get_storage_meta_path(storage_dir: Path) -> Path:
    return storage_dir / '.archivo-storage'


def _validate_storage(instance, attribute, value):
    storage_meta_path = _get_storage_meta_path(value)
    if not storage_meta_path.is_file():
        raise ValueError(f'metadata file {storage_meta_path} not found')


def _get_apparent_name(path: Path) -> str:
    # Doing path.resolve() on a symlink would give the name of what
    # the symlink points to, but we want the name of the symlink
    # to represent the file or directory of what the symlink points to.
    if path.is_symlink():
        return path.name

    # For concrete files and directories (including '.', and '..' etc)
    # we get the right name by doing path.resolve()
    else:
        return path.resolve().name


def _copy_into(src_path: Path, dst_dir: Path) -> None:
    name = _get_apparent_name(src_path)
    dst_path = dst_dir.joinpath(name)

    if src_path.is_dir():
        shutil.copytree(src_path, dst_path)
    elif src_path.is_file():
        shutil.copy2(src_path, dst_path)
    else:
        raise ValueError(f'path {src_path} is of unsupported type')


@attr.s(auto_attribs=True)
class Storage:
    path: Path = attr.ib(converter=_to_abs_path, validator=_validate_storage)
    meta: Dict[str, Any] = attr.ib(init=False, repr=False)
    hash_name: str = attr.ib(init=False, repr=False)

    def get_file_path(self, file_spec):
        return self.path / f'{file_spec.hash_name}/{file_spec.hexdigest}'

    def get_tmp_dir(self):
        return self.path / 'temp'

    def __attrs_post_init__(self):
        with open(_get_storage_meta_path(self.path), 'r') as f:
            self.meta = json.load(f)
            self.hash_name = self.meta['hash_name']

    @staticmethod
    def create(path: path_like, hash_name=DEFAULT_HASH) -> None:
        path = Path(path)
        os.makedirs(path, exist_ok=False)
        metadata = {
            'created': now_to_text(),
            'hash_name': hash_name,
        }
        write_json_file(metadata, _get_storage_meta_path(path))


    def has_file(self, file_spec: FileSpec) -> bool:
        return self.get_file_path(file_spec).exists()


    def store(self, path: path_like) -> FileList:
        path = Path(path)
        tmp_dir = self.get_tmp_dir()
        os.makedirs(tmp_dir, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=tmp_dir) as tmp_dir:
            tmp_dir = Path(tmp_dir)

            _copy_into(path, tmp_dir)

            file_list = make_file_list(tmp_dir, self.hash_name)

            moves = {
                tmp_dir.joinpath(rel_path): self.get_file_path(file_spec)
                for rel_path, file_spec in file_list.items()
                if not self.has_file(file_spec)
                }

            _move_files(moves)

        return file_list

    def _restore_to_tmp_dir(self, file_list, dst_dir):
        for rel_path, file_spec in file_list.items():
            src_path = self.get_file_path(file_spec)
            dst_path = dst_dir.joinpath(rel_path)
            os.makedirs(dst_path.parent, exist_ok=True)
            shutil.copy2(src_path, dst_path)
            _restore_stat_data(dst_path, file_spec)

            restored_spec = make_file_spec(dst_path, file_spec.hash_name)
            if not file_spec.is_equivalent(restored_spec):
                message = f'restored {restored_spec} but expected {file_spec}'
                raise RestoreError(message)


    def restore(self, file_list: FileList, dst_dir: path_like) -> None:
        dst_dir = Path(dst_dir)

        colliders = {p for p in file_list if dst_dir.joinpath(p).exists()}

        for rel_path in colliders:
            desired_spec = file_list[rel_path]
            dst_path = dst_dir.joinpath(rel_path)
            existing_spec = make_file_spec(dst_path, desired_spec.hash_name)
            if not existing_spec.is_equivalent(desired_spec):
                raise CollisionError(
                    f'collision at {dst_path}: '
                    f'existing {existing_spec} '
                    f'but should place {desired_spec}'
                    )

        to_restore = {k: v for k, v in file_list.items() if k not in colliders}

        tmp_dir = self.get_tmp_dir()
        os.makedirs(tmp_dir, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=tmp_dir) as tmp_dir:
            tmp_dir = Path(tmp_dir)
            self._restore_to_tmp_dir(to_restore, tmp_dir)

            moves = {
                tmp_dir.joinpath(rel_path): dst_dir.joinpath(rel_path)
                for rel_path in to_restore
            }

            _move_files(moves)
