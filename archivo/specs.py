# -*- coding: utf-8 -*-

"""Main module."""

from __future__ import annotations
from pathlib import Path
import os
import hashlib
from typing import (
    Union,
    Sequence,
    Generator,
    Tuple,
    )

import attr

from archivo import *


_CHUNK_SIZE = 4096

def get_file_hexdigest(path: PathLike, hash_name: str) -> str:
    m = hashlib.new(hash_name)
    with open(path, 'rb') as f:
        while True:
            data = f.read(_CHUNK_SIZE)
            if not data:
                break
            m.update(data)
    return m.hexdigest()

@attr.s(auto_attribs=True)
class FileMeta:
    mode: int
    mtime_ns: int
    size: int

@attr.s(auto_attribs=True)
class FileSpec:
    name: str
    hash_name: str
    hexdigest: str
    meta: FileMeta

@attr.s(auto_attribs=True)
class DirMeta:
    mode: int
    mtime_ns: int

@attr.s(auto_attribs=True)
class DirSpec:
    name: str
    contents: Sequence[FileOrDirSpec]
    meta: DirMeta

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


def read_meta(path: PathLike) -> Union[FileMeta, DirMeta]:
    path = Path(path).resolve()

    stat_result = os.stat(path)

    if path.is_file():
        return FileMeta(
            mtime_ns=stat_result.st_mtime_ns,
            mode=stat_result.st_mode,
            size=stat_result.st_size,
            )
    elif path.is_dir():
        return DirMeta(
            mtime_ns=stat_result.st_mtime_ns,
            mode=stat_result.st_mode,
            )


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


def read_spec(path: PathLike, hash_name: str) -> FileOrDirSpec:
    path = Path(path)
    # Doing stat first of all to get it before any possible modification
    # by following operations
    meta = read_meta(path)

    if path.is_dir():
        type_ = DirSpec
    elif path.is_file():
        type_ = FileSpec
    else:
        raise ValueError(f'path {path} has unsupported type')

    kwargs = {}
    kwargs['name'] = get_apparent_name(path)
    kwargs['meta'] = meta

    if type_ == FileSpec:
        kwargs['hexdigest'] = get_file_hexdigest(path, hash_name)
        kwargs['hash_name'] = hash_name

    if type_ == DirSpec:
        kwargs['contents'] = [
            read_spec(child, hash_name)
            for child in path.iterdir()
            ]

    return type_(**kwargs)


class DifferentSpec(Exception):
    def __init__(self, message, expected_spec, target_info):
        self.message = message
        self.expected_spec = expected_spec
        self.target_info = target_info

    def __str__(self):
        return self.message


def check_fulfils_spec(path: PathLike, root_spec: FileOrDirSpec):
    if path.is_symlink():
        raise ValueError(f'cannot check symlink {path}')

    path = Path(path).resolve()
    containing_dir = path.parent

    # For all dirs and files...
    for rel_path, spec in iter_paths_and_specs(root_spec):

        # Check that they exist
        abs_path = containing_dir / rel_path
        if not abs_path.exists():
            raise DifferentSpec(
                message=f'subpath {rel_path} does not exist',
                expected_spec=spec,
                target_info={'path': abs_path},
                )

        # Check that they have the right metadata
        meta = read_meta(abs_path)
        if meta != spec.meta:
            raise DifferentSpec(
                message=(
                    f'metadata at {rel_path} does not match: '
                    f'expected {spec.meta} but found {meta}'
                    ),
                expected_spec=spec,
                target_info={'path': abs_path, 'meta': meta},
                )

    # Then check that all files have the right spec (including content digest)
    for rel_path, spec in iter_paths_and_specs(root_spec, dirs=False):
        abs_path = containing_dir / rel_path
        assert abs_path.is_file()

        target_spec = read_spec(abs_path, spec.hash_name)
        if target_spec != spec:
            raise DifferentSpec(
                message=(
                    f'file spec at {rel_path} does not match: '
                    f'expected {spec} but found {target_spec}'
                    ),
                expected_spec=spec,
                target_info={'path': rel_path, 'spec': target_spec}
                )