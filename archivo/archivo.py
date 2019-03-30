# -*- coding: utf-8 -*-

"""Main module."""

from pathlib import Path
import hashlib
import typing
from typing import (
    Union,
    Sequence,
    Dict,
    Generator,
    )
import attr

path_like = Union[Path, str]

def _validate_abspath(instance, attribute, value):
    if not value.is_absolute():
        raise ValueError(f'path {value} is not absolute')

@attr.s(auto_attribs=True)
class Storage:
    path: Path = attr.ib(converter=Path, validator=_validate_abspath)

@attr.s(auto_attribs=True)
class FileSpec:
    hash_name: str
    hexdigest: str

FileList = Dict[str, FileSpec]

_CHUNK_SIZE = 8096

def make_file_spec(path: Path, hash_name: str) -> FileSpec:
    if not path.is_file():
        raise ValueError(f'the path {path} is not a file')
    m = hashlib.new(hash_name)
    with open(path, 'rb') as f:
        while True:
            data = f.read(_CHUNK_SIZE)
            if not data:
                break
            m.update(data)

    return FileSpec(hash_name, m.hexdigest())

def _generate_file_paths(
    current_dir: Path,
    root_dir=None
    ) -> Generator[Path, None, None]:

    if not current_dir.is_dir():
        raise ValueError(f'the path {current_dir} is not a directory')

    if not root_dir:
        root_dir = current_dir

    for child in current_dir.iterdir():
        if child.is_file():
            yield child.relative_to(root_dir)
        else:
            yield from _generate_file_paths(child, root_dir=root_dir)

def make_file_list(the_dir: Path, hash_name: str) -> FileList:
    if not the_dir.is_dir():
        raise ValueError(f'the path {the_dir} is not a directory')

    files = {
        str(child): make_file_spec(child, hash_name)
        for child in _generate_file_paths(the_dir)
        }
    return files

def store(
    paths: Sequence[path_like],
    storage: Storage,
    relative_to: path_like = '.'
    ) -> FileList:
    pass


