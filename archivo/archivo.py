# -*- coding: utf-8 -*-

"""Main module."""

from pathlib import Path
import hashlib
import typing
from typing import (
    Union,
    Sequence,
    Dict,
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

@attr.s(auto_attribs=True)
class DirSpec:
    files: Dict[str, 'NodeSpec']

NodeSpec = Union[DirSpec, FileSpec]

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


def make_node_spec(path: Path, hash_name: str) -> NodeSpec:
    if path.is_file():
        return make_file_spec(path, hash_name)
    else:
        return make_dir_spec(path, hash_name)

def make_dir_spec(start_dir: Path, hash_name: str) -> DirSpec:
    if not start_dir.is_dir():
        raise ValueError(f'the path {start_dir} is not a directory')

    files = {
        child.name: make_node_spec(child, hash_name)
        for child in start_dir.iterdir()
        }
    return DirSpec(files)

def store(
    paths: Sequence[path_like],
    storage: Storage,
    relative_to: path_like = '.'
    ) -> DirSpec:
    pass


