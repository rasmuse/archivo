# -*- coding: utf-8 -*-

from __future__ import annotations
from pathlib import Path
import os
import json
import shutil
import tempfile
import datetime
from typing import (
    Union,
    )

import attr

from archivo import *
from archivo.specs import (
    iter_paths_and_specs,
    get_apparent_name,
    read_spec,
    DifferentSpec,
    check_fulfils_spec,
    )


class CollisionError(Exception):
    pass

class RestoreError(Exception):
    pass


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


def set_mode(path, mode):
    os.chmod(path, mode)

def set_mtime_ns(path, mtime_ns):
    atime_ns = os.stat(path).st_atime_ns
    os.utime(path, ns=(atime_ns, mtime_ns))


@attr.s(auto_attribs=True)
class Storage:
    path: Path = attr.ib(converter=ensure_abs)
    meta: Dict[str, Union[int, str]] = attr.ib(init=False, repr=False)
    hash_name: str = attr.ib(init=False, repr=False)

    def _get_storage_path(self, file_spec):
        return self.path / f'{file_spec.hash_name}/{file_spec.hexdigest}'

    def __attrs_post_init__(self):
        with open(Storage._get_meta_path(self.path), 'r') as f:
            self.meta = json.load(f)
            self.hash_name = self.meta['hash_name']

    @staticmethod
    def _get_meta_path(storage_path: Path) -> Path:
        return storage_path / '.archivo-storage'

    @staticmethod
    def create(path: PathLike, hash_name=DEFAULT_HASH) -> Storage:
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


    def store(self, src_path: PathLike) -> FileOrDirSpec:
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

    def _restore_metadata(self, meta, dst_path):
        set_mode(dst_path, meta.mode)
        set_mtime_ns(dst_path, meta.mtime_ns)

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
            self._restore_metadata(spec.meta, dst_dir / rel_path)

        try:
            check_fulfils_spec(root_dst_path, root_spec)
        except DifferentSpec as e:
            raise RestoreError('Could not restore') from e

    def restore(self, spec: FileOrDirSpec, dst_dir: PathLike) -> None:
        dst_dir = Path(dst_dir)
        dst_path = dst_dir / spec.name

        if dst_path.exists():
            try:
                check_fulfils_spec(dst_path, spec)
            except DifferentSpec as e:
                raise CollisionError(f'Other file or dir at {dst_path}') from e

            # All fine; no restore needed
            return

        with tempfile.TemporaryDirectory(dir=self.path) as tmp_dir:
            tmp_dir = Path(tmp_dir)
            tmp_path = tmp_dir / spec.name
            self._restore_into(spec, tmp_dir)
            os.rename(tmp_path, dst_path)
