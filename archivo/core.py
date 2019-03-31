# -*- coding: utf-8 -*-

from typing import (
    Union,
    )

from pathlib import Path

PathLike = Union[Path, str]

def ensure_abs(path: PathLike) -> Path:
    return Path(path).resolve()

def ensure_rel(path: PathLike) -> Path:
    path = Path(path)
    if path.is_absolute():
        raise ValueError(f'path {path} is not relative')
    return path

def write_json_file(data, path):
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)
        f.write('\n')

def now_to_text():
    return datetime.datetime.utcnow().isoformat()
