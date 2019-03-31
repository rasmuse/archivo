# -*- coding: utf-8 -*-

"""Top-level package for Archivo."""

__author__ = """Rasmus Einarsson"""
__email__ = 'mr@rasmuseinarsson.se'
__version__ = '0.1.0'

from archivo.core import (
    PathLike,
    ensure_abs,
    ensure_rel,
    now_to_text,
    write_json_file,
    )

DEFAULT_HASH = 'sha256'

