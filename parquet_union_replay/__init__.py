"""Parquet Schema Union Rewrite — development version 0.1.0."""

from .core import UnionRewriter, inspect_receipt, plan_union, rewrite
from .errors import UnionError

__version__ = '0.1.0'
__all__ = ['UnionRewriter', 'UnionError', 'plan_union', 'rewrite', 'inspect_receipt']
