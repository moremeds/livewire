"""Reversible filesystem-safe symbol partition names."""

from __future__ import annotations

from urllib.parse import quote, unquote


def encode_symbol(symbol: str) -> str:
    return quote(symbol, safe="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-")


def decode_symbol(value: str) -> str:
    return unquote(value)
