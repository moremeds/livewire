"""Reversible filesystem-safe symbol partition names."""

from __future__ import annotations

from urllib.parse import unquote

_CASE_SAFE = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")


def canonical_symbol(symbol: str) -> str:
    """Normalize lowercase user input without collapsing provider-significant case."""
    value = str(symbol).strip()
    return value.upper() if value.islower() else value


def encode_symbol(symbol: str) -> str:
    """Encode symbols distinctly on case-insensitive filesystems."""
    parts: list[str] = []
    for character in symbol:
        if character in _CASE_SAFE:
            parts.append(character)
        else:
            parts.extend(f"%{byte:02X}" for byte in character.encode("utf-8"))
    return "".join(parts)


def decode_symbol(value: str) -> str:
    return unquote(value)
