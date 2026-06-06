from clients.symbol_paths import decode_symbol, encode_symbol


def test_symbol_path_roundtrip():
    assert decode_symbol(encode_symbol("ABC/DEF %")) == "ABC/DEF %"


def test_common_symbols_are_unchanged():
    assert encode_symbol("BRK.B") == "BRK.B"
