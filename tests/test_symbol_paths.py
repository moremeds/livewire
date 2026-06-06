from clients.symbol_paths import decode_symbol, encode_symbol


def test_symbol_path_roundtrip():
    assert decode_symbol(encode_symbol("ABC/DEF %")) == "ABC/DEF %"


def test_common_symbols_are_unchanged():
    assert encode_symbol("BRK.B") == "BRK.B"


def test_mixed_case_symbol_does_not_collide_on_case_insensitive_filesystems():
    assert encode_symbol("BCPC") == "BCPC"
    assert encode_symbol("BCpC") == "BC%70C"
    assert encode_symbol("BCPC").lower() != encode_symbol("BCpC").lower()
    assert decode_symbol(encode_symbol("BCpC")) == "BCpC"
