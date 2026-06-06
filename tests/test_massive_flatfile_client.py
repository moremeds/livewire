from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from clients.massive_flatfile_client import (
    FlatfileObjectStatus,
    MassiveFlatfileClient,
    _s3_key_for_date,
    normalize_object_key,
)


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "redacted"}}, "HeadObject")


def test_key_and_bucket_prefix_normalization():
    assert _s3_key_for_date(date(2026, 3, 15)) == "us_stocks_sip/minute_aggs_v1/2026/03/2026-03-15.csv.gz"
    assert normalize_object_key("flatfiles/us_stocks_sip/x") == "us_stocks_sip/x"


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("404", FlatfileObjectStatus.NOT_FOUND),
        ("NoSuchKey", FlatfileObjectStatus.NOT_FOUND),
        ("403", FlatfileObjectStatus.FORBIDDEN),
        ("AccessDenied", FlatfileObjectStatus.FORBIDDEN),
        ("500", FlatfileObjectStatus.TRANSIENT_ERROR),
    ],
)
def test_inspect_date_classifies_provider_errors(code, expected):
    s3 = MagicMock()
    s3.head_object.side_effect = _client_error(code)
    info = MassiveFlatfileClient(_s3_client=s3).inspect_date(date(2026, 5, 28))
    assert info.status == expected
    assert info.error == code


def test_inspect_date_classifies_available():
    s3 = MagicMock()
    s3.head_object.return_value = {"ContentLength": 123, "ETag": '"abc"'}
    info = MassiveFlatfileClient(_s3_client=s3).inspect_date(date(2026, 5, 28))
    assert info.status == FlatfileObjectStatus.AVAILABLE
    assert info.size_bytes == 123
    assert info.etag == "abc"


def test_list_objects_paginates_and_normalizes_prefix():
    s3 = MagicMock()
    s3.list_objects_v2.side_effect = [
        {"Contents": [{"Key": "a"}], "IsTruncated": True, "NextContinuationToken": "next"},
        {"Contents": [{"Key": "b"}], "IsTruncated": False},
    ]
    client = MassiveFlatfileClient(_s3_client=s3)
    assert client.list_objects("flatfiles/us_stocks_sip") == [{"Key": "a"}, {"Key": "b"}]
    assert s3.list_objects_v2.call_args_list[0].kwargs["Prefix"] == "us_stocks_sip"
    assert s3.list_objects_v2.call_args_list[1].kwargs["ContinuationToken"] == "next"


def test_download_date_to_path_streams_and_cleans_partial_failure(tmp_path):
    destination = tmp_path / "day.csv.gz"
    s3 = MagicMock()

    def fail(_bucket, _key, fh):
        fh.write(b"partial")
        raise RuntimeError("network failure")

    s3.download_fileobj.side_effect = fail
    with pytest.raises(RuntimeError, match="network failure"):
        MassiveFlatfileClient(_s3_client=s3).download_date_to_path(date(2026, 5, 28), destination)
    assert not destination.exists()


def test_context_manager_returns_client():
    s3 = MagicMock()
    with MassiveFlatfileClient(_s3_client=s3) as client:
        assert client._s3 is s3
