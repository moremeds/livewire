"""Tests for scripts/sync_to_r2.py — R2 sync logic."""

from pathlib import Path
from unittest.mock import MagicMock, patch, call

from livewire_scripts.sync_to_r2 import upload, download, main, _get_s3_client, _get_bucket, PARQUET_FILES_TO_SYNC


class TestClientHelpers:
    def test_get_s3_client(self, monkeypatch):
        monkeypatch.setenv("R2_ENDPOINT_URL", "https://fake.r2.cloudflarestorage.com")
        monkeypatch.setenv("R2_ACCESS_KEY_ID", "test-key")
        monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "test-secret")
        mock_boto3 = MagicMock()
        with patch.dict("sys.modules", {"boto3": mock_boto3}):
            _get_s3_client()
            mock_boto3.client.assert_called_once_with(
                "s3",
                endpoint_url="https://fake.r2.cloudflarestorage.com",
                aws_access_key_id="test-key",
                aws_secret_access_key="test-secret",
                region_name="auto",
            )

    def test_get_bucket_default(self, monkeypatch):
        monkeypatch.delenv("R2_BUCKET", raising=False)
        assert _get_bucket() == "market-data"

    def test_get_bucket_custom(self, monkeypatch):
        monkeypatch.setenv("R2_BUCKET", "my-bucket")
        assert _get_bucket() == "my-bucket"


class TestUpload:
    def test_uploads_parquet_files(self, tmp_path):
        bronze_dir = tmp_path / "bronze"
        equity_dir = bronze_dir / "asset_class=equity" / "symbol=AAPL"
        equity_dir.mkdir(parents=True)
        (equity_dir / "1d.parquet").write_bytes(b"fake parquet")

        mock_client = MagicMock()
        with patch("livewire_scripts.sync_to_r2._get_s3_client", return_value=mock_client):
            with patch("livewire_scripts.sync_to_r2._get_bucket", return_value="test-bucket"):
                count = upload(bronze_dir)

        assert count == 1
        mock_client.upload_file.assert_called_once()
        args = mock_client.upload_file.call_args
        assert "1d.parquet" in args[0][0]
        assert args[0][1] == "test-bucket"
        assert "asset_class=equity/symbol=AAPL/1d.parquet" in args[0][2]

    def test_uploads_multiple_files(self, tmp_path):
        bronze_dir = tmp_path / "bronze"
        for sym in ["AAPL", "NVDA"]:
            d = bronze_dir / "asset_class=equity" / f"symbol={sym}"
            d.mkdir(parents=True)
            (d / "1d.parquet").write_bytes(b"fake")

        mock_client = MagicMock()
        with patch("livewire_scripts.sync_to_r2._get_s3_client", return_value=mock_client):
            with patch("livewire_scripts.sync_to_r2._get_bucket", return_value="b"):
                count = upload(bronze_dir)

        assert count == 2
        assert mock_client.upload_file.call_count == 2

    def test_dry_run_does_not_upload(self, tmp_path):
        bronze_dir = tmp_path / "bronze"
        d = bronze_dir / "asset_class=equity" / "symbol=AAPL"
        d.mkdir(parents=True)
        (d / "1d.parquet").write_bytes(b"fake")

        mock_client = MagicMock()
        with patch("livewire_scripts.sync_to_r2._get_s3_client", return_value=mock_client):
            with patch("livewire_scripts.sync_to_r2._get_bucket", return_value="b"):
                count = upload(bronze_dir, dry_run=True)

        assert count == 1
        mock_client.upload_file.assert_not_called()

    def test_missing_dir_returns_zero(self, tmp_path):
        count = upload(tmp_path / "nonexistent")
        assert count == 0


class TestDownload:
    def test_downloads_parquet_files(self, tmp_path):
        bronze_dir = tmp_path / "bronze"

        mock_client = MagicMock()
        mock_client.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": "bronze/asset_class=equity/symbol=AAPL/1d.parquet"}]}
        ]

        with patch("livewire_scripts.sync_to_r2._get_s3_client", return_value=mock_client):
            with patch("livewire_scripts.sync_to_r2._get_bucket", return_value="b"):
                count = download(bronze_dir)

        assert count == 1
        mock_client.download_file.assert_called_once()

    def test_skips_non_parquet_keys(self, tmp_path):
        bronze_dir = tmp_path / "bronze"

        mock_client = MagicMock()
        mock_client.get_paginator.return_value.paginate.return_value = [
            {"Contents": [
                {"Key": "bronze/asset_class=equity/symbol=AAPL/1d.parquet"},
                {"Key": "bronze/asset_class=equity/symbol=AAPL/metadata.json"},
            ]}
        ]

        with patch("livewire_scripts.sync_to_r2._get_s3_client", return_value=mock_client):
            with patch("livewire_scripts.sync_to_r2._get_bucket", return_value="b"):
                count = download(bronze_dir)

        assert count == 1

    def test_dry_run_does_not_download(self, tmp_path):
        bronze_dir = tmp_path / "bronze"

        mock_client = MagicMock()
        mock_client.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": "bronze/asset_class=equity/symbol=AAPL/1d.parquet"}]}
        ]

        with patch("livewire_scripts.sync_to_r2._get_s3_client", return_value=mock_client):
            with patch("livewire_scripts.sync_to_r2._get_bucket", return_value="b"):
                count = download(bronze_dir, dry_run=True)

        assert count == 1
        mock_client.download_file.assert_not_called()

    def test_empty_bucket(self, tmp_path):
        bronze_dir = tmp_path / "bronze"

        mock_client = MagicMock()
        mock_client.get_paginator.return_value.paginate.return_value = [
            {"Contents": []}
        ]

        with patch("livewire_scripts.sync_to_r2._get_s3_client", return_value=mock_client):
            with patch("livewire_scripts.sync_to_r2._get_bucket", return_value="b"):
                count = download(bronze_dir)

        assert count == 0


class TestMain:
    def test_upload_mode(self, tmp_path):
        with patch("livewire_scripts.sync_to_r2.upload", return_value=5) as mock_upload:
            rc = main(["--upload", "--data-lake", str(tmp_path)])

        assert rc == 0
        mock_upload.assert_called_once()

    def test_download_mode(self, tmp_path):
        with patch("livewire_scripts.sync_to_r2.download", return_value=3) as mock_download:
            rc = main(["--download", "--data-lake", str(tmp_path)])

        assert rc == 0
        mock_download.assert_called_once()

    def test_dry_run_flag(self, tmp_path):
        with patch("livewire_scripts.sync_to_r2.upload", return_value=1) as mock_upload:
            main(["--upload", "--dry-run", "--data-lake", str(tmp_path)])

        mock_upload.assert_called_once_with(tmp_path / "bronze", dry_run=True)


class TestMultiTimeframeSync:
    def test_uploads_all_three_timeframes(self, tmp_path):
        bronze_dir = tmp_path / "bronze"
        equity_dir = bronze_dir / "asset_class=equity" / "symbol=AAPL"
        equity_dir.mkdir(parents=True)
        (equity_dir / "1d.parquet").write_bytes(b"d")
        (equity_dir / "1h.parquet").write_bytes(b"h")
        (equity_dir / "5m.parquet").write_bytes(b"m")

        # All three filenames should be in PARQUET_FILES_TO_SYNC
        assert "1d.parquet" in PARQUET_FILES_TO_SYNC
        assert "1h.parquet" in PARQUET_FILES_TO_SYNC
        assert "5m.parquet" in PARQUET_FILES_TO_SYNC

        with patch("livewire_scripts.sync_to_r2._get_s3_client") as mock_s3:
            with patch("livewire_scripts.sync_to_r2._get_bucket", return_value="test-bucket"):
                client = MagicMock()
                mock_s3.return_value = client
                count = upload(bronze_dir, dry_run=False)

        assert count == 3
        # Verify upload_file called 3 times with the right keys
        calls = client.upload_file.call_args_list
        keys_uploaded = [c[0][2] for c in calls]
        assert any("1d.parquet" in k for k in keys_uploaded)
        assert any("1h.parquet" in k for k in keys_uploaded)
        assert any("5m.parquet" in k for k in keys_uploaded)
        assert all("symbol=AAPL" in k for k in keys_uploaded)

    def test_download_accepts_all_three_timeframes(self, tmp_path):
        bronze_dir = tmp_path / "bronze"

        mock_client = MagicMock()
        mock_client.get_paginator.return_value.paginate.return_value = [
            {"Contents": [
                {"Key": "bronze/asset_class=equity/symbol=AAPL/1d.parquet"},
                {"Key": "bronze/asset_class=equity/symbol=AAPL/1h.parquet"},
                {"Key": "bronze/asset_class=equity/symbol=AAPL/5m.parquet"},
                {"Key": "bronze/asset_class=equity/symbol=AAPL/metadata.json"},
            ]}
        ]

        with patch("livewire_scripts.sync_to_r2._get_s3_client", return_value=mock_client):
            with patch("livewire_scripts.sync_to_r2._get_bucket", return_value="b"):
                count = download(bronze_dir)

        assert count == 3
        assert mock_client.download_file.call_count == 3
