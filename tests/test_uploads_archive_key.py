"""The /upload-url validators must accept v2/archives/<device>/... archive keys
(not just v1/) and scope them to the right device, so a device can get a
presigned URL to upload its hourly batch tar.
"""
import pytest

from routes.uploads import _validate_device_scope, _validate_s3_key


class TestValidateKey:
    def test_v1_allowed(self):
        assert _validate_s3_key("v1/dev/20260101/results.json") == "v1/dev/20260101/results.json"

    def test_v2_archive_allowed(self):
        key = "v2/archives/dev/20260101_120000.tar"
        assert _validate_s3_key(key) == key

    def test_other_prefix_rejected(self):
        with pytest.raises(ValueError):
            _validate_s3_key("v3/whatever.tar")

    def test_traversal_rejected(self):
        with pytest.raises(ValueError):
            _validate_s3_key("v2/archives/dev/../escape.tar")


class TestDeviceScope:
    def _device(self):
        return {"device_id": "FLIK2", "dot_ids": ["dot1", "dot2"]}

    def test_v2_archive_in_scope(self):
        _validate_device_scope("v2/archives/dot1/20260101_120000.tar", self._device())  # no raise

    def test_v2_archive_out_of_scope(self):
        with pytest.raises(PermissionError):
            _validate_device_scope("v2/archives/stranger/x.tar", self._device())

    def test_v2_archive_missing_device(self):
        with pytest.raises(PermissionError):
            _validate_device_scope("v2/archives/", self._device())

    def test_v1_scope_still_works(self):
        _validate_device_scope("v1/FLIK2/20260101_120000/results.json", self._device())
        with pytest.raises(PermissionError):
            _validate_device_scope("v1/stranger/x/results.json", self._device())
