"""Presigned multipart endpoints: lifecycle, key/device validation, and the
404-when-gone behaviour the device relies on to restart a reaped upload.
"""
import json

import pytest
from botocore.exceptions import ClientError

import auth
from routes import multipart


DEVICE = {"device_id": "FLIK2", "dot_ids": ["dot1"]}


def _event(body):
    return {"body": json.dumps(body)}


class FakeS3:
    def __init__(self, *, gone=False):
        self.gone = gone
        self.calls = []

    def create_multipart_upload(self, **kw):
        self.calls.append(("create", kw))
        return {"UploadId": "UP-1"}

    def generate_presigned_url(self, op, Params=None, ExpiresIn=None):
        self.calls.append(("part-url", op, Params))
        return f"https://s3/part/{Params['PartNumber']}"

    def complete_multipart_upload(self, **kw):
        self.calls.append(("complete", kw))
        if self.gone:
            raise ClientError({"Error": {"Code": "NoSuchUpload"}}, "CompleteMultipartUpload")

    def abort_multipart_upload(self, **kw):
        self.calls.append(("abort", kw))
        if self.gone:
            raise ClientError({"Error": {"Code": "NoSuchUpload"}}, "AbortMultipartUpload")


@pytest.fixture
def fake_s3(monkeypatch):
    client = FakeS3()
    monkeypatch.setattr(multipart, "_client", lambda: client)
    return client


def _body(resp):
    return json.loads(resp["body"])


class TestLifecycle:
    def test_create(self, fake_s3):
        resp = multipart.handle_multipart_create(
            _event({"s3_key": "v2/archives/FLIK2/20260101_120000.tar"}), authenticated_device=DEVICE
        )
        assert resp["statusCode"] == 200
        assert _body(resp)["upload_id"] == "UP-1"

    def test_part_url(self, fake_s3):
        resp = multipart.handle_multipart_part_url(
            _event({"s3_key": "v2/archives/FLIK2/x.tar", "upload_id": "UP-1", "part_number": 3}),
            authenticated_device=DEVICE,
        )
        assert resp["statusCode"] == 200
        assert _body(resp)["upload_url"].endswith("/3")

    def test_complete_orders_parts(self, fake_s3):
        resp = multipart.handle_multipart_complete(
            _event({
                "s3_key": "v2/archives/FLIK2/x.tar", "upload_id": "UP-1",
                "parts": [{"part_number": 2, "etag": "b"}, {"part_number": 1, "etag": "a"}],
            }),
            authenticated_device=DEVICE,
        )
        assert resp["statusCode"] == 200
        sent = [c for c in fake_s3.calls if c[0] == "complete"][0][1]["MultipartUpload"]["Parts"]
        assert sent == [{"ETag": "a", "PartNumber": 1}, {"ETag": "b", "PartNumber": 2}]

    def test_abort(self, fake_s3):
        resp = multipart.handle_multipart_abort(
            _event({"s3_key": "v2/archives/FLIK2/x.tar", "upload_id": "UP-1"}), authenticated_device=DEVICE
        )
        assert resp["statusCode"] == 200


class TestGone:
    def test_complete_gone_is_404(self, monkeypatch):
        monkeypatch.setattr(multipart, "_client", lambda: FakeS3(gone=True))
        resp = multipart.handle_multipart_complete(
            _event({"s3_key": "v2/archives/FLIK2/x.tar", "upload_id": "UP-1", "parts": [{"part_number": 1, "etag": "a"}]}),
            authenticated_device=DEVICE,
        )
        assert resp["statusCode"] == 404

    def test_abort_gone_is_404(self, monkeypatch):
        monkeypatch.setattr(multipart, "_client", lambda: FakeS3(gone=True))
        resp = multipart.handle_multipart_abort(
            _event({"s3_key": "v2/archives/FLIK2/x.tar", "upload_id": "UP-1"}), authenticated_device=DEVICE
        )
        assert resp["statusCode"] == 404


class TestValidation:
    def test_v1_key_allowed(self, fake_s3):
        resp = multipart.handle_multipart_create(
            _event({"s3_key": "v1/FLIK2/20260101/results.json"}), authenticated_device=DEVICE
        )
        assert resp["statusCode"] == 200

    def test_out_of_scope_device_403(self, fake_s3):
        resp = multipart.handle_multipart_create(
            _event({"s3_key": "v2/archives/stranger/x.tar"}), authenticated_device=DEVICE
        )
        assert resp["statusCode"] == 403

    def test_missing_key_400(self, fake_s3):
        resp = multipart.handle_multipart_create(_event({}), authenticated_device=DEVICE)
        assert resp["statusCode"] == 400

    def test_no_device_403(self, fake_s3):
        resp = multipart.handle_multipart_create(_event({"s3_key": "v1/FLIK2/x/results.json"}), authenticated_device=None)
        assert resp["statusCode"] == 403

    def test_part_number_must_be_int(self, fake_s3):
        resp = multipart.handle_multipart_part_url(
            _event({"s3_key": "v2/archives/FLIK2/x.tar", "upload_id": "UP-1", "part_number": "abc"}),
            authenticated_device=DEVICE,
        )
        assert resp["statusCode"] == 400


def test_routes_are_device_allowed():
    for path in ("/multipart/create", "/multipart/part-url", "/multipart/complete", "/multipart/abort"):
        assert ("POST", path) in auth.DEVICE_ALLOWED_ROUTES
