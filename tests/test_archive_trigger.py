from __future__ import annotations

import io
import json
import os
import sys
import tarfile
from pathlib import Path

import pytest
from PIL import Image


os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
TRIGGER_SRC = Path(__file__).resolve().parents[1] / "trigger" / "src"
for _name in ("activity", "schemas", "composites", "trigger_handler"):
    sys.modules.pop(_name, None)
sys.path.insert(0, str(TRIGGER_SRC))

import trigger_handler  # noqa: E402

sys.path.remove(str(TRIGGER_SRC))


class MemoryStorage:
    """In-memory StorageAdapter, mirroring the one in test_trigger_logging."""

    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects: dict[str, bytes] = dict(objects or {})

    def read_text(self, bucket, key, *, version_id=None, etag=None) -> str:
        return self.objects[key].decode("utf-8")

    def read_json(self, bucket, key, *, version_id=None, etag=None) -> dict:
        return json.loads(self.read_text(bucket, key))

    def read_bytes(self, bucket, key) -> bytes:
        return self.objects[key]

    def write_bytes(self, bucket, key, body, content_type) -> None:
        self.objects[key] = body

    def exists(self, bucket, key) -> bool:
        return key in self.objects

    def list_keys(self, bucket, prefix, suffix="") -> list[str]:
        return sorted(k for k in self.objects if k.startswith(prefix) and k.endswith(suffix))


@pytest.fixture(autouse=True)
def _silence_activity(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in dir(trigger_handler.activity):
        if name.startswith("record_"):
            monkeypatch.setattr(trigger_handler.activity, name, lambda *a, **k: None)


def _jpeg_bytes() -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (10, 10), "white").save(buffer, format="JPEG")
    return buffer.getvalue()


def _make_tar(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as tar:
        for name, body in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))
    return buffer.getvalue()


# One DOT track that requires server-side composite generation — same shape as the
# golden case in test_trigger_logging, so we can assert archive == direct equivalence.
RESULTS_KEY = "v1/FLIK2-dot01/20260412/results.json"
LABELS_KEY = "v1/FLIK2-dot01/20260412/labels/12224.json"
CROP_KEY = "v1/FLIK2-dot01/20260412/crops/12224_163315/frame_000000.jpg"
COMPOSITE_KEY = "v1/FLIK2-dot01/20260412/composites/12224_163315.jpg"

RESULTS_BODY = json.dumps(
    {
        "source_device": "FLIK2-dot01",
        "date": "20260412",
        "tracks": [
            {
                "track_id": "12224",
                "timestamp": "163315",
                "final_prediction": {
                    "family": "Family",
                    "genus": "Genus",
                    "species": "Species",
                    "family_confidence": 0.9,
                    "genus_confidence": 0.8,
                    "species_confidence": 0.7,
                },
                "num_detections": 1,
                "frames": [],
            }
        ],
    }
).encode("utf-8")

LABELS_BODY = json.dumps(
    {
        "resolution": {"width": 100, "height": 80},
        "points": [{"x": 10, "y": 20, "width": 10, "height": 10, "frameIndex": 1505}],
    }
).encode("utf-8")


def _unpacked_members() -> dict[str, bytes]:
    return {LABELS_KEY: LABELS_BODY, CROP_KEY: _jpeg_bytes(), RESULTS_KEY: RESULTS_BODY}


def test_archive_unpacks_to_canonical_keys_and_processes_results() -> None:
    archive_key = "v2/archives/FLIK2-dot01/20260412_160000.tar"
    storage = MemoryStorage({archive_key: _make_tar(_unpacked_members())})
    writer = trigger_handler.CollectingWriter()

    summary = trigger_handler.process_archive_object(storage, writer, "bucket", archive_key)

    # Media + results.json materialised at their canonical S3 keys.
    assert storage.exists("bucket", LABELS_KEY)
    assert storage.exists("bucket", CROP_KEY)
    assert storage.exists("bucket", RESULTS_KEY)
    # Server-side composite generated for the DOT track.
    assert storage.exists("bucket", COMPOSITE_KEY)
    # Records produced.
    assert summary["archives"] == 1
    assert summary["result_objects"] == 1
    assert summary["tracks"] == 1
    assert summary["composites_created"] == 1
    assert writer.tracks[0]["track_id"] == "12224_163315"
    assert writer.tracks[0]["composite_key"] == COMPOSITE_KEY


def test_archive_path_matches_direct_upload_path() -> None:
    archive_key = "v2/archives/FLIK2-dot01/20260412_160000.tar"
    archive_storage = MemoryStorage({archive_key: _make_tar(_unpacked_members())})
    archive_writer = trigger_handler.CollectingWriter()
    trigger_handler.process_archive_object(archive_storage, archive_writer, "bucket", archive_key)

    direct_storage = MemoryStorage(_unpacked_members())
    direct_writer = trigger_handler.CollectingWriter()
    trigger_handler.process_results_object(direct_storage, direct_writer, "bucket", RESULTS_KEY)

    assert archive_writer.tracks == direct_writer.tracks
    assert archive_writer.classifications == direct_writer.classifications

    # Device records carry a wall-clock `created` set at process time, so compare on the
    # stable identity fields only.
    def _device_identity(records):
        return sorted((r["device_id"], r.get("parent_device_id")) for r in records)

    assert _device_identity(archive_writer.devices) == _device_identity(direct_writer.devices)


def test_archive_skips_sidecars_and_unsafe_members() -> None:
    archive_key = "v2/archives/dev/20260412_160000.tar"
    members = _unpacked_members()
    members["v1/FLIK2-dot01/20260412/.done"] = b"classified=1\nexpected=1\n"
    members["v1/FLIK2-dot01/20260412/.detection.json"] = b"{}"
    members["../escape.txt"] = b"nope"
    members["v1/FLIK2-dot01/20260412/../../etc/passwd"] = b"nope"
    storage = MemoryStorage({archive_key: _make_tar(members)})
    writer = trigger_handler.CollectingWriter()

    summary = trigger_handler.process_archive_object(storage, writer, "bucket", archive_key)

    assert not storage.exists("bucket", "v1/FLIK2-dot01/20260412/.done")
    assert not storage.exists("bucket", "v1/FLIK2-dot01/20260412/.detection.json")
    assert not storage.exists("bucket", "../escape.txt")
    assert summary["skipped_members"] >= 2
    # Legit track still processed.
    assert summary["tracks"] == 1


def test_archive_routes_heartbeat_and_environment_members() -> None:
    archive_key = "v2/archives/dev/20260412_160000.tar"
    heartbeat_key = "v1/dev/heartbeats/20260412_160000.json"
    environment_key = "v1/dev/environment/20260412_160000.json"
    members = {
        heartbeat_key: json.dumps({"device_id": "dev", "timestamp": "2026-04-12T16:00:00"}).encode(),
        environment_key: json.dumps({"device_id": "dev", "timestamp": "2026-04-12T16:00:00", "temperature": 21.5}).encode(),
    }
    storage = MemoryStorage({archive_key: _make_tar(members)})
    writer = trigger_handler.CollectingWriter()

    summary = trigger_handler.process_archive_object(storage, writer, "bucket", archive_key)

    assert storage.exists("bucket", heartbeat_key)
    assert storage.exists("bucket", environment_key)
    assert summary["heartbeats"] == 1
    assert summary["environmental_readings"] == 1
    assert len(writer.heartbeats) == 1
    assert len(writer.environmental_readings) == 1


def test_archive_continues_after_poison_results_member() -> None:
    archive_key = "v2/archives/dev/20260412_160000.tar"
    good = _unpacked_members()
    poison_key = "v1/FLIK2-dot01/20260413/results.json"
    members = dict(good)
    members[poison_key] = b"{ this is not valid json"
    storage = MemoryStorage({archive_key: _make_tar(members)})
    writer = trigger_handler.CollectingWriter()

    summary = trigger_handler.process_archive_object(storage, writer, "bucket", archive_key)

    # Malformed results.json is handled inside process_results_object (records malformed,
    # returns empty) rather than aborting the archive — the good track still lands.
    assert summary["tracks"] == 1
    assert summary["result_objects"] == 2


def test_processing_kind_detects_archive() -> None:
    assert trigger_handler._processing_kind("v2/archives/dev/20260412_160000.tar") == trigger_handler.ProcessingKind.ARCHIVE
    assert trigger_handler._processing_kind("v1/dev/20260412/results.json") == trigger_handler.ProcessingKind.RESULTS
    # A .tar outside v2/archives/ is not an archive (and stays ignored).
    assert trigger_handler._processing_kind("v1/dev/x.tar") == trigger_handler.ProcessingKind.IGNORED
    assert trigger_handler._processing_kind("v2/other/x.tar") == trigger_handler.ProcessingKind.IGNORED


def _event(key: str) -> "trigger_handler.S3ObjectEvent":
    # No etag/version -> idempotency is UNAVAILABLE, so process_s3_object runs the
    # body without needing a DynamoDB ledger table.
    return trigger_handler.S3ObjectEvent(bucket="bucket", key=key, etag=None, version_id=None)


def test_process_s3_object_routes_v2_archive_past_v1_guard() -> None:
    # The end-to-end path: a v2/archives tar must reach the archive processor even
    # though it is not under v1/ (the guard previously dropped all non-v1 keys).
    archive_key = "v2/archives/FLIK2-dot01/20260412_160000.tar"
    storage = MemoryStorage({archive_key: _make_tar(_unpacked_members())})
    writer = trigger_handler.CollectingWriter()
    store = trigger_handler.ProcessedObjectStore(table_name="")

    summary = trigger_handler.process_s3_object(storage, writer, _event(archive_key), store)

    assert summary.get("archives") == 1
    assert summary.get("tracks") == 1
    assert storage.exists("bucket", RESULTS_KEY)  # exploded to canonical key
    assert writer.tracks[0]["track_id"] == "12224_163315"


def test_process_s3_object_ignores_non_archive_v2_key() -> None:
    storage = MemoryStorage({"v2/archives/dev/stray.json": b"{}"})
    writer = trigger_handler.CollectingWriter()
    store = trigger_handler.ProcessedObjectStore(table_name="")

    summary = trigger_handler.process_s3_object(storage, writer, _event("v2/archives/dev/stray.json"), store)

    assert summary == {}
    assert writer.tracks == []


def test_unsafe_member_validation() -> None:
    assert trigger_handler._is_safe_archive_member("v1/dev/20260412/results.json")
    assert not trigger_handler._is_safe_archive_member("../escape")
    assert not trigger_handler._is_safe_archive_member("/abs/path")
    assert not trigger_handler._is_safe_archive_member("v1/dev/../../etc/passwd")
    assert not trigger_handler._is_safe_archive_member("outside/v1/results.json")
    assert not trigger_handler._is_safe_archive_member("v1/dev/20260412/.done")
