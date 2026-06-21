"""Index (range-read) ingestion of hourly batch archives.

Unlike the explode approach, media stays inside the tar: a tar-backed
StorageAdapter feeds the existing processors so the same DynamoDB rows are
written, and each row is stamped with the archive location + the member's byte
range so a serve path can range-read instead of fetching a standalone object.
"""
from __future__ import annotations

import io
import json
import os
import sys
import tarfile
from pathlib import Path

from PIL import Image


os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
TRIGGER_SRC = Path(__file__).resolve().parents[1] / "trigger" / "src"
for _name in ("activity", "schemas", "composites", "trigger_handler"):
    sys.modules.pop(_name, None)
sys.path.insert(0, str(TRIGGER_SRC))

import trigger_handler  # noqa: E402

sys.path.remove(str(TRIGGER_SRC))


import pytest  # noqa: E402


class MemoryStorage:
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


def _jpeg_bytes(color: str = "white") -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (10, 10), color).save(buffer, format="JPEG")
    return buffer.getvalue()


def _make_tar(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as tar:  # uncompressed -> stable offsets
        for name, body in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))
    return buffer.getvalue()


ARCHIVE_KEY = "v2/archives/FLIK2-dot01/20260412_160000.tar"
PREFIX = "v1/FLIK2-dot01/20260412"
CROP_KEY = f"{PREFIX}/crops/12224_163315/frame_000000.jpg"
COMPOSITE_KEY = f"{PREFIX}/composites/12224_163315.jpg"
LABELS_KEY = f"{PREFIX}/labels/12224.json"
RESULTS_KEY = f"{PREFIX}/results.json"

CROP_BYTES = _jpeg_bytes("red")
COMPOSITE_BYTES = _jpeg_bytes("blue")
LABELS_BYTES = json.dumps(
    {"resolution": {"width": 100, "height": 80},
     "points": [{"x": 10, "y": 20, "width": 10, "height": 10, "frameIndex": 0}]}
).encode("utf-8")

_PRED = {
    "family": "Family", "genus": "Genus", "species": "Species",
    "family_confidence": 0.9, "genus_confidence": 0.8, "species_confidence": 0.7,
}
RESULTS_BODY = json.dumps(
    {
        "source_device": "FLIK2-dot01",
        "date": "20260412",
        "tracks": [
            {
                "track_id": "12224",
                "timestamp": "163315",
                "final_prediction": _PRED,
                "num_detections": 1,
                "frames": [{"frame_number": 0, "prediction": _PRED, "bbox": [1.0, 2.0, 3.0, 4.0]}],
            }
        ],
    }
).encode("utf-8")


def _members() -> dict[str, bytes]:
    # The device supplies the composite, so it stays in the tar (not regenerated).
    return {CROP_KEY: CROP_BYTES, COMPOSITE_KEY: COMPOSITE_BYTES, RESULTS_KEY: RESULTS_BODY}


def _slice(tar_bytes: bytes, item: dict, prefix: str) -> bytes:
    return tar_bytes[item[f"{prefix}_offset"]:item[f"{prefix}_offset"] + item[f"{prefix}_size"]]


# --------------------------------------------------------------------------- #
class TestIndexIngestion:
    def test_media_not_exploded_to_s3(self):
        storage = MemoryStorage({ARCHIVE_KEY: _make_tar(_members())})
        writer = trigger_handler.CollectingWriter()

        trigger_handler.process_archive_object(storage, writer, "bucket", ARCHIVE_KEY)

        # The crop/composite are never written out as standalone S3 objects.
        assert CROP_KEY not in storage.objects
        assert COMPOSITE_KEY not in storage.objects
        assert set(storage.objects) == {ARCHIVE_KEY}

    def test_track_carries_composite_byte_range(self):
        tar_bytes = _make_tar(_members())
        storage = MemoryStorage({ARCHIVE_KEY: tar_bytes})
        writer = trigger_handler.CollectingWriter()

        summary = trigger_handler.process_archive_object(storage, writer, "bucket", ARCHIVE_KEY)

        assert summary["tracks"] == 1
        track = writer.tracks[0]
        assert track["composite_key"] == COMPOSITE_KEY
        assert track["archive_key"] == ARCHIVE_KEY
        assert track["archive_bucket"] == "bucket"
        # The stamped range slices the archive back to the exact composite bytes.
        assert _slice(tar_bytes, track, "composite") == COMPOSITE_BYTES

    def test_classification_carries_image_byte_range(self):
        tar_bytes = _make_tar(_members())
        storage = MemoryStorage({ARCHIVE_KEY: tar_bytes})
        writer = trigger_handler.CollectingWriter()

        trigger_handler.process_archive_object(storage, writer, "bucket", ARCHIVE_KEY)

        assert len(writer.classifications) == 1
        clf = writer.classifications[0]
        assert clf["image_key"] == CROP_KEY
        assert clf["archive_key"] == ARCHIVE_KEY
        assert _slice(tar_bytes, clf, "image") == CROP_BYTES

    def test_rows_match_direct_processing(self):
        archive_storage = MemoryStorage({ARCHIVE_KEY: _make_tar(_members())})
        archive_writer = trigger_handler.CollectingWriter()
        trigger_handler.process_archive_object(archive_storage, archive_writer, "bucket", ARCHIVE_KEY)

        direct_storage = MemoryStorage(_members())
        direct_writer = trigger_handler.CollectingWriter()
        trigger_handler.process_results_object(direct_storage, direct_writer, "bucket", RESULTS_KEY)

        # Identical on the content fields; the index path only adds archive coords.
        def _core(rows):
            return [{k: r[k] for k in ("track_id", "family", "genus", "species")} for r in rows]

        assert _core(archive_writer.tracks) == _core(direct_writer.tracks)
        assert archive_writer.tracks[0]["composite_key"] == direct_writer.tracks[0]["composite_key"]
        assert "archive_key" not in direct_writer.tracks[0]  # direct path is unchanged

    def test_generated_composite_falls_back_to_flat_key(self):
        # No composite supplied -> backend generates it and writes to S3; that row
        # is not a tar member, so it is left unstamped (serves from its flat key).
        members = {CROP_KEY: CROP_BYTES, LABELS_KEY: LABELS_BYTES, RESULTS_KEY: RESULTS_BODY}
        storage = MemoryStorage({ARCHIVE_KEY: _make_tar(members)})
        writer = trigger_handler.CollectingWriter()

        summary = trigger_handler.process_archive_object(storage, writer, "bucket", ARCHIVE_KEY)

        assert summary["composites_created"] == 1
        assert COMPOSITE_KEY in storage.objects  # generated composite written to S3
        assert "archive_key" not in writer.tracks[0]  # not stamped


# --------------------------------------------------------------------------- #
class TestRouting:
    def _event(self, key: str):
        return trigger_handler.S3ObjectEvent(bucket="bucket", key=key, etag=None, version_id=None)

    def test_processing_kind_detects_v2_archive(self):
        assert trigger_handler._processing_kind(ARCHIVE_KEY) == trigger_handler.ProcessingKind.ARCHIVE
        assert trigger_handler._processing_kind("v1/d/x.tar") == trigger_handler.ProcessingKind.IGNORED

    def test_process_s3_object_routes_archive_without_exploding(self):
        storage = MemoryStorage({ARCHIVE_KEY: _make_tar(_members())})
        writer = trigger_handler.CollectingWriter()
        store = trigger_handler.ProcessedObjectStore(table_name="")

        summary = trigger_handler.process_s3_object(storage, writer, self._event(ARCHIVE_KEY), store)

        assert summary.get("archives") == 1
        assert summary.get("tracks") == 1
        assert CROP_KEY not in storage.objects  # indexed in place, not exploded
        assert writer.tracks[0]["archive_key"] == ARCHIVE_KEY
