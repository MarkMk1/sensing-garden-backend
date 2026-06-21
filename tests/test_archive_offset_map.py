"""Metadata-only ingest via the sibling .idx offset map: read member byte ranges
instead of the whole archive, and never fetch the crop bytes.
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
    """In-memory storage that records range vs full reads."""

    def __init__(self, objects):
        self.objects = dict(objects)
        self.full_reads = []
        self.range_reads = []

    def read_text(self, bucket, key, *, version_id=None, etag=None):
        self.full_reads.append(key)
        return self.objects[key].decode("utf-8")

    def read_json(self, bucket, key, *, version_id=None, etag=None):
        self.full_reads.append(key)
        return json.loads(self.objects[key])

    def read_bytes(self, bucket, key):
        self.full_reads.append(key)
        return self.objects[key]

    def read_range(self, bucket, key, offset, size):
        self.range_reads.append((key, offset, size))
        return self.objects[key][offset:offset + size]

    def write_bytes(self, bucket, key, body, content_type):
        self.objects[key] = body

    def exists(self, bucket, key):
        return key in self.objects

    def list_keys(self, bucket, prefix, suffix=""):
        return sorted(k for k in self.objects if k.startswith(prefix) and k.endswith(suffix))


@pytest.fixture(autouse=True)
def _silence_activity(monkeypatch):
    for name in dir(trigger_handler.activity):
        if name.startswith("record_"):
            monkeypatch.setattr(trigger_handler.activity, name, lambda *a, **k: None)


def _jpeg():
    buf = io.BytesIO()
    Image.new("RGB", (8, 8), "red").save(buf, format="JPEG")
    return buf.getvalue()


PREFIX = "v1/FLIK2-dot01/20260412"
ARCHIVE_KEY = "v2/archives/FLIK2-dot01/20260412_160000.tar"
INDEX_KEY = ARCHIVE_KEY + ".idx"
CROP_KEY = f"{PREFIX}/crops/12224_163315/frame_000000.jpg"
COMPOSITE_KEY = f"{PREFIX}/composites/12224_163315.jpg"
LABELS_KEY = f"{PREFIX}/labels/12224.json"
RESULTS_KEY = f"{PREFIX}/results.json"

_PRED = {"family": "F", "genus": "G", "species": "S",
         "family_confidence": 0.9, "genus_confidence": 0.8, "species_confidence": 0.7}
RESULTS_BODY = json.dumps({
    "source_device": "FLIK2-dot01", "date": "20260412",
    "tracks": [{
        "track_id": "12224", "timestamp": "163315", "final_prediction": _PRED, "num_detections": 1,
        "frames": [{"frame_number": 0, "prediction": _PRED, "bbox": [1.0, 2.0, 3.0, 4.0]}],
    }],
}).encode()
LABELS_BODY = json.dumps({"resolution": {"width": 100, "height": 80},
                          "points": [{"x": 1, "y": 2, "width": 3, "height": 4, "frameIndex": 0}]}).encode()


def _members():
    return {RESULTS_KEY: RESULTS_BODY, LABELS_KEY: LABELS_BODY, CROP_KEY: _jpeg(), COMPOSITE_KEY: _jpeg()}


def _make_tar_and_index(members):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, body in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))
    tar_bytes = buf.getvalue()
    index = {}
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:") as tar:
        for m in tar.getmembers():
            if m.isfile():
                index[m.name] = {"offset": m.offset_data, "size": m.size}
    return tar_bytes, json.dumps({"members": index}).encode()


def _storage():
    tar_bytes, idx_bytes = _make_tar_and_index(_members())
    return MemoryStorage({ARCHIVE_KEY: tar_bytes, INDEX_KEY: idx_bytes})


class TestRangedIngest:
    def test_uses_index_and_never_reads_whole_tar(self):
        tar_bytes, idx_bytes = _make_tar_and_index(_members())
        storage = MemoryStorage({ARCHIVE_KEY: tar_bytes, INDEX_KEY: idx_bytes})
        index = json.loads(idx_bytes)["members"]
        writer = trigger_handler.CollectingWriter()

        summary = trigger_handler.process_archive_object(storage, writer, "bucket", ARCHIVE_KEY)

        assert summary["tracks"] == 1
        assert writer.tracks[0]["archive_key"] == ARCHIVE_KEY  # still stamped with coords
        # whole tar never fully read; member bytes come from ranged GETs on the tar
        assert ARCHIVE_KEY not in storage.full_reads
        read_offsets = {offset for (k, offset, size) in storage.range_reads if k == ARCHIVE_KEY}
        assert index[RESULTS_KEY]["offset"] in read_offsets       # results.json range-read
        # crop/composite bytes are NEVER fetched -- only existence-checked via the index
        assert index[CROP_KEY]["offset"] not in read_offsets
        assert index[COMPOSITE_KEY]["offset"] not in read_offsets

    def test_rows_match_full_read_path(self):
        ranged = trigger_handler.CollectingWriter()
        trigger_handler.process_archive_object(_storage(), ranged, "bucket", ARCHIVE_KEY)

        # Same archive without an index -> full-read fallback -> identical core rows.
        tar_bytes, _ = _make_tar_and_index(_members())
        full_storage = MemoryStorage({ARCHIVE_KEY: tar_bytes})  # no .idx
        full = trigger_handler.CollectingWriter()
        trigger_handler.process_archive_object(full_storage, full, "bucket", ARCHIVE_KEY)

        def _core(rows):
            return [{k: r[k] for k in ("track_id", "family", "image_key" if "image_key" in r else "track_id")} for r in rows]

        assert _core(ranged.classifications) == _core(full.classifications)
        assert ARCHIVE_KEY in full_storage.full_reads  # fallback did read the whole tar

    def test_missing_index_falls_back(self):
        tar_bytes, _ = _make_tar_and_index(_members())
        storage = MemoryStorage({ARCHIVE_KEY: tar_bytes})  # no .idx
        writer = trigger_handler.CollectingWriter()
        summary = trigger_handler.process_archive_object(storage, writer, "bucket", ARCHIVE_KEY)
        assert summary["tracks"] == 1
        assert ARCHIVE_KEY in storage.full_reads
