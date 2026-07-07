from __future__ import annotations

import json
import os
import sys
import tarfile
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List

import pytest


os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
TRIGGER_SRC = Path(__file__).resolve().parents[1] / "trigger" / "src"
sys.modules.pop("activity", None)
sys.modules.pop("schemas", None)
sys.modules.pop("composites", None)
sys.modules.pop("trigger_handler", None)
sys.path.insert(0, str(TRIGGER_SRC))

import trigger_handler  # noqa: E402

sys.path.remove(str(TRIGGER_SRC))
sys.modules.pop("activity", None)
sys.modules.pop("schemas", None)
sys.modules.pop("composites", None)


def _track(track_id: str, timestamp: str = "163315") -> Dict[str, Any]:
    return {
        "track_id": track_id,
        "timestamp": timestamp,
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


def _results_json(source_device: str, track_id: str) -> bytes:
    return json.dumps(
        {
            "source_device": source_device,
            "date": "20260412",
            "tracks": [_track(track_id)],
        }
    ).encode("utf-8")


def _build_tar(members: Dict[str, bytes]) -> bytes:
    buffer = BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:") as tar:
        for name, data in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tar.addfile(info, BytesIO(data))
    return buffer.getvalue()


class ArchiveOnlyStorage:
    """Fallback storage backing a TarStorageAdapter: no manifest, no composites."""

    def __init__(self, archive_key: str, archive_bytes: bytes) -> None:
        self._archive_key = archive_key
        self._archive_bytes = archive_bytes

    def read_bytes(self, bucket: str, key: str) -> bytes:
        assert key == self._archive_key
        return self._archive_bytes

    def read_text(self, bucket: str, key: str, *, version_id=None, etag=None) -> str:
        raise FileNotFoundError(key)

    def read_json(self, bucket: str, key: str, *, version_id=None, etag=None) -> Dict[str, Any]:
        raise FileNotFoundError(key)

    def exists(self, bucket: str, key: str) -> bool:
        return False

    def list_keys(self, bucket: str, prefix: str, suffix: str = "") -> List[str]:
        return []


class CountingCollectingWriter(trigger_handler.CollectingWriter):
    """A CollectingWriter that also records how many times each put_* was called."""

    def __init__(self) -> None:
        super().__init__()
        self.call_counts: Dict[str, int] = {
            "tracks": 0,
            "classifications": 0,
            "devices": 0,
            "videos": 0,
            "heartbeats": 0,
            "environmental_readings": 0,
        }

    def put_tracks(self, items):
        self.call_counts["tracks"] += 1
        super().put_tracks(items)

    def put_classifications(self, items):
        self.call_counts["classifications"] += 1
        super().put_classifications(items)

    def put_devices_if_missing(self, items):
        self.call_counts["devices"] += 1
        super().put_devices_if_missing(items)

    def put_videos(self, items):
        self.call_counts["videos"] += 1
        super().put_videos(items)

    def put_heartbeats(self, items):
        self.call_counts["heartbeats"] += 1
        super().put_heartbeats(items)

    def put_environmental_readings(self, items):
        self.call_counts["environmental_readings"] += 1
        super().put_environmental_readings(items)


def test_process_archive_object_flushes_writes_once_for_whole_archive():
    archive_key = "v2/archives/2026/07/07/12/batch.tar"
    members = {
        "v1/device-1/20260412/results.json": _results_json("device-1", "1001"),
        "v1/device-2/20260412/results.json": _results_json("device-2", "2002"),
    }
    archive_bytes = _build_tar(members)
    storage = ArchiveOnlyStorage(archive_key, archive_bytes)
    writer = CountingCollectingWriter()

    summary = trigger_handler.process_archive_object(storage, writer, "bucket-1", archive_key)

    assert summary["result_objects"] == 2
    assert summary["skipped_members"] == 0
    # One flush call per table for the whole archive, not one per member.
    assert writer.call_counts == {
        "tracks": 1,
        "classifications": 1,
        "devices": 1,
        "videos": 1,
        "heartbeats": 1,
        "environmental_readings": 1,
    }
    # Both members' records landed in that single call.
    assert sorted(item["track_id"] for item in writer.tracks) == ["1001_163315", "2002_163315"]
    assert {item["device_id"] for item in writer.devices} == {"device-1", "device-2"}


def test_process_archive_object_isolates_per_member_parse_failures():
    archive_key = "v2/archives/2026/07/07/12/batch.tar"
    bad_results = json.dumps({"date": "20260412", "tracks": [_track("9999")]}).encode("utf-8")
    members = {
        # Missing "source_device" -> _resolve_devices raises KeyError for this member only.
        "v1/device-bad/20260412/results.json": bad_results,
        "v1/device-good/20260412/results.json": _results_json("device-good", "1001"),
    }
    archive_bytes = _build_tar(members)
    storage = ArchiveOnlyStorage(archive_key, archive_bytes)
    writer = CountingCollectingWriter()

    summary = trigger_handler.process_archive_object(storage, writer, "bucket-1", archive_key)

    assert summary["skipped_members"] == 1
    assert summary["result_objects"] == 1
    # The failing member contributed nothing; the good member's write still went through.
    assert writer.call_counts["tracks"] == 1
    assert [item["device_id"] for item in writer.tracks] == ["device-good"]


def test_dynamo_writer_dedupes_batches_by_table_primary_key(monkeypatch):
    """Records from different archive members can share a primary key (e.g. two
    videos whose derived timestamps land on the same second). They now meet in
    one batch_writer context, and DynamoDB rejects a BatchWriteItem request
    holding duplicate keys — so every batched put must dedupe via
    overwrite_by_pkeys matching the table's key schema (terraform/dynamodb.tf)."""

    class StubBatch:
        def __init__(self) -> None:
            self.items: List[Dict[str, Any]] = []

        def put_item(self, Item: Dict[str, Any]) -> None:
            self.items.append(Item)

        def __enter__(self) -> "StubBatch":
            return self

        def __exit__(self, *exc_info: object) -> None:
            pass

    class StubTable:
        def __init__(self, name: str) -> None:
            self.name = name
            self.pkeys: List[str] | None = None

        def batch_writer(self, overwrite_by_pkeys=None) -> StubBatch:
            self.pkeys = overwrite_by_pkeys
            return StubBatch()

    class StubResource:
        def Table(self, name: str) -> StubTable:
            return StubTable(name)

    monkeypatch.setattr(trigger_handler.boto3, "resource", lambda service: StubResource())
    writer = trigger_handler.DynamoWriter()

    writer.put_tracks([])
    writer.put_classifications([])
    writer.put_videos([])
    writer.put_heartbeats([])
    writer.put_environmental_readings([])

    assert writer.tracks.pkeys == ["track_id", "device_id"]
    assert writer.classifications.pkeys == ["device_id", "timestamp"]
    assert writer.videos.pkeys == ["device_id", "timestamp"]
    assert writer.heartbeats.pkeys == ["device_id", "timestamp"]
    assert writer.environmental_readings.pkeys == ["device_id", "timestamp"]


def test_process_archive_object_propagates_flush_failure_for_whole_archive():
    """A failure during the final flush is not swallowed per-member: it aborts and
    propagates so the caller (process_s3_object) retries the whole archive, rather
    than silently dropping the failed table's writes the way a per-member failure
    would have been swallowed and logged as 'archive_member_failed'."""
    archive_key = "v2/archives/2026/07/07/12/batch.tar"
    members = {
        "v1/device-1/20260412/results.json": _results_json("device-1", "1001"),
    }
    archive_bytes = _build_tar(members)
    storage = ArchiveOnlyStorage(archive_key, archive_bytes)

    class FailingWriter(trigger_handler.CollectingWriter):
        def put_tracks(self, items):
            raise RuntimeError("dynamodb write failed")

    writer = FailingWriter()

    with pytest.raises(RuntimeError, match="dynamodb write failed"):
        trigger_handler.process_archive_object(storage, writer, "bucket-1", archive_key)
