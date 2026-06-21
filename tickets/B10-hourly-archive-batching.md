# B10 — Hourly archive batching (option 2b: outer tar)

## Goal

Reduce the device → S3 upload frequency/request-count to **once per hour** by having
the bugcam upload step package each hour's already-complete per-`{datetime}` output
directories into a single **outer tar**, uploaded as one object. The backend trigger
Lambda is rewritten to unpack that archive back into the **exact canonical S3 layout**
and then run the existing per-`results.json` processing unchanged.

Key principle: **the tar is purely a transport + trigger optimisation.** After the Lambda
unpacks it, the S3 state (crops, composites, videos, labels, results.json, heartbeats,
environment) is byte-for-byte what direct per-file upload produces today. So the dashboard,
CSV export, composite serving, and all DynamoDB record shapes stay unchanged.

Multipart **download** in the Lambda is explicitly out of scope for now — the Lambda
downloads the whole archive (`get_object`) and streams extraction from memory/`/tmp`.

## Current state (verified in code)

Device side (`sensing-garden/bugcam`):
- Per-video FLIK output dir `results_dir/{flick_id}/{YYYYMMDD_HHMMSS}/`; per-day DOT dir
  `results_dir/{dot_id}/{YYYYMMDD}/`. Each holds `crops/ composites/ videos/ labels/
  results.json` plus sidecars (`.done .detection.json .expected_tracks .completed_tracks
  .uploaded`).
- `commands/upload.py:watch_uploads` polls every 30s; FLIK dirs upload whole **once
  `.done` exists** then `rmtree`; DOT dirs upload incrementally via `.uploaded` diff.
- `s3_upload.py` uploads each file through a presigned PUT; sidecars are skipped;
  `results.json` is always uploaded **last**.

Backend side (`sensing-garden-backend/trigger/src`):
- `aws_s3_bucket_notification` fires the Lambda on `s3:ObjectCreated:*` for **any**
  `filter_prefix = "v1/"` object (`terraform/lambda.tf:207`). Today crops/composites also
  invoke the Lambda but are IGNORED by `_processing_kind` (wasted invocations).
- `trigger_handler.lambda_handler` → `process_s3_object` → idempotency claim
  (`ProcessedObjectStore`, conditional DynamoDB put keyed on
  `sha256(bucket,key,version/etag)`) → `_processing_kind`:
  - `*/results.json` → `process_results_object` (builds Track + Classification + Device +
    Video records; `ensure_track_composite` reads crops from S3 and writes any missing
    composite back to S3).
  - `v1/{dev}/heartbeats/*.json` → `process_heartbeat_object`.
  - `v1/{dev}/environment/*.json` → `process_environment_object`.
- All media/record keys are resolved relative to `s3_prefix = key without "/results.json"`.

## The recursion problem (why terraform must change)

If the Lambda unpacks an archive and writes `…/results.json` (and crops, etc.) back under
`v1/` while the notification still fires on the whole `v1/` prefix, **every unpacked object
re-invokes the Lambda** — results.json would be reprocessed and crops would fire ignored
no-ops. Therefore the notification filter must be narrowed so that **only the archive object
triggers the Lambda**, and the unpacked canonical objects do not.

Decision: single notification on `filter_prefix = "v1/"`, `filter_suffix = ".tar"`. The
hourly archive must therefore carry **everything** for that hour (result dirs + heartbeats +
environment), because the bare `.json` direct-upload triggers are retired.

> ⚠️ Heartbeat latency tradeoff — batching heartbeats hourly delays device-liveness signal
> by up to an hour. Options: (a) accept hourly liveness; (b) keep heartbeats on a separate
> real-time path writing to a **non-`v1/` prefix** (e.g. `live/…`) with its own notification.
> **Open question for the user — default assumed: (a) archive carries everything.**

## Archive format (contract between device and Lambda)

- **Object key:** `v1/{device_id}/{YYYYMMDD_HH}/batch-{seq}.tar`
  (`{seq}` allows >1 archive per hour if a flush is forced; each is independent.)
- **Compression:** `r:*` autodetect on read; device may use `.tar` or `.tar.gz`. Crops/mp4
  are already compressed, so gzip buys little and costs CPU — default plain `.tar`.
  (Notification `filter_suffix` must then match the real suffix; if gzip is used the key
  must end `.tar` regardless, or add a second `.tar.gz` filter. Recommend: always name the
  object `*.tar`.)
- **Member names = full canonical S3 keys**, e.g. `v1/{device_id}/{datetime}/results.json`,
  `v1/{device_id}/{datetime}/crops/<id>/frame_000123.jpg`. Self-describing and
  location-independent: the Lambda writes each member to its own name.
- **Excluded from the tar:** all sidecars (`.done .detection.json .expected_tracks
  .completed_tracks .uploaded .tmp`). The device only tars dirs that already have `.done`
  (FLIK) / are flushed (DOT).
- **Ordering:** members for a given dir should have `results.json` last (mirrors current
  "results last" invariant); the Lambda also enforces this by deferring results members.

## Device-side changes (bugcam — separate repo, NOT in this Lambda PR)

New aggregation step in `commands/upload.py`, replacing per-dir presigned PUTs for FLIK:
1. Hourly trigger (time-based; replaces the 30s ship-everything loop for FLIK results).
2. Collect all `results_dir/{device}/{datetime}/` dirs that are **ready** (`.done` present),
   plus `heartbeats/ environment/` for the hour.
3. `tarfile` them with arcname = canonical key (above), excluding sidecars.
4. Single presigned-PUT (or direct PUT) of the `.tar` to `v1/{device}/{hour}/batch-{seq}.tar`.
5. On success, `rmtree` the packed dirs (same as today's post-upload cleanup).
DOT incremental upload can either stay as-is or also fold into the hourly tar — recommend
folding for consistency once FLIK lands.

(Full device diff tracked separately; this ticket's PR is the **Lambda + terraform + tests**.)

## Backend-side changes (this PR)

### `trigger/src/trigger_handler.py`
- `ProcessingKind.ARCHIVE = "archive"`; `S3TriggerAction` unchanged.
- `ARCHIVE_SUFFIXES = (".tar",)`; `_processing_kind` returns `ARCHIVE` first when key ends
  with an archive suffix.
- New `process_archive_object(storage, writer, bucket, key, *, version_id, etag)`:
  1. `data = storage.read_bytes(bucket, key)` (whole-archive download).
  2. `tarfile.open(fileobj=BytesIO(data), mode="r:*")`; iterate `getmembers()` files.
  3. **Validate** each member name: must start with `v1/`, no `..`/absolute, no sidecar.
     (Path-traversal guard even though we map to S3 keys, not the local FS.)
  4. **Pass 1 — media:** for every non-`results.json`, non-heartbeat/env member, upload to
     its canonical key via `storage.write_bytes(bucket, name, body, content_type(name))`.
  5. **Pass 2 — results:** for each `…/results.json` member, `write_bytes` it, then call the
     existing `process_results_object(storage, writer, bucket, name)` (reuses composite gen +
     record building unchanged; crops it reads are already in S3 from pass 1).
  6. **Heartbeat/env members:** route via `_processing_kind(name)` to the existing
     `process_heartbeat_object` / `process_environment_object` after `write_bytes`.
  7. Aggregate and return a merged summary (sum of inner summaries + `archives: 1`,
     `result_objects: n`).
- `content_type(name)` helper: `.jpg/.jpeg→image/jpeg`, `.mp4→video/mp4`,
  `.json→application/json`, else `application/octet-stream`.
- Wire `ARCHIVE` into `process_s3_object` alongside the existing kinds. **Idempotency stays
  one claim per archive object** — reprocessing a whole archive is safe because every
  downstream write is deterministic/overwriting (tracks/classifications/videos `put` by
  deterministic key; devices conditional put). Inner objects are **not** separately claimed.
- `_processing_status` already returns "success"/"error"/"empty" from the merged summary;
  extend row-key set if needed.

### `terraform/lambda.tf`
- Add `filter_suffix = ".tar"` to the `output_bucket_notification` lambda_function block.
- Bump Lambda `ephemeral_storage` (e.g. 2–10 GB) and `memory_size`/`timeout` to fit an
  hour of crops in `/tmp`/memory during extraction. (Confirm typical hourly tar size.)

### Tests (`tests/`)
- `LocalStorageAdapter` already enables hermetic processing. Add:
  - `test_archive_trigger.py`: build an in-memory tar of a known `{datetime}/results.json`
    + crops + labels (reuse existing FLIK + DOT fixtures), run `process_archive_object`
    against a `LocalStorageAdapter`, assert the unpacked keys exist and the produced
    Track/Classification/Video/Device records match the **direct-upload** path exactly
    (golden-equivalence: archive path ≡ per-file path).
  - Path-traversal/`..` member rejected.
  - Sidecar members skipped (not written, not processed).
  - Idempotency: second invocation on same archive object → `skipped_duplicate`.
  - Heartbeat/environment member routed and written.

## Rollout / sequencing

1. Land Lambda + terraform + tests (archive path **added**, old per-file path still works
   because `_processing_kind` still recognises bare `results.json`). Deploy.
2. Apply terraform notification `filter_suffix=".tar"` — switches the trigger to archives.
   (Brief window: do this only once devices start uploading tars.)
3. Ship the bugcam aggregation step; flip devices to archive upload.
4. Retire the 30s per-file FLIK upload loop.

## Risks / open questions
- **Heartbeat latency** (see above) — needs user decision.
- **Archive size vs Lambda `/tmp`** — measure; may need 10 GB ephemeral or streaming
  member-by-member `get_object` range reads later (the deferred multipart-download work).
- **Partial-archive failure** — whole archive is retried; safe due to idempotent writes, but
  a single poison inner results.json fails the whole archive. Consider per-results
  try/except that records `activity` failure and continues (matches current per-track
  resilience) rather than aborting the archive. **Recommended: continue-on-inner-error.**
