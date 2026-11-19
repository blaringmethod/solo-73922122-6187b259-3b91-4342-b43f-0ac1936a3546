"""Recording engine.

Each job runs a playlist poller (follows the sliding window, keyed on media
sequence numbers), a downloader (persists every expected segment) and a
verifier pool. A segment is hashed (SHA-256) on its first save; whenever the
same sequence number shows up again in a later playlist the verifier
re-fetches that URL *as a stream* and compares hashes. Re-fetches run in
independent tasks, so they never block polling, downloading, stop handling or
the query API.

Verification outcomes per segment:

  unverified      saved, no matching re-fetch yet (or one is in flight)
  verifying       a re-fetch is currently in progress
  verified        a re-fetch reproduced the exact same bytes
  conflict        a re-fetch returned different bytes for the same seq+URL
  no_opportunity  the segment slid out of every later playlist before it
                  could ever be re-fetched -- explicitly NOT consistent

A conflict fails the whole recording and forbids download; conflict and
pending-verification states are persisted and survive restarts.
"""

import asyncio
import hashlib
import logging
import os
import zipfile

import httpx

from .playlist import parse_media_playlist, PlaylistError

log = logging.getLogger("recorder")

MAX_POLL_ERRORS = 15          # consecutive playlist fetch failures before failing the job
MAX_DOWNLOAD_ATTEMPTS = 5     # per segment, transient errors only (404 fails immediately)
MAX_VERIFY_ATTEMPTS = 3       # per re-fetch round, transient errors only
VERIFIER_WORKERS = 2          # concurrent streaming re-fetches per job

# Test-only hooks (active solely when RECORDER_TEST_HOOKS=1):
#   crash_after_write: crash between a segment being written and registered
#   verify_delay:      seconds to sleep while streaming a verification re-fetch
TEST_HOOKS = {"crash_after_write": None, "verify_delay": 0.0}


class SourceError(Exception):
    """The source violates the accepted subset or continuity rules."""


class JobRunner:
    def __init__(self, mgr: "Manager", job_id: int):
        self.mgr = mgr
        self.job_id = job_id
        self.db = mgr.db
        self.wake = asyncio.Event()
        self.poller: asyncio.Task | None = None
        self.downloader: asyncio.Task | None = None
        self.verifiers: list[asyncio.Task] = []
        self.verify_queue: asyncio.Queue = asyncio.Queue()
        self.poll_interval = 1.0

    # ---- paths ----

    @property
    def seg_dir(self) -> str:
        return self.mgr.seg_dir(self.job_id)

    def seg_path(self, seq: int) -> str:
        return os.path.join(self.seg_dir, f"seg_{seq:08d}.ts")

    # ---- lifecycle ----

    def start(self):
        os.makedirs(self.seg_dir, exist_ok=True)
        self._reconcile_disk()
        self.db.reset_stale_verifying(self.job_id)
        job = self.db.get_job(self.job_id)
        if job["state"] == "recording":
            self.poller = asyncio.create_task(self._poll_loop(), name=f"poll-{self.job_id}")
        self.downloader = asyncio.create_task(self._download_loop(), name=f"dl-{self.job_id}")
        for i in range(VERIFIER_WORKERS):
            self.verifiers.append(
                asyncio.create_task(self._verify_loop(i), name=f"verify-{self.job_id}-{i}"))
        # Rebuild the verification queue from persisted state after a restart.
        for seg in self.db.list_unverified_done(self.job_id):
            self._enqueue_verify(seg["seq"])
        log.info("job %s started/resumed in state %s", self.job_id, job["state"])

    def _reconcile_disk(self):
        """Crash recovery: a segment may have been fully written but not yet
        registered (exit between flush and DB update) -> adopt it; a segment
        registered done but missing on disk -> re-download it."""
        for seg in self.db.list_segments(self.job_id):
            path = self.seg_path(seg["seq"])
            if seg["status"] == "done" and not os.path.exists(path):
                log.warning("job %s seq %s: registered but file gone, re-download", self.job_id, seg["seq"])
                self.db.segment_pending(self.job_id, seg["seq"])
            elif seg["status"] == "pending" and os.path.exists(path) and os.path.getsize(path) > 0:
                log.info("job %s seq %s: adopting fully written segment from disk", self.job_id, seg["seq"])
                with open(path, "rb") as fh:
                    digest = hashlib.file_digest(fh, "sha256").hexdigest()
                self.db.segment_done(self.job_id, seg["seq"], path, os.path.getsize(path), digest)
        for name in os.listdir(self.seg_dir):
            if name.endswith(".part"):
                try:
                    os.remove(os.path.join(self.seg_dir, name))
                except OSError:
                    pass

    # ---- playlist polling ----

    async def _poll_loop(self):
        errors = 0
        while True:
            job = self.db.get_job(self.job_id)
            if job["state"] != "recording":
                return
            try:
                await self._poll_once(job)
                errors = 0
            except (SourceError, PlaylistError) as exc:
                self.mgr.fail_job(self.job_id, str(exc))
                return
            except Exception as exc:  # transient fetch/parse trouble: retry
                errors += 1
                log.warning("job %s playlist fetch failed (%s/%s): %s",
                            self.job_id, errors, MAX_POLL_ERRORS, exc)
                if errors >= MAX_POLL_ERRORS:
                    self.mgr.fail_job(self.job_id, f"清单反复拉取失败: {exc}")
                    return
            await asyncio.sleep(self.poll_interval)

    async def _poll_once(self, job: dict):
        resp = await self.mgr.client.get(job["url"])
        resp.raise_for_status()
        # If stop was accepted while the fetch was in flight, this playlist must
        # not extend the range: the boundary is the tail of the last playlist
        # that was fully received before the stop.
        fresh = self.db.get_job(self.job_id)
        if fresh["state"] != "recording":
            return
        pl = parse_media_playlist(resp.text, job["url"])
        last = pl.segments[-1].seq

        if fresh["first_seq"] is None:
            self.db.init_window(self.job_id, pl.media_sequence, last, pl.target_duration)
            self.poll_interval = min(max(pl.target_duration / 2.0, 0.25), 5.0)
            log.info("job %s window starts at seq %s", self.job_id, pl.media_sequence)
        else:
            if pl.media_sequence > fresh["last_seq"] + 1:
                missing = list(range(fresh["last_seq"] + 1, pl.media_sequence))
                self.db.insert_missing(self.job_id, missing)
                raise SourceError(f"窗口越过未发现的序号: {missing}")
            if last > fresh["last_seq"]:
                self.db.update_last_seq(self.job_id, last)

        for seg in pl.segments:
            row = self.db.get_segment(self.job_id, seg.seq)
            if row is not None:
                if row["url"] is not None and row["url"] != seg.url:
                    raise SourceError(f"序号 {seg.seq} 的片段地址发生变化（源不一致）")
                # Same seq and same URL in a later playlist: this is a
                # verification opportunity. Re-fetch and compare hashes.
                if row["status"] == "done" and row["verify_status"] == "unverified":
                    self._enqueue_verify(seg.seq)
                continue
            dup = self.db.get_segment_by_url(self.job_id, seg.url)
            if dup is not None:
                raise SourceError(
                    f"片段地址复用: {seg.url} 同时属于序号 {dup['seq']} 与 {seg.seq}")
            self.db.insert_segment(self.job_id, seg.seq, seg.url, seg.duration, seg.title)
        self.wake.set()

        # Segments that already slid out of this playlist and were never
        # re-fetched get no further chance: mark them explicitly so the UI can
        # never report an unverified segment as consistent.
        self.db.mark_slipped_unverified(
            self.job_id, pl.media_sequence, self._queued_seqs())

        if pl.endlist:
            log.info("job %s source signalled ENDLIST, pinning boundary", self.job_id)
            self.mgr.accept_stop(self.job_id)

    # ---- verification scheduling ----

    def _queued_seqs(self) -> list:
        return list(self.verify_queue._queue)

    def _enqueue_verify(self, seq: int):
        # Idempotent: the same seq may appear in several consecutive windows.
        if seq not in self._queued_seqs():
            self.verify_queue.put_nowait(seq)

    async def _verify_loop(self, worker: int):
        # Keep re-fetching while the job may still need results, including the
        # whole finalizing phase: a stop must not cancel in-flight verification,
        # and no ZIP is published until this queue has fully drained.
        while True:
            job = self.db.get_job(self.job_id)
            if job["state"] in ("failed", "done"):
                return
            seq = await self.verify_queue.get()
            try:
                await self._verify_one(seq, worker)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Never strand a segment in 'verifying': give it another chance.
                log.exception("job %s verifier %s crashed on seq %s", self.job_id, worker, seq)
                self.db.bump_verify_attempts(self.job_id, seq)
            finally:
                self.verify_queue.task_done()

    async def _verify_one(self, seq: int, worker: int):
        seg = self.db.get_segment(self.job_id, seq)
        if seg is None or seg["status"] != "done" or seg["verify_status"] != "unverified":
            return  # conflict/verified/no_opportunity meanwhile, or re-downloaded
        self.db.mark_verifying(self.job_id, seq)
        url, want_hash = seg["url"], seg["sha256"]
        delay = 0.5
        try:
            for attempt in range(1, MAX_VERIFY_ATTEMPTS + 1):
                try:
                    got_hash = await self._stream_hash(url)
                    # The segment may have slid out of the window (no_opportunity)
                    # or been re-downloaded while this stream was in flight.
                    cur = self.db.get_segment(self.job_id, seq)
                    if cur is None or cur["verify_status"] != "verifying":
                        log.info("job %s seq %s 复核返回时状态已变为 %s，放弃该结论",
                                 self.job_id, seq,
                                 cur["verify_status"] if cur else "gone")
                        return
                    if got_hash == want_hash:
                        self.db.mark_verified(self.job_id, seq)
                        log.info("job %s seq %s 复核一致", self.job_id, seq)
                    else:
                        self.db.mark_conflict(self.job_id, seq)
                        log.error("job %s seq %s 同序号同地址片段内容被改写（SHA-256 冲突）",
                                  self.job_id, seq)
                        self.mgr.fail_job(
                            self.job_id,
                            f"片段原件复核冲突: 序号 {seq} 的同地址 TS 内容被改写"
                            f"（原 SHA-256 {want_hash[:12]}…，复取 {got_hash[:12]}…）")
                    return
                except httpx.HTTPError as exc:
                    self.db.bump_verify_attempts(self.job_id, seq)
                    if attempt >= MAX_VERIFY_ATTEMPTS:
                        # Leave it unverified; finalize gives it one last round,
                        # and if the source is gone it closes as no_opportunity.
                        log.warning("job %s seq %s 复核复取失败，保留待复核: %s",
                                    self.job_id, seq, exc)
                        return
                    log.warning("job %s seq %s 复核尝试 %s 失败: %s",
                                self.job_id, seq, attempt, exc)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 8.0)
        except asyncio.CancelledError:
            log.info("job %s seq %s 复核被中断（重启后继续）", self.job_id, seq)
            raise

    async def _stream_hash(self, url: str) -> str:
        """Re-fetch a segment as a stream and hash it incrementally. The body
        is never buffered whole and nothing is written to disk, so concurrent
        recordings, stop handling and queries stay responsive."""
        hasher = hashlib.sha256()
        client = self.mgr.client
        async with client.stream("GET", url, timeout=httpx.Timeout(60.0, connect=10.0)) as resp:
            if resp.status_code == 404:
                raise httpx.HTTPError("HTTP 404")
            if resp.status_code != 200:
                raise httpx.HTTPError(f"HTTP {resp.status_code}")
            async for chunk in resp.aiter_bytes(chunk_size=64 * 1024):
                hasher.update(chunk)
                if TEST_HOOKS["verify_delay"]:
                    await asyncio.sleep(TEST_HOOKS["verify_delay"])
        return hasher.hexdigest()

    # ---- segment downloading ----

    async def _download_loop(self):
        try:
            while True:
                self.wake.clear()
                job = self.db.get_job(self.job_id)
                if job["state"] in ("failed", "done"):
                    return
                seg = self.db.next_pending(self.job_id)
                if seg is None:
                    if job["state"] == "finalizing":
                        await self._finalize(job)
                        return
                    await self.wake.wait()
                    continue
                await self._download_one(seg)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("job %s downloader crashed", self.job_id)
            self.mgr.fail_job(self.job_id, f"内部错误: {exc}")

    async def _download_one(self, seg: dict):
        seq, url = seg["seq"], seg["url"]
        path = self.seg_path(seq)
        tmp = path + ".part"
        delay = 0.5
        for attempt in range(1, MAX_DOWNLOAD_ATTEMPTS + 1):
            try:
                resp = await self.mgr.client.get(
                    url, timeout=httpx.Timeout(60.0, connect=10.0))
                if resp.status_code == 404:
                    self.db.segment_missing(self.job_id, seq, "HTTP 404")
                    self.mgr.fail_job(self.job_id, f"应收片段返回 404: 序号 {seq}")
                    return
                if resp.status_code != 200:
                    raise httpx.HTTPError(f"HTTP {resp.status_code}")
                data = resp.content
                digest = hashlib.sha256(data).hexdigest()
                await asyncio.to_thread(self._write_segment, tmp, path, data)
                if TEST_HOOKS["crash_after_write"] == (self.job_id, seq):
                    os._exit(137)  # test hook: die between flush and DB register
                self.db.segment_done(self.job_id, seq, path, len(data), digest)
                return
            except httpx.HTTPError as exc:
                log.warning("job %s seq %s download attempt %s failed: %s",
                            self.job_id, seq, attempt, exc)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 8.0)
        self.mgr.fail_job(self.job_id, f"片段序号 {seq} 多次重试后仍下载失败")

    @staticmethod
    def _write_segment(tmp: str, path: str, data: bytes):
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)  # atomic: either the old state or the full segment

    # ---- finalize ----

    async def _drain_verifications(self):
        """Wait until every verification that has started (or been queued)
        has settled. A ZIP must never be published while one is outstanding."""
        while True:
            if self.db.get_job(self.job_id)["state"] == "failed":
                return
            await self.verify_queue.join()
            # Everything queued has been processed. Rows still 'unverified'
            # are ones whose re-fetch failed transiently; the caller decides
            # whether to give them another round or close them out.
            return

    async def _finalize(self, job: dict):
        segs = self.db.list_segments(self.job_id)
        if any(s["status"] == "missing" for s in segs):
            return  # fail_job already ran / will run
        if any(s["status"] != "done" for s in segs):
            return  # still waiting for downloads; wake will re-enter

        # The poller is already stopped: unverified segments that are still
        # within the pinned boundary get one last re-fetch round.
        for seg in self.db.list_unverified_done(self.job_id):
            self._enqueue_verify(seg["seq"])
        await self._drain_verifications()

        job = self.db.get_job(self.job_id)
        if job["state"] == "failed":
            return  # a conflict was found during the last-chance round
        # Segments the source no longer serves could not be verified; record
        # that explicitly rather than pretending they matched.
        self.db.mark_remaining_unverified_no_opportunity(self.job_id)

        segs = self.db.list_segments(self.job_id)
        conflicts = [s["seq"] for s in segs if s["verify_status"] == "conflict"]
        if conflicts:
            self.mgr.fail_job(self.job_id, f"片段原件复核冲突，序号: {conflicts}")
            return
        first = job["first_seq"]
        target = job["target_duration"] or 2.0
        lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            f"#EXT-X-TARGETDURATION:{max(1, round(target))}",
            "#EXT-X-PLAYLIST-TYPE:VOD",
            f"#EXT-X-MEDIA-SEQUENCE:{first if first is not None else 0}",
        ]
        for s in segs:
            lines.append(f"#EXTINF:{(s['duration'] or target):.3f},")
            lines.append(os.path.basename(self.seg_path(s["seq"])))
        lines.append("#EXT-X-ENDLIST")
        playlist_text = "\n".join(lines) + "\n"
        zip_path = self.mgr.zip_path(self.job_id)
        await asyncio.to_thread(self._write_package, playlist_text, segs, zip_path)
        self.db.job_done(self.job_id, zip_path)
        log.info("job %s done: %s segments packaged", self.job_id, len(segs))

    def _write_package(self, playlist_text: str, segs: list, zip_path: str):
        os.makedirs(os.path.dirname(zip_path), exist_ok=True)
        playlist_file = os.path.join(self.seg_dir, "playlist.m3u8")
        with open(playlist_file, "w", encoding="utf-8") as fh:
            fh.write(playlist_text)
        tmp_zip = zip_path + ".part"
        with zipfile.ZipFile(tmp_zip, "w", zipfile.ZIP_STORED) as zf:
            zf.writestr("playlist.m3u8", playlist_text)
            for s in segs:
                zf.write(self.seg_path(s["seq"]), os.path.basename(self.seg_path(s["seq"])))
        os.replace(tmp_zip, zip_path)


class Manager:
    def __init__(self, db, data_dir: str):
        self.db = db
        self.data_dir = data_dir
        self.client = httpx.AsyncClient(
            follow_redirects=True,
            headers={"User-Agent": "hls-live-recorder/1.0"},
            limits=httpx.Limits(max_connections=32, max_keepalive_connections=16),
        )
        self.runners: dict[int, JobRunner] = {}

    def seg_dir(self, job_id: int) -> str:
        return os.path.join(self.data_dir, "segments", str(job_id))

    def zip_path(self, job_id: int) -> str:
        return os.path.join(self.data_dir, "zips", f"job_{job_id}.zip")

    def create_job(self, url: str) -> int:
        job_id = self.db.insert_job(url)
        runner = JobRunner(self, job_id)
        self.runners[job_id] = runner
        runner.start()
        return job_id

    def recover(self):
        os.makedirs(os.path.join(self.data_dir, "zips"), exist_ok=True)
        for job in self.db.unfinished_jobs():
            runner = JobRunner(self, job["id"])
            self.runners[job["id"]] = runner
            runner.start()

    def accept_stop(self, job_id: int) -> bool:
        """Pin the boundary at the tail of the last received playlist and
        persist it immediately, so a restart cannot change it. In-flight
        verifications keep running; the downloader waits for them before any
        ZIP is published."""
        job = self.db.get_job(job_id)
        if job is None or job["state"] != "recording":
            return False
        self.db.set_stopping(job_id, job["last_seq"])
        runner = self.runners.get(job_id)
        if runner is not None:
            if runner.poller is not None and runner.poller is not asyncio.current_task():
                runner.poller.cancel()
            runner.wake.set()
        log.info("job %s stop accepted, boundary pinned at seq %s", job_id, job["last_seq"])
        return True

    def fail_job(self, job_id: int, message: str):
        self.db.job_failed(job_id, message)
        runner = self.runners.get(job_id)
        if runner is not None:
            if runner.poller is not None and runner.poller is not asyncio.current_task():
                runner.poller.cancel()
            runner.wake.set()
        log.error("job %s failed: %s", job_id, message)

    async def shutdown(self):
        tasks = []
        for runner in self.runners.values():
            for task in (runner.poller, runner.downloader, *runner.verifiers):
                if task is not None and not task.done():
                    task.cancel()
                    tasks.append(task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self.client.aclose()
