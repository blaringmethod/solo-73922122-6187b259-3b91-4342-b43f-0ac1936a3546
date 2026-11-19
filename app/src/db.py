"""SQLite progress store. Segments themselves live on the persistent volume;
the DB only tracks job state and per-segment progress, including the
verification (re-fetch hash comparison) state of every saved segment."""

import os
import sqlite3
import threading
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'recording',   -- recording | finalizing | done | failed
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL,
  first_seq INTEGER,          -- media sequence of the first segment of the first playlist
  last_seq INTEGER,           -- highest sequence seen in any received playlist
  stop_seq INTEGER,           -- pinned tail of the last received playlist once stop accepted
  target_duration REAL,
  error TEXT,
  zip_path TEXT
);
CREATE TABLE IF NOT EXISTS segments (
  job_id INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  url TEXT,                   -- NULL for segments the window skipped before discovery
  duration REAL,
  title TEXT,
  status TEXT NOT NULL DEFAULT 'pending',    -- pending | done | missing
  path TEXT,
  bytes INTEGER,
  error TEXT,
  sha256 TEXT,                -- hash of the bytes captured at first save (NULL until done)
  verify_status TEXT NOT NULL DEFAULT 'unverified',
      -- unverified | verifying | verified | conflict | no_opportunity
  verify_attempts INTEGER NOT NULL DEFAULT 0,
  last_verify_at REAL,
  PRIMARY KEY (job_id, seq)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_segments_job_url
  ON segments(job_id, url) WHERE url IS NOT NULL;
"""

# Columns added after the initial release; applied lazily so databases on the
# persistent volume migrate themselves on startup.
_ADDED_COLUMNS = [
    ("sha256", "ALTER TABLE segments ADD COLUMN sha256 TEXT"),
    ("verify_status",
     "ALTER TABLE segments ADD COLUMN verify_status TEXT NOT NULL DEFAULT 'unverified'"),
    ("verify_attempts",
     "ALTER TABLE segments ADD COLUMN verify_attempts INTEGER NOT NULL DEFAULT 0"),
    ("last_verify_at", "ALTER TABLE segments ADD COLUMN last_verify_at REAL"),
]


class DB:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(SCHEMA)
        self._migrate()
        self._conn.commit()
        self._lock = threading.Lock()

    def _migrate(self):
        existing = {r["name"] for r in self._conn.execute("PRAGMA table_info(segments)")}
        for name, ddl in _ADDED_COLUMNS:
            if name not in existing:
                self._conn.execute(ddl)

    # ---- jobs ----

    def insert_job(self, url: str) -> int:
        now = time.time()
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT INTO jobs(url, state, created_at, updated_at) VALUES (?,?,?,?)",
                (url, "recording", now, now),
            )
            return cur.lastrowid

    def get_job(self, job_id: int):
        with self._lock:
            row = self._conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            return dict(row) if row else None

    def list_jobs(self):
        with self._lock:
            rows = self._conn.execute("SELECT * FROM jobs ORDER BY id").fetchall()
            return [dict(r) for r in rows]

    def unfinished_jobs(self):
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs WHERE state IN ('recording','finalizing') ORDER BY id"
            ).fetchall()
            return [dict(r) for r in rows]

    def _touch(self, job_id: int):
        self._conn.execute("UPDATE jobs SET updated_at=? WHERE id=?", (time.time(), job_id))

    def init_window(self, job_id: int, first_seq: int, last_seq: int, target: float):
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE jobs SET first_seq=?, last_seq=?, target_duration=? WHERE id=?",
                (first_seq, last_seq, target, job_id),
            )
            self._touch(job_id)

    def update_last_seq(self, job_id: int, last_seq: int):
        with self._lock, self._conn:
            self._conn.execute("UPDATE jobs SET last_seq=? WHERE id=?", (last_seq, job_id))
            self._touch(job_id)

    def set_stopping(self, job_id: int, stop_seq):
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE jobs SET state='finalizing', stop_seq=? WHERE id=? AND state='recording'",
                (stop_seq, job_id),
            )
            self._touch(job_id)

    def job_done(self, job_id: int, zip_path: str):
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE jobs SET state='done', zip_path=? WHERE id=?", (zip_path, job_id)
            )
            self._touch(job_id)

    def job_failed(self, job_id: int, error: str):
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE jobs SET state='failed', error=? WHERE id=? AND state NOT IN ('done','failed')",
                (error, job_id),
            )
            self._touch(job_id)

    # ---- segments ----

    def insert_segment(self, job_id: int, seq: int, url: str, duration: float, title: str):
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO segments(job_id, seq, url, duration, title, status) VALUES (?,?,?,?,?,'pending')",
                (job_id, seq, url, duration, title),
            )

    def insert_missing(self, job_id: int, seqs):
        with self._lock, self._conn:
            for s in seqs:
                self._conn.execute(
                    "INSERT OR IGNORE INTO segments(job_id, seq, url, status, error) VALUES (?,?,NULL,'missing','窗口越过，未发现该序号')",
                    (job_id, s),
                )

    def get_segment(self, job_id: int, seq: int):
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM segments WHERE job_id=? AND seq=?", (job_id, seq)
            ).fetchone()
            return dict(row) if row else None

    def get_segment_by_url(self, job_id: int, url: str):
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM segments WHERE job_id=? AND url=?", (job_id, url)
            ).fetchone()
            return dict(row) if row else None

    def next_pending(self, job_id: int):
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM segments WHERE job_id=? AND status='pending' ORDER BY seq LIMIT 1",
                (job_id,),
            ).fetchone()
            return dict(row) if row else None

    def list_segments(self, job_id: int):
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM segments WHERE job_id=? ORDER BY seq", (job_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    def segment_done(self, job_id: int, seq: int, path: str, nbytes: int, sha256: str):
        """First successful save: persist the bytes' SHA-256. The segment
        starts out unverified -- only a later matching re-fetch may promote
        it, and nothing may ever call it consistent without one."""
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE segments SET status='done', path=?, bytes=?, error=NULL, "
                "sha256=?, verify_status='unverified', verify_attempts=0 "
                "WHERE job_id=? AND seq=?",
                (path, nbytes, sha256, job_id, seq),
            )

    def segment_pending(self, job_id: int, seq: int):
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE segments SET status='pending', path=NULL, bytes=NULL, "
                "sha256=NULL, verify_status='unverified', verify_attempts=0, last_verify_at=NULL "
                "WHERE job_id=? AND seq=?",
                (job_id, seq),
            )

    def segment_missing(self, job_id: int, seq: int, error: str):
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE segments SET status='missing', error=? WHERE job_id=? AND seq=?",
                (error, job_id, seq),
            )

    # ---- verification ----

    def reset_stale_verifying(self, job_id: int):
        """Restart recovery: a verification interrupted by a crash gets
        another chance rather than being stuck in 'verifying' forever."""
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE segments SET verify_status='unverified' "
                "WHERE job_id=? AND verify_status='verifying'",
                (job_id,),
            )

    def mark_verifying(self, job_id: int, seq: int):
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE segments SET verify_status='verifying' "
                "WHERE job_id=? AND seq=? AND verify_status='unverified'",
                (job_id, seq),
            )

    def mark_verified(self, job_id: int, seq: int):
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE segments SET verify_status='verified', last_verify_at=? "
                "WHERE job_id=? AND seq=?",
                (time.time(), job_id, seq),
            )

    def mark_conflict(self, job_id: int, seq: int):
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE segments SET verify_status='conflict', last_verify_at=? "
                "WHERE job_id=? AND seq=?",
                (time.time(), job_id, seq),
            )

    def mark_no_opportunity(self, job_id: int, seq: int):
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE segments SET verify_status='no_opportunity', last_verify_at=? "
                "WHERE job_id=? AND seq=? AND verify_status='unverified'",
                (time.time(), job_id, seq),
            )

    def bump_verify_attempts(self, job_id: int, seq: int):
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE segments SET verify_attempts=verify_attempts+1, "
                "verify_status='unverified' WHERE job_id=? AND seq=?",
                (job_id, seq),
            )

    def list_unverified_done(self, job_id: int):
        """Saved segments that never got a matching re-fetch."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM segments WHERE job_id=? AND status='done' "
                "AND verify_status='unverified' ORDER BY seq",
                (job_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def mark_slipped_unverified(self, job_id: int, window_first: int, queued_seqs):
        """Segments that already left the manifest window without a completed
        re-fetch: record that no verification opportunity remains. They must
        not be presented as consistent. This covers segments still 'unverified'
        and even ones whose re-fetch is in flight ('verifying') or queued: by
        the time it answers, the source window no longer offers another chance,
        and a single in-flight answer cannot be repeated anyway. Segments
        currently queued for that in-flight attempt are excluded so the poller
        does not fight the verifier's own state transition."""
        with self._lock, self._conn:
            sql = ("UPDATE segments SET verify_status='no_opportunity', last_verify_at=? "
                   "WHERE job_id=? AND status='done' "
                   "AND verify_status IN ('unverified','verifying') AND seq < ?")
            params = [time.time(), job_id, window_first]
            if queued_seqs:
                placeholders = ",".join("?" for _ in queued_seqs)
                sql += f" AND seq NOT IN ({placeholders})"
                params.extend(queued_seqs)
            self._conn.execute(sql, params)

    def mark_remaining_unverified_no_opportunity(self, job_id: int):
        """Finalize-time fallback: after the last-chance re-fetch round, any
        saved segment still unverified (source unreachable/gone) is closed out
        as never having had a verification opportunity."""
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE segments SET verify_status='no_opportunity', last_verify_at=? "
                "WHERE job_id=? AND status='done' AND verify_status='unverified'",
                (time.time(), job_id),
            )

    def conflict_seqs(self, job_id: int):
        with self._lock:
            rows = self._conn.execute(
                "SELECT seq FROM segments WHERE job_id=? AND verify_status='conflict' ORDER BY seq",
                (job_id,),
            ).fetchall()
            return [r["seq"] for r in rows]

    def verify_counts(self, job_id: int):
        with self._lock:
            row = self._conn.execute(
                "SELECT "
                "SUM(CASE WHEN verify_status='verified' THEN 1 ELSE 0 END) AS verified, "
                "SUM(CASE WHEN verify_status='verifying' THEN 1 ELSE 0 END) AS verifying, "
                "SUM(CASE WHEN verify_status='unverified' THEN 1 ELSE 0 END) AS unverified, "
                "SUM(CASE WHEN verify_status='no_opportunity' THEN 1 ELSE 0 END) AS no_opportunity, "
                "SUM(CASE WHEN verify_status='conflict' THEN 1 ELSE 0 END) AS conflict "
                "FROM segments WHERE job_id=? AND status='done'",
                (job_id,),
            ).fetchone()
            return {k: (row[k] or 0) for k in
                    ("verified", "verifying", "unverified", "no_opportunity", "conflict")}

    def counts(self, job_id: int):
        with self._lock:
            row = self._conn.execute(
                "SELECT "
                "COUNT(*) AS total, "
                "SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS done, "
                "SUM(CASE WHEN status='missing' THEN 1 ELSE 0 END) AS missing "
                "FROM segments WHERE job_id=?",
                (job_id,),
            ).fetchone()
            return {
                "total": row["total"] or 0,
                "done": row["done"] or 0,
                "missing": row["missing"] or 0,
            }

    def missing_seqs(self, job_id: int):
        with self._lock:
            rows = self._conn.execute(
                "SELECT seq FROM segments WHERE job_id=? AND status='missing' ORDER BY seq",
                (job_id,),
            ).fetchall()
            return [r["seq"] for r in rows]
