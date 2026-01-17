from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Iterable, Protocol

from .service import ArchiveConfig, prepare_tasks
from .state import VideoTask
from .watchlist import WatchEntry, WatchlistStore


class JobScheduler(Protocol):
    """Minimal interface required from a job manager/queue implementation."""

    def create_job(self, config: ArchiveConfig, log_override: Path | None = None) -> str: ...
    def list_jobs(self) -> list[dict]: ...


ACTIVE_JOB_STATES = {"queued", "running", "paused"}


class WatchDaemon:
    """
    Periodically scans the watchlist and enqueues archive jobs for new uploads.

    The daemon is intentionally decoupled from the Flask app; instantiate it with the
    shared WatchlistStore and JobManager (or any object implementing JobScheduler),
    then call `run_forever()` from a long-lived process (systemd service, Docker, etc.).
    """

    def __init__(
        self,
        job_manager: JobScheduler,
        watchlist: WatchlistStore,
        *,
        poll_interval: int = 120,
        batch_size: int = 3,
        logger: logging.Logger | None = None,
    ):
        self.job_manager = job_manager
        self.watchlist = watchlist
        self.poll_interval = max(15, int(poll_interval))
        self.batch_size = max(1, int(batch_size))
        self.logger = logger or logging.getLogger("ytarchiver.watch")
        self._stop_event = threading.Event()

    def run_forever(self):
        """Block while polling the watchlist until `stop()` is called."""
        self.logger.info(
            "Watch daemon started (interval=%ss, batch_size=%s)",
            self.poll_interval,
            self.batch_size,
        )
        while not self._stop_event.is_set():
            loop_started = time.time()
            try:
                self.tick(now=loop_started)
            except Exception:  # noqa: BLE001
                self.logger.exception("Watch tick failed")
            elapsed = time.time() - loop_started
            wait_for = max(0.0, self.poll_interval - elapsed)
            if wait_for:
                self._stop_event.wait(wait_for)
        self.logger.info("Watch daemon stopped")

    def stop(self):
        """Signal the daemon to exit after the current iteration."""
        self._stop_event.set()

    def tick(self, now: float | None = None):
        """Execute a single polling cycle (useful for tests)."""
        now = now or time.time()
        due_entries = self._collect_due_entries(now)
        if not due_entries:
            self.logger.debug("No watchlist entries due at %s", time.ctime(now))
            return

        touched: list[tuple[int, float]] = []
        enqueued: list[tuple[int, float]] = []
        archive_cache: dict[Path, set[str]] = {}

        for entry in due_entries:
            touched.append((entry.id, now))
            if self._has_active_job(entry):
                self.logger.debug(
                    "Skipping %s (%s) because a job is already queued or running.",
                    entry.handle,
                    entry.mode,
                )
                continue
            try:
                job_created = self._process_entry(entry, now, archive_cache)
            except Exception:  # noqa: BLE001
                self.logger.exception(
                    "Failed to evaluate watch entry %s (%s)",
                    entry.handle,
                    entry.mode,
                )
                continue
            if job_created:
                enqueued.append((entry.id, now))

        if touched:
            self.watchlist.bulk_touch(touched)
        if enqueued:
            self.watchlist.mark_enqueued(enqueued)

    def _collect_due_entries(self, now: float) -> list[WatchEntry]:
        due: list[WatchEntry] = []
        for entry in self.watchlist.iter_due_entries(now):
            if entry.id is None:
                continue
            due.append(entry)
            if len(due) >= self.batch_size:
                break
        return due

    def _process_entry(
        self,
        entry: WatchEntry,
        now_ts: float,
        archive_cache: dict[Path, set[str]],
    ) -> bool:
        config = ArchiveConfig(
            command=entry.mode,
            handle=entry.normalized_handle(),
            video_ids=[],
            out=entry.out_dir,
            no_cache=entry.no_cache,
            log_file=str(self._log_path_for_entry(entry)),
            log_level=entry.log_level,
            clear_screen=entry.clear_screen,
        )

        tasks, _channel_meta = prepare_tasks(config)
        if not tasks:
            self.logger.warning(
                "No videos returned for %s (%s). The channel may be empty or private.",
                entry.handle,
                entry.mode,
            )
            return False

        candidate_tasks = self._filter_new_tasks(entry, tasks, archive_cache)
        if not candidate_tasks:
            self.logger.debug("No new uploads detected for %s", entry.handle)
            return False

        log_path = self._log_path_for_entry(entry)
        job_id = self._enqueue_job(config, log_path)
        self.logger.info(
            "Enqueued %s job %s for %s (%d candidate videos).",
            entry.mode,
            job_id[:8],
            entry.handle,
            len(candidate_tasks),
        )
        return True

    def _filter_new_tasks(
        self,
        entry: WatchEntry,
        tasks: Iterable[VideoTask],
        archive_cache: dict[Path, set[str]],
    ) -> list[VideoTask]:
        if entry.no_cache:
            return list(tasks)

        archive_path = Path(entry.out_dir).expanduser() / "downloaded.txt"
        downloaded = archive_cache.get(archive_path)
        if downloaded is None:
            downloaded = self._read_archive_file(archive_path)
            archive_cache[archive_path] = downloaded
        return [task for task in tasks if task.video_id not in downloaded]

    def _read_archive_file(self, archive_path: Path) -> set[str]:
        if not archive_path.exists():
            return set()
        try:
            with archive_path.open("r", encoding="utf-8", errors="ignore") as handle:
                return {line.strip() for line in handle if line.strip()}
        except OSError as exc:
            self.logger.warning("Unable to read archive file %s (%s)", archive_path, exc)
            return set()

    def _has_active_job(self, entry: WatchEntry) -> bool:
        normalized = entry.normalized_handle().lower()
        try:
            jobs = self.job_manager.list_jobs()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("Unable to inspect job queue (%s)", exc)
            return False

        for job in jobs:
            if job.get("command") != entry.mode:
                continue
            handle = (job.get("handle") or "").strip().lower()
            if handle and handle != normalized:
                continue
            if job.get("status") in ACTIVE_JOB_STATES:
                return True
        return False

    def _enqueue_job(self, config: ArchiveConfig, log_path: Path) -> str:
        log_path = Path(log_path).expanduser()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            return self.job_manager.create_job(config, log_path)
        except Exception as exc:  # noqa: BLE001
            self.logger.error(
                "Unable to enqueue job for %s (%s): %s",
                config.handle,
                config.command,
                exc,
            )
            raise

    def _log_path_for_entry(self, entry: WatchEntry) -> Path:
        suffix = entry.mode
        identifier = entry.id if entry.id is not None else entry.handle.strip("@") or "watch"
        return Path(f"logs/watch-{identifier}-{suffix}.log")