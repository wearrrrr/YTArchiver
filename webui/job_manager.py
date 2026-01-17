from __future__ import annotations

import json
import logging
import threading
import uuid
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable

from ytarchiver.progress import bind_interrupt_probe, register_progress_sink, unregister_progress_sink
from ytarchiver.service import ArchiveConfig, JobControl, JobInterrupted, prepare_tasks, run_archive
from ytarchiver.state import deserialize_video_task, serialize_video_task


JobListener = Callable[[dict], None]
LOG = logging.getLogger("ytarchiver.webui.jobs")
if not LOG.handlers:
	handler = logging.StreamHandler()
	handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
	LOG.addHandler(handler)
LOG.setLevel(logging.INFO)
LOG.propagate = False


def _utc_now() -> str:
	return datetime.utcnow().isoformat()


def _default_progress() -> dict:
	return {
		"label": "Queued",
		"detail": "",
		"percent": None,
		"downloaded": 0,
		"total": None,
		"eta": None,
		"speed": None,
		"show_transfer": False,
		"batch_index": None,
		"batch_total": None,
		"updated": _utc_now(),
	}


def _read_log_tail(log_path: str | Path | None, max_lines: int) -> list[str]:
	max_lines = max(1, min(max_lines, 1000))
	if not log_path:
		return ["Log file not specified."]
	path = Path(log_path)
	if not path.exists():
		return ["Log file not created yet."]
	buffer: deque[str] = deque(maxlen=max_lines)
	try:
		with path.open("r", encoding="utf-8", errors="ignore") as handle:
			for line in handle:
				buffer.append(line.rstrip("\n"))
	except OSError:
		return ["Unable to read log file."]
	return list(buffer)


def _config_to_dict(config: ArchiveConfig) -> dict:
	return {
		"command": config.command,
		"handle": config.handle,
		"video_ids": list(config.video_ids or []),
		"out": config.out,
		"no_cache": config.no_cache,
		"log_file": config.log_file,
		"log_level": config.log_level,
		"clear_screen": config.clear_screen,
	}


def _config_from_dict(payload: dict) -> ArchiveConfig:
	return ArchiveConfig(
		command=payload.get("command", "channel"),
		handle=payload.get("handle"),
		video_ids=payload.get("video_ids") or [],
		out=payload.get("out", "yt"),
		no_cache=bool(payload.get("no_cache")),
		log_file=payload.get("log_file", "logs/ytarchiver.log"),
		log_level=payload.get("log_level", "INFO"),
		clear_screen=bool(payload.get("clear_screen", True)),
	)


class JobManager:
	def __init__(self, storage_path: Path):
		self.storage_path = Path(storage_path)
		self.storage_path.parent.mkdir(parents=True, exist_ok=True)
		self.jobs: dict[str, dict] = {}
		self.queue: list[str] = []
		self.listeners: set[JobListener] = set()
		self.job_controls: dict[str, JobControl] = {}
		self.lock = threading.Lock()
		self.condition = threading.Condition(self.lock)
		self.active_job: str | None = None
		self._load()
		self.worker = threading.Thread(target=self._worker_loop, daemon=True)
		self.worker.start()

	# ------------------------------------------------------------------
	# Persistence helpers
	# ------------------------------------------------------------------
	def _load(self):
		if not self.storage_path.exists():
			return
		try:
			with self.storage_path.open("r", encoding="utf-8") as handle:
				payload = json.load(handle)
		except (OSError, ValueError):
			return

		jobs = payload.get("jobs") or []
		saved_queue = payload.get("queue") or []

		for item in jobs:
			job_id = item.get("id")
			if not job_id:
				continue
			status = item.get("status") or "queued"
			if status == "running":
				item["status"] = "stopped"
				item["error"] = "Interrupted during server restart."
			progress = item.get("progress") or _default_progress()
			progress.setdefault("updated", _utc_now())
			item["progress"] = progress
			item.setdefault("next_index", 1)
			item.setdefault("resume_supported", item.get("command") in {"channel", "shorts"})
			item.setdefault("tasks", [])
			item.setdefault("video_count", len(item.get("tasks") or []))
			self.jobs[job_id] = item

		for job_id in saved_queue:
			if job_id in self.jobs and self.jobs[job_id].get("status") == "queued":
				self.queue.append(job_id)

		for job_id, job in self.jobs.items():
			if job.get("status") == "queued" and job_id not in self.queue:
				self.queue.append(job_id)

	def _persist(self):
		with self.lock:
			self._persist_unlocked()

	def _persist_unlocked(self):
		snapshot = {
			"jobs": [self._cleanup_for_storage(job) for job in self.jobs.values()],
			"queue": list(self.queue),
		}
		tmp_path = self.storage_path.with_suffix(".tmp")
		with tmp_path.open("w", encoding="utf-8") as handle:
			json.dump(snapshot, handle, indent=2)
		tmp_path.replace(self.storage_path)

	@staticmethod
	def _cleanup_for_storage(job: dict) -> dict:
		stored = dict(job)
		stored.pop("queue_position", None)
		return stored

	# ------------------------------------------------------------------
	# Public APIs
	# ------------------------------------------------------------------
	def register_listener(self, callback: JobListener):
		with self.lock:
			self.listeners.add(callback)

	def unregister_listener(self, callback: JobListener):
		with self.lock:
			self.listeners.discard(callback)

	def list_jobs(self) -> list[dict]:
		with self.lock:
			return [self._job_payload(job) for job in sorted(self.jobs.values(), key=lambda item: item.get("created"), reverse=True)]

	def get_job(self, job_id: str) -> dict | None:
		with self.lock:
			job = self.jobs.get(job_id)
			return self._job_payload(job) if job else None

	def serialize_job(self, job_id: str, *, include_log: bool = False, max_lines: int = 40) -> dict | None:
		job_payload = self.get_job(job_id)
		if not job_payload:
			return None
		if include_log:
			lines = _read_log_tail(job_payload.get("log_file"), max_lines)
			job_payload["tail"] = lines
			job_payload["tail_text"] = "\n".join(lines)
		return job_payload

	def log_payload(self, job_id: str, max_lines: int = 200) -> dict:
		job = self.get_job(job_id)
		if not job:
			raise ValueError("Job not found")
		lines = _read_log_tail(job.get("log_file"), max_lines)
		return {
			"job_id": job_id,
			"status": job.get("status"),
			"progress": job.get("progress"),
			"tail": lines,
			"tail_text": "\n".join(lines),
		}

	def create_job(self, config: ArchiveConfig, log_override: Path | None = None) -> str:
		tasks, channel_meta = prepare_tasks(config)
		job_id = str(uuid.uuid4())
		log_path = log_override.expanduser() if log_override else Path(f"logs/webui-{job_id}.log")
		config.log_file = str(log_path)

		progress = _default_progress()
		progress["label"] = "Queued"

		job_record = {
			"id": job_id,
			"config": _config_to_dict(config),
			"status": "queued",
			"error": None,
			"log_file": str(log_path),
			"created": _utc_now(),
			"updated": _utc_now(),
			"progress": progress,
			"tasks": [serialize_video_task(task) for task in tasks],
			"next_index": 1,
			"resume_supported": config.command in {"channel", "shorts"},
			"channel_meta": channel_meta,
			"command": config.command,
			"handle": config.handle,
			"video_ids": list(config.video_ids or []),
			"video_count": len(tasks) or len(config.video_ids or []),
		}

		with self.lock:
			self.jobs[job_id] = job_record
			self._enqueue_locked(job_id)
			self._persist_unlocked()
			self._notify_unlocked(job_record)
		return job_id

	def pause_job(self, job_id: str):
		with self.lock:
			job = self.jobs.get(job_id)
			if not job:
				raise ValueError("Job not found")
			status = job.get("status")
			if status == "running":
				control = self.job_controls.get(job_id)
				if control:
					LOG.info("Pause requested for running job %s", job_id)
					control.request_pause()
				return
			if status == "queued":
				LOG.info("Pause requested for queued job %s", job_id)
				self._remove_from_queue(job_id)
				job["status"] = "paused"
				job["updated"] = _utc_now()
				self._persist_unlocked()
				self._notify_unlocked(job)
				return
			raise ValueError("Job is not running or queued")

	def stop_job(self, job_id: str):
		with self.lock:
			job = self.jobs.get(job_id)
			if not job:
				raise ValueError("Job not found")
			status = job.get("status")
			if status == "running":
				control = self.job_controls.get(job_id)
				if control:
					LOG.info("Stop requested for running job %s", job_id)
					control.request_stop()
				return
			if status in {"queued", "paused"}:
				LOG.info("Stop requested for queued/paused job %s", job_id)
				self._remove_from_queue(job_id)
				job["status"] = "stopped"
				job["updated"] = _utc_now()
				self._persist_unlocked()
				self._notify_unlocked(job)
				return
			raise ValueError("Job cannot be stopped in its current state")

	def resume_job(self, job_id: str):
		with self.lock:
			job = self.jobs.get(job_id)
			if not job:
				raise ValueError("Job not found")
			if job.get("status") not in {"paused", "stopped", "failed"}:
				raise ValueError("Job is not paused or stopped")
			job["status"] = "queued"
			job["error"] = None
			job["updated"] = _utc_now()
			if not job.get("resume_supported"):
				job["next_index"] = 1
			self._enqueue_locked(job_id)
			self._persist_unlocked()
			self._notify_unlocked(job)

	def delete_job(self, job_id: str):
		with self.lock:
			job = self.jobs.get(job_id)
			if not job:
				raise ValueError("Job not found")
			if job.get("status") == "running":
				raise ValueError("Cannot delete a running job")
			self._remove_from_queue(job_id)
			self.jobs.pop(job_id, None)
			self._persist_unlocked()
			self._notify_deleted(job_id)

	# ------------------------------------------------------------------
	# Worker and queue management
	# ------------------------------------------------------------------
	def _enqueue_locked(self, job_id: str):
		if job_id not in self.queue:
			self.queue.append(job_id)
			self.condition.notify_all()

	def _remove_from_queue(self, job_id: str):
		if job_id in self.queue:
			self.queue = [jid for jid in self.queue if jid != job_id]

	def _job_payload(self, job: dict | None) -> dict | None:
		if not job:
			return None
		payload = json.loads(json.dumps(job))
		payload["queue_position"] = self._queue_position(job["id"])
		return payload

	def _queue_position(self, job_id: str) -> int | None:
		try:
			idx = self.queue.index(job_id)
		except ValueError:
			return None
		return idx + 1

	def _notify_unlocked(self, job: dict):
		job_payload = self._job_payload(job)
		if not job_payload:
			return
		event_payload = {"event": "job_update", "job": job_payload}
		callbacks: Iterable[JobListener] = list(self.listeners)
		for listener in callbacks:
			try:
				listener(event_payload)
			except Exception:
				continue

	def _notify_deleted(self, job_id: str):
		payload = {"event": "job_deleted", "job_id": job_id}
		callbacks: Iterable[JobListener] = list(self.listeners)
		for listener in callbacks:
			try:
				listener(payload)
			except Exception:
				continue

	def _worker_loop(self):
		while True:
			job_id, control = self._await_job()
			if not job_id:
				continue
			self._execute_job(job_id, control)

	def _await_job(self) -> tuple[str | None, JobControl | None]:
		with self.condition:
			while True:
				if self.queue:
					job_id = self.queue.pop(0)
					job = self.jobs.get(job_id)
					if not job or job.get("status") != "queued":
						continue
					job["status"] = "running"
					job["error"] = None
					job["updated"] = _utc_now()
					control = JobControl()
					LOG.info("Created JobControl for job %s", job_id)
					self.job_controls[job_id] = control
					self.active_job = job_id
					self._persist_unlocked()
					self._notify_unlocked(job)
					return job_id, control
				self.condition.wait()

	def _execute_job(self, job_id: str, control: JobControl | None):
		progress_callback = None
		interrupt_bound = False
		try:
			with self.lock:
				job = self.jobs.get(job_id)
				if not job:
					return
				config = _config_from_dict(job["config"])
				tasks = [deserialize_video_task(item) for item in job.get("tasks") or []]
				if not tasks:
					fetched_tasks, channel_meta = prepare_tasks(config)
					tasks = fetched_tasks
					job["tasks"] = [serialize_video_task(item) for item in tasks]
					job["channel_meta"] = channel_meta
				channel_meta = job.get("channel_meta")
				start_index = int(job.get("next_index") or 1)
				if not job.get("resume_supported"):
					start_index = 1

			def checkpoint_cb(completed_idx: int, _task):
				with self.lock:
					job = self.jobs.get(job_id)
					if not job:
						return
					if job.get("resume_supported"):
						job["next_index"] = completed_idx + 1
					else:
						job["next_index"] = 1
					job["updated"] = _utc_now()
					self._persist_unlocked()
					self._notify_unlocked(job)

			def progress_sink(payload: dict):
				progress_snapshot = {
					"label": payload.get("label", ""),
					"detail": payload.get("detail", ""),
					"percent": payload.get("percent"),
					"downloaded": payload.get("downloaded"),
					"total": payload.get("total"),
					"eta": payload.get("eta"),
					"speed": payload.get("speed"),
					"show_transfer": payload.get("show_transfer", False),
					"batch_index": payload.get("batch_index"),
					"batch_total": payload.get("batch_total"),
					"updated": _utc_now(),
				}
				with self.lock:
					job = self.jobs.get(job_id)
					if not job:
						return
					job["progress"] = progress_snapshot
					job["updated"] = _utc_now()
					self._notify_unlocked(job)

			progress_callback = progress_sink
			register_progress_sink(progress_callback)
			if control:
				probe_state = {"notified": False}
				def interrupt_probe():
					reason = control.pending_reason()
					if reason and not probe_state["notified"]:
						LOG.info("Interrupt probe triggered for job %s (%s)", job_id, reason)
						probe_state["notified"] = True
					return reason
				bind_interrupt_probe(interrupt_probe)
				interrupt_bound = True
			else:
				LOG.warning("Job %s is running without a JobControl; interrupts disabled", job_id)
			run_archive(
				config,
				tasks=tasks,
				start_index=start_index,
				job_control=control,
				checkpoint_cb=checkpoint_cb,
				channel_meta=channel_meta,
			)
			with self.lock:
				job = self.jobs.get(job_id)
				if not job:
					return
				job["status"] = "completed"
				job["error"] = None
				job["progress"] = {
					"label": "Completed",
					"detail": "",
					"percent": None,
					"downloaded": job.get("progress", {}).get("downloaded"),
					"total": job.get("progress", {}).get("total"),
					"eta": None,
					"speed": None,
					"show_transfer": False,
					"batch_index": job.get("progress", {}).get("batch_index"),
					"batch_total": job.get("progress", {}).get("batch_total"),
					"updated": _utc_now(),
				}
				job["next_index"] = len(job.get("tasks") or []) + 1
				job["updated"] = _utc_now()
				self.job_controls.pop(job_id, None)
				self.active_job = None
				self._persist_unlocked()
				self._notify_unlocked(job)
		except JobInterrupted as interrupt:
			status = "paused" if interrupt.reason == "paused" else "stopped"
			with self.lock:
				job = self.jobs.get(job_id)
				if job:
					job["status"] = status
					job["error"] = None
					job["updated"] = _utc_now()
					if not job.get("resume_supported"):
						job["next_index"] = 1
					self.job_controls.pop(job_id, None)
					self.active_job = None
					self._persist_unlocked()
					self._notify_unlocked(job)
		except Exception as exc:  # noqa: BLE001
			with self.lock:
				job = self.jobs.get(job_id)
				if job:
					job["status"] = "failed"
					job["error"] = str(exc)
					job["updated"] = _utc_now()
					self.job_controls.pop(job_id, None)
					self.active_job = None
					self._persist_unlocked()
					self._notify_unlocked(job)
		finally:
			if interrupt_bound:
				bind_interrupt_probe(None)
			if progress_callback:
				unregister_progress_sink(progress_callback)
