import json
import re
import threading
import uuid
from collections import deque
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, redirect, render_template, request, url_for
from flask_sock import Sock
from simple_websocket import ConnectionClosed, Server

from ytarchiver.progress import register_progress_sink, unregister_progress_sink
from ytarchiver.service import ArchiveConfig, run_archive

app = Flask(__name__)
sock = Sock(app)

_jobs: dict[str, dict] = {}
_jobs_lock = threading.Lock()
_ws_clients: set[Server] = set()
_ws_clients_lock = threading.Lock()
_log_subscribers: dict[str, set[Server]] = {}
_log_subscribers_lock = threading.Lock()
_ws_log_targets: dict[Server, str] = {}


def _read_log_tail(job: dict, max_lines: int) -> list[str]:
    max_lines = max(1, min(max_lines, 500))
    log_path = Path(job["log_file"])
    if log_path.exists():
        try:
            log_buffer: deque[str] = deque(maxlen=max_lines)
            with log_path.open("r", encoding="utf-8", errors="ignore") as handle:
                for line in handle:
                    log_buffer.append(line.rstrip("\n"))
            return list(log_buffer)
        except Exception:  # noqa: BLE001
            return ["Unable to read log file."]
    return ["Log file not created yet."]


def _serialize_job(job: dict, include_log: bool = False, max_lines: int = 40) -> dict:
    config: ArchiveConfig = job["config"]
    video_ids = list(config.video_ids or [])
    payload = {
        "id": job["id"],
        "status": job["status"],
        "error": job["error"],
        "log_file": job["log_file"],
        "progress": job.get("progress"),
        "command": config.command,
        "handle": config.handle,
        "video_ids": video_ids,
        "video_count": len(video_ids),
    }

    if include_log:
        tail = _read_log_tail(job, max_lines=max_lines)
        payload["tail"] = tail
        payload["tail_text"] = "\n".join(tail)

    return payload


def _send_ws_message(ws: Server, payload: dict):
    try:
        ws.send(json.dumps(payload))
    except ConnectionClosed:
        _detach_ws(ws)
        raise


def _broadcast(payload: dict):
    message = json.dumps(payload)
    stale: list[Server] = []
    with _ws_clients_lock:
        clients = list(_ws_clients)
    for ws in clients:
        try:
            ws.send(message)
        except ConnectionClosed:
            stale.append(ws)
    if stale:
        for ws in stale:
            _detach_ws(ws)


def _unsubscribe_log(ws: Server):
    with _log_subscribers_lock:
        job_id = _ws_log_targets.pop(ws, None)
        if not job_id:
            return
        watchers = _log_subscribers.get(job_id)
        if not watchers:
            return
        watchers.discard(ws)
        if not watchers:
            _log_subscribers.pop(job_id, None)


def _subscribe_log(ws: Server, job_id: str):
    with _log_subscribers_lock:
        previous = _ws_log_targets.get(ws)
        if previous and previous != job_id:
            watchers = _log_subscribers.get(previous)
            if watchers:
                watchers.discard(ws)
                if not watchers:
                    _log_subscribers.pop(previous, None)
        _ws_log_targets[ws] = job_id
        watchers = _log_subscribers.setdefault(job_id, set())
        watchers.add(ws)


def _detach_ws(ws: Server):
    with _ws_clients_lock:
        _ws_clients.discard(ws)
    _unsubscribe_log(ws)


def _broadcast_job(job: dict, include_log: bool = False):
    _broadcast({"type": "job_update", "job": _serialize_job(job, include_log=include_log)})


def _send_jobs_snapshot(ws: Server):
    with _jobs_lock:
        jobs = sorted(_jobs.values(), key=lambda entry: entry["created"], reverse=True)
        snapshot = [_serialize_job(job) for job in jobs]
    _send_ws_message(ws, {"type": "jobs_snapshot", "jobs": snapshot})


def _send_job_log(ws: Server, job_id: str, job: dict, max_lines: int = 200):
    tail = _read_log_tail(job, max_lines=max_lines)
    _send_ws_message(
        ws,
        {
            "type": "job_log",
            "job_id": job_id,
            "status": job["status"],
            "progress": job.get("progress"),
            "tail": tail,
            "tail_text": "\n".join(tail),
        },
    )


def _broadcast_job_log(job_id: str, job: dict, max_lines: int = 200):
    with _log_subscribers_lock:
        targets = list(_log_subscribers.get(job_id, ()))
    if not targets:
        return
    tail = _read_log_tail(job, max_lines=max_lines)
    payload = {
        "type": "job_log",
        "job_id": job_id,
        "status": job["status"],
        "progress": job.get("progress"),
        "tail": tail,
        "tail_text": "\n".join(tail),
    }
    message = json.dumps(payload)
    stale: list[Server] = []
    for ws in targets:
        try:
            ws.send(message)
        except ConnectionClosed:
            stale.append(ws)
    for ws in stale:
        _detach_ws(ws)


def _handle_ws_message(ws: Server, data):
    if not isinstance(data, dict):
        return

    message_type = data.get("type")
    if message_type == "request_log":
        job_id = data.get("job_id")
        if not job_id:
            _send_ws_message(ws, {"type": "job_log", "error": "Job ID required."})
            return

        max_lines = data.get("lines")
        try:
            lines = max(1, int(max_lines or 200))
        except (TypeError, ValueError):
            lines = 200

        with _jobs_lock:
            job = _jobs.get(job_id)

        if not job:
            _send_ws_message(ws, {"type": "job_log", "job_id": job_id, "error": "Job not found."})
            return

        _subscribe_log(ws, job_id)
        _send_job_log(ws, job_id, job, max_lines=lines)
    elif message_type == "create_job":
        payload = data.get("payload")
        if not isinstance(payload, dict):
            payload = {}
        config, log_override, validation_error = _prepare_job_submission(payload)
        if validation_error:
            _send_ws_message(ws, {"type": "job_error", "error": validation_error})
            return
        try:
            job_id = _start_job(config, log_override)
        except Exception as exc:  # noqa: BLE001
            _send_ws_message(ws, {"type": "job_error", "error": str(exc)})
            return
        _send_ws_message(ws, {"type": "job_created", "job_id": job_id})


def _parse_video_ids(raw: str) -> list[str]:
    parts = re.split(r"[\s,]+", raw.strip()) if raw else []
    return [part for part in parts if part]


def _coerce_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip()
    return str(value).strip() or default


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "on", "yes"}
    return bool(value)


def _coerce_video_field(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        return "\n".join(str(item) for item in value if item)
    return str(value)


def _prepare_job_submission(data: Mapping[str, Any] | dict[str, Any] | None):
    if data is None:
        return None, None, "Missing submission payload."

    command = _coerce_str(data.get("command")).lower()
    if command not in {"channel", "shorts", "video"}:
        return None, None, "Invalid command selected."

    handle = _coerce_str(data.get("handle")) or None
    raw_video_ids = _coerce_video_field(data.get("video_ids"))
    video_ids = _parse_video_ids(raw_video_ids)

    if command in {"channel", "shorts"} and not handle:
        return None, None, "A channel handle is required for channel/shorts modes."
    if command == "video" and not video_ids:
        return None, None, "Provide at least one video ID or URL."

    out_dir = _coerce_str(data.get("out") or "yt") or "yt"
    log_file_input = _coerce_str(data.get("log_file"))
    log_level = _coerce_str(data.get("log_level") or "INFO") or "INFO"

    config = ArchiveConfig(
        command=command,
        handle=handle,
        video_ids=video_ids,
        out=out_dir,
        subs=_coerce_bool(data.get("subs")),
        no_cache=_coerce_bool(data.get("no_cache")),
        log_file=log_file_input or "logs/webui-placeholder.log",
        log_level=log_level.upper(),
        clear_screen=not _coerce_bool(data.get("no_clear")),
    )

    log_override = Path(log_file_input).expanduser() if log_file_input else None
    return config, log_override, None


def _start_job(config: ArchiveConfig, log_file: Path | None) -> str:
    job_id = str(uuid.uuid4())
    log_path = log_file.expanduser() if log_file else Path(f"logs/webui-{job_id}.log")

    job_record = {
        "id": job_id,
        "config": config,
        "status": "queued",
        "error": None,
        "log_file": str(log_path),
        "created": datetime.utcnow(),
        "progress": {
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
            "updated": datetime.utcnow().isoformat(),
        },
    }

    def runner():
        job_record["status"] = "running"

        def _sink(payload: dict):
            snapshot = {
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
                "updated": datetime.utcnow().isoformat(),
            }
            job_record["progress"] = snapshot
            _broadcast_job(job_record)
            _broadcast_job_log(job_id, job_record)

        register_progress_sink(_sink)
        try:
            config.log_file = str(log_path)
            run_archive(config)
            job_record["status"] = "completed"
            job_record["progress"] = {
                "label": "Completed",
                "detail": "",
                "percent": None,
                "downloaded": job_record.get("progress", {}).get("downloaded"),
                "total": job_record.get("progress", {}).get("total"),
                "eta": None,
                "speed": None,
                "show_transfer": False,
                "batch_index": job_record.get("progress", {}).get("batch_index"),
                "batch_total": job_record.get("progress", {}).get("batch_total"),
                "updated": datetime.utcnow().isoformat(),
            }
            _broadcast_job(job_record)
            _broadcast_job_log(job_id, job_record)
        except Exception as exc:
            job_record["status"] = "failed"
            job_record["error"] = str(exc)
            job_record["progress"] = {
                "label": "Error",
                "detail": str(exc),
                "percent": job_record.get("progress", {}).get("percent"),
                "downloaded": job_record.get("progress", {}).get("downloaded"),
                "total": job_record.get("progress", {}).get("total"),
                "eta": None,
                "speed": None,
                "show_transfer": False,
                "batch_index": job_record.get("progress", {}).get("batch_index"),
                "batch_total": job_record.get("progress", {}).get("batch_total"),
                "updated": datetime.utcnow().isoformat(),
            }
            _broadcast_job(job_record)
            _broadcast_job_log(job_id, job_record)
        finally:
            unregister_progress_sink(_sink)

    thread = threading.Thread(target=runner, daemon=True)

    with _jobs_lock:
        _jobs[job_id] = job_record
        thread.start()

    _broadcast_job(job_record)

    return job_id


@app.route("/", methods=["GET", "POST"])
def index():
    error = None
    if request.method == "POST":
        config, log_override, validation_error = _prepare_job_submission(request.form)
        if validation_error:
            error = validation_error
        else:
            job_id = _start_job(config, log_override)
            return redirect(url_for("index", job_id=job_id))

    with _jobs_lock:
        jobs = sorted(_jobs.values(), key=lambda entry: entry["created"], reverse=True)

    return render_template("index.html", jobs=jobs, error=error)


@app.route("/jobs/<job_id>.json")
def job_status(job_id: str):
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404

    max_lines = request.args.get("lines", type=int) or 40
    return jsonify(_serialize_job(job, include_log=True, max_lines=max_lines))


@sock.route("/ws")
def websocket_endpoint(ws: Server):  # pragma: no cover - network path
    with _ws_clients_lock:
        _ws_clients.add(ws)

    try:
        _send_jobs_snapshot(ws)
        while True:
            message = ws.receive()
            if message is None:
                continue
            if isinstance(message, bytes):
                try:
                    message = message.decode("utf-8")
                except UnicodeDecodeError:
                    continue
            try:
                data = json.loads(message)
            except (TypeError, ValueError):
                continue
            _handle_ws_message(ws, data)
    except ConnectionClosed:
        pass
    finally:
        _detach_ws(ws)


if __name__ == "__main__":  # pragma: no cover
    app.run(debug=True, threaded=True)
