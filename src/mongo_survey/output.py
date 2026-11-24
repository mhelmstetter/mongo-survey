"""Capture file writer.

One record per line, relaxed Extended JSON (readable by jq, mongoimport, and
bson.json_util). The first line is a header, the last a footer. Records are
flushed as they are produced, so an interrupted run is readable up to its last
complete line.

Record shape (mirrors getMongoData.js so existing parsers adapt easily):
  {type:"record", section, subsection, host, shard, target, command,
   ts:{start,end}, output, error}
"""
from __future__ import annotations

import datetime as dt
import gzip
import io
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, List, Optional

from bson import json_util
from bson.json_util import JSONOptions, JSONMode
from pymongo.errors import PyMongoError, OperationFailure

_JSON_OPTIONS = JSONOptions(json_mode=JSONMode.RELAXED, tz_aware=True, tzinfo=dt.timezone.utc)


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def dumps(doc: Any) -> str:
    return json_util.dumps(doc, json_options=_JSON_OPTIONS)


def loads(line: str) -> Any:
    return json_util.loads(line, json_options=_JSON_OPTIONS)


def describe_error(exc: BaseException) -> Dict[str, Any]:
    err: Dict[str, Any] = {"type": type(exc).__name__, "message": str(exc)}
    if isinstance(exc, OperationFailure):
        err["code"] = exc.code
        if exc.details and exc.details.get("codeName"):
            err["codeName"] = exc.details["codeName"]
    return err


class CaptureWriter:
    def __init__(self, path: str, compress: bool):
        self.path = path
        raw = open(path, "wb")
        self._fh: io.TextIOBase
        if compress:
            self._fh = io.TextIOWrapper(gzip.GzipFile(fileobj=raw, mode="wb"), encoding="utf-8")
        else:
            self._fh = io.TextIOWrapper(raw, encoding="utf-8")
        self._lock = threading.Lock()
        self._last_flush = time.monotonic()
        self.errors: List[Dict[str, Any]] = []
        self.records = 0

    def write(self, doc: Dict[str, Any]) -> None:
        line = dumps(doc)
        with self._lock:
            self._fh.write(line)
            self._fh.write("\n")
            self.records += 1
            if time.monotonic() - self._last_flush > 2:
                self._fh.flush()
                self._last_flush = time.monotonic()

    def record(
        self,
        section: str,
        subsection: str,
        output: Any = None,
        *,
        error: Optional[Dict[str, Any]] = None,
        host: Optional[str] = None,
        shard: Optional[str] = None,
        target: Optional[str] = None,
        command: Any = None,
        start: Optional[dt.datetime] = None,
        end: Optional[dt.datetime] = None,
        **extra: Any,
    ) -> None:
        now = utcnow()
        doc: Dict[str, Any] = {
            "type": "record",
            "section": section,
            "subsection": subsection,
            "host": host,
            "shard": shard,
            "target": target,
            "command": command,
            "ts": {"start": start or now, "end": end or now},
            "output": output,
            "error": error,
        }
        doc.update(extra)
        if error is not None:
            with self._lock:
                self.errors.append(
                    {"section": section, "subsection": subsection, "host": host,
                     "target": target, "command": command, "error": error}
                )
        self.write(doc)

    @contextmanager
    def capture(self, section: str, subsection: str, **meta: Any) -> Iterator[Dict[str, Any]]:
        """Time a block and record its result or its error.

        Usage:
            with w.capture("server_info", "serverStatus", host=h, command=cmd) as rec:
                rec["output"] = client.admin.command(cmd)

        A PyMongoError is recorded and swallowed (the run continues and the
        error lands in the footer + exit code). Anything else propagates.
        """
        holder: Dict[str, Any] = {"output": None, "extra": {}}
        start = utcnow()
        try:
            yield holder
        except PyMongoError as exc:
            self.record(section, subsection, None, error=describe_error(exc), start=start,
                        end=utcnow(), **meta, **holder["extra"])
            holder["failed"] = True
            return
        self.record(section, subsection, holder["output"], start=start, end=utcnow(),
                    **meta, **holder["extra"])

    def run(self, section: str, subsection: str, fn: Callable[[], Any], **meta: Any) -> Any:
        """Run fn(), record its output (or error), return output or None on error."""
        with self.capture(section, subsection, **meta) as rec:
            rec["output"] = fn()
        return rec["output"]

    def close(self) -> None:
        with self._lock:
            self._fh.flush()
            self._fh.close()


def read_capture(path: str) -> Iterator[Dict[str, Any]]:
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as fh:  # type: ignore[operator]
        for n, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield loads(line)
            except ValueError as exc:
                raise ValueError(f"{path}:{n}: unparseable line ({exc})") from exc
