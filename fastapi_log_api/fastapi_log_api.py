from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
import uuid
import os
import glob
import logging

LOGS_DIR = os.environ.get("LOGS_DIsR", "./logs")
LOG_LINE_FORMAT = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("log_api")

app = FastAPI(title="Log File Data Access API")
class LogEntry(BaseModel):
    id: str
    timestamp: datetime
    level: str
    component: str
    message: str
    source_file: Optional[str] = None
    line_number: Optional[int] = None

class LogsResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: List[LogEntry]

class StatsResponse(BaseModel):
    total_entries: int
    by_level: Dict[str, int]
    by_component: Dict[str, int]

_LOGS: List[LogEntry] = []
_LOG_MAP: Dict[str, LogEntry] = {}


def _make_id(file_path: str, line_no: int, timestamp_str: str) -> str:
    name = f"{os.path.abspath(file_path)}:{line_no}:{timestamp_str}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, name))


def _parse_log_line(line: str):
    parts = line.rstrip("\n").split("\t", 3)
    if len(parts) < 4:
        raise ValueError("log line does not have 4 tab-separated fields")
    ts_str, level, component, message = parts
    # parse timestamp
    try:
        ts = datetime.strptime(ts_str.strip(), LOG_LINE_FORMAT)
    except ValueError as e:
        raise ValueError(f"invalid timestamp format: {e}")
    return ts, level.strip(), component.strip(), message.strip()


def load_logs(directory: str = LOGS_DIR) -> None:
    global _LOGS, _LOG_MAP
    _LOGS = []
    _LOG_MAP = {}

    if not os.path.exists(directory):
        logger.warning("Log directory '%s' does not exist. No logs loaded.", directory)
        return

    pattern = os.path.join(directory, "*")
    files = sorted(glob.glob(pattern))
    logger.info("Loading logs from directory '%s' (%d files found)", directory, len(files))

    for file_path in files:
        if os.path.isdir(file_path):
            continue
        try:
            with open(file_path, "r", encoding="utf-8") as fh:
                for i, line in enumerate(fh, start=1):
                    if not line.strip():
                        continue
                    try:
                        ts, level, component, message = _parse_log_line(line)
                        ts_str = ts.strftime(LOG_LINE_FORMAT)
                        entry_id = _make_id(file_path, i, ts_str)
                        entry = LogEntry(
                            id=entry_id,
                            timestamp=ts,
                            level=level,
                            component=component,
                            message=message,
                            source_file=os.path.basename(file_path),
                            line_number=i,
                        )
                        _LOGS.append(entry)
                        _LOG_MAP[entry_id] = entry
                    except ValueError as e:
                        logger.warning("Skipping malformed line %d in %s: %s", i, file_path, e)
        except Exception as e:
            logger.exception("Failed to read file %s: %s", file_path, e)

    _LOGS.sort(key=lambda x: x.timestamp)
    logger.info("Loaded %d log entries", len(_LOGS))

load_logs()


def _apply_filters(items: List[LogEntry], level: Optional[str], component: Optional[str],
                   start_time: Optional[str], end_time: Optional[str]) -> List[LogEntry]:
    res = items
    if level:
        res = [e for e in res if e.level.lower() == level.lower()]
    if component:
        res = [e for e in res if e.component.lower() == component.lower()]
    if start_time:
        try:
            st = datetime.strptime(start_time, LOG_LINE_FORMAT)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"start_time must be in format '{LOG_LINE_FORMAT}'")
        res = [e for e in res if e.timestamp >= st]
    if end_time:
        try:
            et = datetime.strptime(end_time, LOG_LINE_FORMAT)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"end_time must be in format '{LOG_LINE_FORMAT}'")
        res = [e for e in res if e.timestamp <= et]
    return res


@app.get("/logs", response_model=LogsResponse)
def get_logs(
    level: Optional[str] = Query(None, description="Filter by log level, e.g., ERROR"),
    component: Optional[str] = Query(None, description="Filter by component, e.g., UserAuth"),
    start_time: Optional[str] = Query(None, description=f"Filter after timestamp ({LOG_LINE_FORMAT})"),
    end_time: Optional[str] = Query(None, description=f"Filter before timestamp ({LOG_LINE_FORMAT})"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of items to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
):
    filtered = _apply_filters(_LOGS, level, component, start_time, end_time)
    total = len(filtered)
    # pagination
    slice_items = filtered[offset: offset + limit]
    return LogsResponse(total=total, limit=limit, offset=offset, items=slice_items)


@app.get("/logs/stats", response_model=StatsResponse)
def get_stats():
    total_entries = len(_LOGS)
    by_level: Dict[str, int] = {}
    by_component: Dict[str, int] = {}
    for e in _LOGS:
        by_level[e.level] = by_level.get(e.level, 0) + 1
        by_component[e.component] = by_component.get(e.component, 0) + 1
    return StatsResponse(total_entries=total_entries, by_level=by_level, by_component=by_component)


@app.get("/logs/{log_id}", response_model=LogEntry)
def get_log_by_id(log_id: str):
    entry = _LOG_MAP.get(log_id)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Log entry with id '{log_id}' not found")
    return entry


@app.get("/reload")
def reload_logs():
    load_logs()
    return JSONResponse(content={"message": "logs reloaded", "count": len(_LOGS)})


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("fastapi_log_api:app", host="0.0.0.0", port=8000, reload=True)
