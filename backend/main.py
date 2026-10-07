
from __future__ import annotations
import asyncio
import threading
import time
from collections import deque
from email.message import EmailMessage
import json
import math
import operator
import os
from pathlib import Path
import re
import smtplib
import ssl
import statistics
from typing import Any, Dict, List, Optional, Union
from fastapi import BackgroundTasks, FastAPI, File, HTTPException, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from mcp import Client
try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None
# ============================================================
# APPLICATION & CORS
# ============================================================
app = FastAPI(title="Dynamic Sensor Tools Agent")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ============================================================
# PATHS & RUNTIME DATA STATE
# ============================================================
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DEFAULT_JSON_PATH = DATA_DIR / "JsonData2.txt"
execution_mode: str = "static"
current_json_data: Optional[Dict[str, Any]] = None
MAX_LIVE_BUFFER = 1000
live_telemetry_buffer: deque = deque(maxlen=MAX_LIVE_BUFFER)
live_sensors_metadata: Dict[str, Dict[str, Any]] = {}
# Shared by static and live modes.
thresholds: Dict[str, Dict[str, Any]] = {}
alert_history: List[Dict[str, Any]] = []
alert_keys_seen: set[str] = set()
threshold_states: Dict[str, Dict[str, Any]] = {}
# A live threshold stops the live plot/monitoring at the first FALSE -> TRUE crossing.
live_monitoring_stopped: Dict[str, Dict[str, Any]] = {}
# Cached sensor registry. Rebuilding the registry for every row is expensive
# because the registry also inspects the active data.
_sensor_registry_cache: Optional[List[Dict[str, Any]]] = None
_sensor_registry_cache_key: Any = None
# ============================================================
# OPTIONAL NOTIFICATIONS
# ============================================================
def load_local_env() -> None:
    """Load a local .env file without overriding existing environment variables."""
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('\"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
    except Exception as exc:
        print(f"[ENV] Failed to read {env_path}: {exc}")
load_local_env()
def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}
ALERT_EMAIL_ENABLED = env_bool("ALERT_EMAIL_ENABLED", True)
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", SMTP_USERNAME)
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "sirinizampatnam6@gmail.com")
NOTIFICATION_TIMEOUT = float(os.getenv("NOTIFICATION_TIMEOUT", "15"))
# ============================================================
# MCP CONNECTION
# ============================================================
MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://127.0.0.1:8001/mcp")
MCP_ALERT_CALLBACK_URL = os.getenv("MCP_ALERT_CALLBACK_URL","http://127.0.0.1:8000/api/mcp/alert",)
MCP_TIMEOUT = float(os.getenv("MCP_TIMEOUT", "10"))
# ============================================================
# MQTT LIVE TELEMETRY
# ============================================================
MQTT_ENABLED = env_bool("MQTT_ENABLED", False)
MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "127.0.0.1")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "sensors/#")
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "dynamic-sensor-backend")
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "60"))
_mqtt_client = None
_mqtt_thread = None
_mqtt_started = False
_main_event_loop = None
print("[EMAIL CONFIG]", json.dumps({
    "smtp_host_configured": bool(SMTP_HOST),
    "smtp_port": SMTP_PORT,
    "smtp_username_configured": bool(SMTP_USERNAME),
    "smtp_password_configured": bool(SMTP_PASSWORD),
    "email_from_configured": bool(ALERT_EMAIL_FROM),
    "email_recipient": ALERT_EMAIL_TO,
    "email_enabled": ALERT_EMAIL_ENABLED,
}, default=str))
# ============================================================
# WEBSOCKET MANAGER
# ============================================================
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
    async def broadcast(self, message: dict):
        dead: List[WebSocket] = []
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                dead.append(connection)
        for connection in dead:
            self.disconnect(connection)
manager = ConnectionManager()
# ============================================================
# REQUEST SCHEMAS
# ============================================================
class ModeSwitchRequest(BaseModel):
    mode: str
class TelemetryPacket(BaseModel):
    timestamp: Optional[Union[str, float, int]] = None
    readings: Dict[str, Union[float, int, str, None]]
class ThresholdRequest(BaseModel):
    sensor: str
    operator: str
    threshold: float
    enabled: bool = True
    notify_email: bool = True
    email_to: Optional[str] = None
class ChatRequest(BaseModel):
    question: str
    selected_sensor: Optional[str] = None
    view: Optional[str] = None
class MCPToolRequest(BaseModel):
    tool: str
    args: Dict[str, Any] = {}
class NotificationTestRequest(BaseModel):
    email: bool = False
    email_to: Optional[str] = None
class CheckThresholdRequest(BaseModel):
    sensor: str
    operator: str
    threshold: float
# ============================================================
# GENERAL HELPERS
# ============================================================
def is_numeric(value: Any) -> bool:
    if value is None or isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return math.isfinite(float(value))
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned or cleaned.upper() in {"NA", "N/A", "NULL", "NONE", "NAN", "-"}:
            return False
        try:
            return math.isfinite(float(cleaned))
        except ValueError:
            return False
    return False
def get_timestamp_from_row(row: Dict[str, Any]) -> Any:
    preferred = ("timestamp", "time", "datetime", "date", "x")
    lowered = {str(k).strip().lower(): k for k in row}
    for name in preferred:
        if name in lowered:
            return row[lowered[name]]
    return None
def normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())
def ignored_field(key: str) -> bool:
    return normalize_name(key) in {"x", "timestamp", "time", "datetime", "date", "id"}
def get_active_data_rows() -> List[Dict[str, Any]]:
    if execution_mode == "live":
        return [dict(row) for row in live_telemetry_buffer]
    if not current_json_data:
        return []
    rows = current_json_data.get("data", [])
    if not isinstance(rows, list):
        return []
    return [dict(row) for row in rows if isinstance(row, dict)]
# ============================================================
# DYNAMIC SENSOR REGISTRY / FIELD RESOLUTION
# ============================================================
def invalidate_sensor_registry_cache() -> None:
    global _sensor_registry_cache, _sensor_registry_cache_key
    _sensor_registry_cache = None
    _sensor_registry_cache_key = None
def get_sensor_registry() -> List[Dict[str, Any]]:
    """Build a dynamic sensor registry from explicit metadata or numeric row fields."""
    global _sensor_registry_cache, _sensor_registry_cache_key
    sensors_signature = ()
    explicit_sensors = []
    if current_json_data and isinstance(current_json_data.get("sensors"), list):
        explicit_sensors = current_json_data.get("sensors", [])
        sensors_signature = tuple(str(item) for item in explicit_sensors if isinstance(item, dict))
    rows = get_active_data_rows()
    row_signature = (len(rows), tuple(sorted(str(k) for r in rows[:3] for k in r.keys())))
    cache_key = (id(current_json_data), execution_mode, sensors_signature, row_signature)
    if _sensor_registry_cache is not None and _sensor_registry_cache_key == cache_key:
        return _sensor_registry_cache
    registry: List[Dict[str, Any]] = []
    seen = set()
    def add_entry(label, live_id=None, metadata=None):
        if label is None or not str(label).strip():
            return
        label_s = str(label).strip()
        key = normalize_name(label_s)
        if not key or key in seen:
            return
        seen.add(key)
        live_s = str(live_id).strip() if live_id not in (None, "") else None
        aliases = [label_s] + ([live_s] if live_s else [])
        registry.append({"label": label_s,"live_id": live_s, "aliases": list(dict.fromkeys(aliases)), "metadata": dict(metadata or {}),})
    # Prefer declared sensor metadata when it exists. This avoids showing
    # unrelated row-only properties such as Prop1/Prop2/Prop3.
    for item in explicit_sensors:
        if not isinstance(item, dict):
            continue
        label = item.get("SensorLabelName") or item.get("sensor") or item.get("name")
        live_id = (item.get("MappedLiveSensorID") or item.get("mappedLiveSensorID")
                   or item.get("liveSensorId") or item.get("live_id"))
        add_entry(label, live_id, item)
    # If there is no explicit sensor list, dynamically discover numeric fields.
    if not registry:
        field_values: Dict[str, List[Any]] = {}
        for row in rows:
            for key, value in row.items():
                if ignored_field(str(key)):
                    continue
                field_values.setdefault(str(key), []).append(value)
        for field, values in field_values.items():
            if any(is_numeric(value) for value in values):
                add_entry(field)
    _sensor_registry_cache = registry
    _sensor_registry_cache_key = cache_key
    return registry
def get_sensor_names() -> List[str]:
    return [entry["label"] for entry in get_sensor_registry()]
def resolve_sensor_name(query_str: Optional[str]) -> Optional[str]:
    """Resolve user text to the logical sensor label, including live-ID aliases."""
    if not query_str:
        return None
    query_norm = normalize_name(query_str)
    if not query_norm:
        return None
    registry = get_sensor_registry()
    # Exact label/live-ID alias match.
    for entry in registry:
        for alias in entry["aliases"]:
            if normalize_name(alias) == query_norm:
                return entry["label"]
    # Match an alias as a complete-ish part of the question.
    candidates: List[tuple[int, str]] = []
    for entry in registry:
        for alias in entry["aliases"]:
            alias_norm = normalize_name(alias)
            if alias_norm and (alias_norm in query_norm or query_norm in alias_norm):
                candidates.append((len(alias_norm), entry["label"]))
                break
    if candidates:
        candidates.sort(reverse=True)
        if len(candidates) == 1 or candidates[0][0] > candidates[1][0]:
            return candidates[0][1]
    # Token overlap fallback.
    q_tokens = set(re.findall(r"[a-z0-9]+", str(query_str).lower()))
    scored: List[tuple[int, int, str]] = []
    for entry in registry:
        best = 0
        for alias in entry["aliases"]:
            tokens = set(re.findall(r"[a-z0-9]+", alias.lower()))
            best = max(best, len(q_tokens & tokens))
        if best:
            scored.append((best, len(normalize_name(entry["label"])), entry["label"]))
    if scored:
        scored.sort(reverse=True)
        if len(scored) == 1 or scored[0][0] > scored[1][0]:
            return scored[0][2]
    return None
def resolve_data_field(sensor_name: str, row: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Resolve a logical sensor or live ID to the actual field present in a row."""
    if row:
        # Exact normalized row-key match.
        target = normalize_name(sensor_name)
        for key in row:
            if not ignored_field(str(key)) and normalize_name(str(key)) == target:
                return str(key)
    logical = resolve_sensor_name(sensor_name) or sensor_name
    logical_norm = normalize_name(logical)
    registry = get_sensor_registry()
    for entry in registry:
        aliases = entry["aliases"]
        if any(normalize_name(alias) == logical_norm for alias in aliases):
            candidates = [entry.get("label"), entry.get("live_id")] + aliases
            if row:
                for candidate in candidates:
                    if candidate is None:
                        continue
                    for key in row:
                        if normalize_name(str(key)) == normalize_name(str(candidate)):
                            return str(key)
            return entry.get("label") or entry.get("live_id")
    # If the resolved logical label is itself present in a current row, use it.
    if row:
        for key in row:
            if normalize_name(str(key)) == logical_norm:
                return str(key)
    return None
def get_sensor_metadata(sensor_name: str) -> Dict[str, Any]:
    resolved = resolve_sensor_name(sensor_name) or sensor_name
    target = normalize_name(resolved)
    for entry in get_sensor_registry():
        if normalize_name(entry["label"]) == target:
            metadata = dict(entry.get("metadata") or {})
            if entry.get("live_id") and "MappedLiveSensorID" not in metadata:
                metadata["MappedLiveSensorID"] = entry["live_id"]
            metadata.setdefault("SensorLabelName", entry["label"])
            metadata.setdefault("source", execution_mode)
            return metadata
    return {"sensor": resolved, "source": execution_mode}
# ============================================================
# THRESHOLD ENGINE
# ============================================================
OPERATORS = {
    ">": operator.gt,
    "<": operator.lt,
    ">=": operator.ge,
    "<=": operator.le,
    "=": operator.eq,
    "==": operator.eq,
    "!=": operator.ne,
}
def normalize_operator(value: str) -> Optional[str]:
    q = str(value).strip().lower()
    mapping = {
        ">": ">", "above": ">", "over": ">", "greaterthan": ">", "greater": ">", "exceeds": ">", "exceed": ">",
        "<": "<", "below": "<", "under": "<", "lessthan": "<", "less": "<",
        ">=": ">=", "atleast": ">=", "minimum": ">=",
        "<=": "<=", "atmost": "<=", "maximum": "<=",
        "=": "=", "==": "=", "equals": "=", "equal": "=",
        "!=": "!=", "notequal": "!=", "different": "!=",
    }
    return mapping.get(re.sub(r"[^a-z0-9<>=!]+", "", q))
def get_threshold(sensor_name: str) -> Optional[Dict[str, Any]]:
    for configured_sensor, config in thresholds.items():
        if normalize_name(configured_sensor) == normalize_name(resolve_sensor_name(sensor_name) or sensor_name):
            return config
    return None
def make_threshold_state_key(sensor: str, config: Dict[str, Any]) -> str:
    op = normalize_operator(config.get("operator", ">")) or ">"
    return f"{normalize_name(sensor)}|{op}|{float(config.get('threshold', 0.0))}"
def clear_threshold_state(sensor: Optional[str] = None) -> None:
    if sensor is None:
        threshold_states.clear()
        live_monitoring_stopped.clear()
        return
    resolved = resolve_sensor_name(sensor) or sensor
    prefix = normalize_name(resolved) + "|"
    for key in list(threshold_states):
        if key.startswith(prefix):
            threshold_states.pop(key, None)
    for key in list(live_monitoring_stopped):
        if key.startswith(prefix):
            live_monitoring_stopped.pop(key, None)
def set_threshold_state(sensor: str,config: Dict[str, Any], matched: bool, value: Any, timestamp: Any, initialized: bool = True,) -> None:
    threshold_states[make_threshold_state_key(sensor, config)] = {
        "matched": bool(matched),"value": float(value) if is_numeric(value) else None,"timestamp": timestamp,"initialized": bool(initialized),}
def get_threshold_state(sensor: str, config: Dict[str, Any]) -> Dict[str, Any]:
    return threshold_states.get(make_threshold_state_key(sensor, config), {
        "matched": False, "value": None, "timestamp": None, "initialized": False, })
def mark_live_monitoring_stopped(sensor: str, config: Dict[str, Any], event: Dict[str, Any]) -> None:
    live_monitoring_stopped[make_threshold_state_key(sensor, config)] = {
        "stopped": True,"timestamp": event.get("timestamp"),"value": event.get("value"),}
def is_live_monitoring_stopped(sensor: str, config: Dict[str, Any]) -> bool:
    return bool(live_monitoring_stopped.get(make_threshold_state_key(sensor, config), {}).get("stopped"))
def threshold_matches(value: Any, config: Dict[str, Any]) -> bool:
    if not is_numeric(value) or not config.get("enabled", True):
        return False
    op = normalize_operator(config.get("operator", ">"))
    if not op:
        return False
    return OPERATORS[op](float(value), float(config["threshold"]))
def make_alert_key(sensor: str, config: Dict[str, Any], row: Dict[str, Any], actual_field: Optional[str] = None) -> str:
    field = actual_field or sensor
    timestamp = get_timestamp_from_row(row)
    value = row.get(field)
    return "|".join([
        normalize_name(sensor),str(normalize_operator(config.get("operator", ">"))),str(config.get("threshold")),
        str(timestamp),str(value),])
def create_alert_event(sensor: str,config: Dict[str, Any],row: Dict[str, Any],actual_field: Optional[str] = None,previous_state: Optional[Dict[str, Any]] = None,) -> Dict[str, Any]:
    field = actual_field or sensor
    value = float(row[field])
    previous_state = previous_state or {}
    return {
        "sensor": sensor,"field": field, "operator": normalize_operator(config.get("operator", ">")), "threshold": float(config["threshold"]),
        "value": value,"timestamp": get_timestamp_from_row(row),"mode": execution_mode,"crossing": True,"notify_email": bool(config.get("notify_email", True)),
        "email_to": config.get("email_to") or ALERT_EMAIL_TO,"previous_value": previous_state.get("value"),"previous_timestamp": previous_state.get("timestamp"),
        "message": ( f"Threshold crossed: sensor '{sensor}' value {value} " f"{normalize_operator(config.get('operator', '>'))} {float(config['threshold'])}."
        ),}
def evaluate_row_for_alerts(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Generate an alert only on a FALSE -> TRUE threshold transition.
    The first numeric reading is treated as the initial FALSE state. Once a
    live threshold crosses, live monitoring is stopped for that threshold.
    Static processing can use this function row-by-row and stop at the first
    returned event.
    """
    events: List[Dict[str, Any]] = []
    for configured_sensor, config in list(thresholds.items()):
        if not config.get("enabled", True):
            continue
        sensor = resolve_sensor_name(configured_sensor) or configured_sensor
        if execution_mode == "live" and is_live_monitoring_stopped(sensor, config):
            continue
        field = resolve_data_field(sensor, row)
        if not field or not is_numeric(row.get(field)):
            continue
        current_match = threshold_matches(row.get(field), config)
        state = get_threshold_state(sensor, config)
        # A new threshold starts from FALSE. This makes the first matching
        # reading a crossing, while subsequent matching readings do not send
        # another email until a FALSE reading occurs.
        previous_match = bool(state.get("matched", False))
        crossing = current_match and not previous_match
        # Always update the state, including FALSE readings.
        set_threshold_state(sensor,config,current_match,row.get(field),get_timestamp_from_row(row),initialized=True,)
        if not crossing:
            continue
        event = create_alert_event(sensor, config, row, field, state)
        events.append(event)
        # Live graph/monitoring stops at the first crossing. The crossing
        # sample itself has already been added to the live buffer.
        if execution_mode == "live":
            mark_live_monitoring_stopped(sensor, config, event)
    return events
def analyze_threshold(sensor_name: str, op_str: str, threshold_val: float) -> Dict[str, Any]:
    """Check rows sequentially and STOP at the first row satisfying the condition.
    This matches List View behavior: rows are evaluated from the beginning of
    the active dataset, and once the threshold condition is satisfied, no
    later rows are inspected for this threshold-condition calculation.
    """
    op = normalize_operator(op_str)
    if not op or op not in OPERATORS:
        return {"error": f"Invalid operator '{op_str}'"}
    resolved = resolve_sensor_name(sensor_name) or sensor_name
    rows = get_active_data_rows()
    crossings: List[Dict[str, Any]] = []
    checked = 0
    rows_with_sensor = 0
    matching = 0
    for row in rows:
        field = resolve_data_field(resolved, row)
        if not field:
            continue
        rows_with_sensor += 1
        value = row.get(field)
        if not is_numeric(value):
            continue
        checked += 1
        numeric = float(value)
        current_match = bool(OPERATORS[op](numeric, float(threshold_val)))
        if current_match:
            matching = 1
            crossings.append({"timestamp": get_timestamp_from_row(row),"value": numeric,})
            # IMPORTANT: exactly like List View, stop at the first row that
            # satisfies the configured condition. Do not inspect later rows.
            break
    if rows and rows_with_sensor == 0:
        return {"error": f"No data field found for sensor '{resolved}' in {execution_mode} mode."}
    return {
        "sensor": resolved,"operator": op,"threshold": float(threshold_val),"mode": execution_mode,
        "total_readings_checked": checked,"rows_with_sensor": rows_with_sensor,"numeric_readings_checked": checked,
        "crossed": bool(crossings),"crossed_count": len(crossings),
        # Number of rows inspected through and including the first matching
        # row. This is the value that must agree with List View.
        "condition_checked_count": checked,"matching_readings_count": matching,"not_crossed_count": max(0, checked - matching),
        "crossings": crossings,}
def _chat_rows_for_scope(sensor: str, view: Optional[str] = None, scope: str = "view") -> List[Dict[str, Any]]:
    """Return exactly the rows that a chatbot calculation should use."""
    resolved = resolve_sensor_name(sensor)
    if not resolved:
        return []
    view = view if view in {"list", "graph"} else "graph"
    rows = list(get_view_rows(resolved, view))
    scope = (scope or "view").lower()
    if scope == "view":
        return rows
    config = get_threshold(resolved)
    if not config or not config.get("enabled", True):
        return rows
    all_rows = get_active_data_rows()
    crossing_index = None
    previous_match = False
    for idx, row in enumerate(all_rows):
        field = resolve_data_field(resolved, row)
        if not field or not is_numeric(row.get(field)):
            continue
        current_match = threshold_matches(row.get(field), config)
        if current_match and not previous_match:
            crossing_index = idx
            break
        previous_match = current_match
    if crossing_index is None:
        return [] if scope == "at" else (all_rows if scope in {"before", "after"} else rows)
    if scope == "before":
        return all_rows[:crossing_index]
    if scope == "at":
        return all_rows[crossing_index:crossing_index + 1]
    if scope == "after":
        return all_rows[crossing_index + 1:]
    return rows
def compute_statistics(sensor_name: str, metric: str, view: Optional[str] = None, scope: str = "view") -> Dict[str, Any]:
    resolved = resolve_sensor_name(sensor_name)
    if not resolved:
        return {"error": f"Sensor '{sensor_name}' was not found."}
    rows = _chat_rows_for_scope(resolved, view, scope)
    values: List[float] = []
    for row in rows:
        field = resolve_data_field(resolved, row)
        if field and is_numeric(row.get(field)):
            values.append(float(row[field]))
    if not values:
        return {"error": f"No numeric data available for sensor '{resolved}' in {execution_mode} mode."}
    metric = metric.lower()
    if metric == "max": result = max(values)
    elif metric == "min": result = min(values)
    elif metric in {"average", "avg", "mean"}: result = sum(values) / len(values)
    elif metric == "median": result = statistics.median(values)
    elif metric == "sum": result = sum(values)
    elif metric == "count": result = len(values)
    elif metric == "range": result = max(values) - min(values)
    elif metric in {"stddev", "std", "standarddeviation"}:
        if len(values) < 2: return {"error": "At least two numeric readings are required for standard deviation."}
        result = statistics.stdev(values)
    elif metric in {"variance", "var"}:
        if len(values) < 2: return {"error": "At least two numeric readings are required for variance."}
        result = statistics.variance(values)
    else: return {"error": f"Unsupported metric '{metric}'."}
    return {"sensor": resolved, "metric": metric, "result": result, "mode": execution_mode, "view": view or "graph", "scope": scope, "count": len(values)}
def tool_list_sensors() -> Dict[str, Any]:
    sensors = get_sensor_names()
    return {"mode": execution_mode, "count": len(sensors), "sensors": sensors}
def tool_threshold_sensor() -> Dict[str, Any]:
    """Return the sensor label(s) that currently have an active threshold."""
    configured = [resolve_sensor_name(name) or name for name, config in thresholds.items() if config.get("enabled", True)]
    # Keep the result deterministic and avoid duplicate labels.
    sensors = list(dict.fromkeys(configured))
    return { "mode": execution_mode,"count": len(sensors),"sensors": sensors,}
def tool_sensor_metadata(sensor: str) -> Dict[str, Any]:
    resolved = resolve_sensor_name(sensor) or sensor
    return {"sensor": resolved, "metadata": get_sensor_metadata(resolved), "mode": execution_mode}
def tool_latest_value(sensor: str, timestamp_only: bool = False) -> Dict[str, Any]:
    resolved = resolve_sensor_name(sensor)
    if not resolved:
        return {"error": f"Sensor '{sensor}' was not found."}
    rows = get_active_data_rows()
    for row in reversed(rows):
        field = resolve_data_field(resolved, row)
        if field and is_numeric(row.get(field)):
            return {
                "sensor": resolved, "field": field, "value": float(row[field]),
                "timestamp": get_timestamp_from_row(row), "mode": execution_mode,
                "timestamp_only": timestamp_only,}
    return {"error": f"No numeric reading found for sensor '{resolved}' in {execution_mode} mode."}
def tool_statistics(sensor: str, metric: str, view: Optional[str] = None, scope: str = "view") -> Dict[str, Any]:
    return compute_statistics(sensor, metric, view, scope)
def tool_values(sensor: str, view: Optional[str] = None, scope: str = "view") -> Dict[str, Any]:
    """Return sensor values for the requested view/scope without changing data processing logic."""
    resolved = resolve_sensor_name(sensor)
    if not resolved:
        return {"error": f"Sensor '{sensor}' was not found."}
    rows = _chat_rows_for_scope(resolved, view, scope)
    values = []
    for row in rows:
        field = resolve_data_field(resolved, row)
        if field and is_numeric(row.get(field)):
            values.append({"timestamp": get_timestamp_from_row(row),"value": float(row[field]),})
    return {"sensor": resolved, "mode": execution_mode,"view": view or "graph","scope": scope,"values": values,"count": len(values),}
def tool_reading_count( sensor: str, view: Optional[str] = None, scope: str = "view",) -> Dict[str, Any]:
    """Count numeric readings using the same live/static rows and scope as the chatbot."""
    resolved = resolve_sensor_name(sensor)
    if not resolved:
        return {"error": f"Sensor '{sensor}' was not found."}
    rows = _chat_rows_for_scope(resolved, view, scope)
    count = 0
    for row in rows:
        field = resolve_data_field(resolved, row)
        if field and is_numeric(row.get(field)):
            count += 1
    return {"sensor": resolved,"mode": execution_mode,"view": view or "graph","scope": scope,"count": count,}
def tool_threshold_configuration(sensor: Optional[str] = None) -> Dict[str, Any]:
    if sensor:
        resolved = resolve_sensor_name(sensor) or sensor
        config = get_threshold(resolved)
        return {"sensor": resolved, "threshold": config}
    return {"thresholds": thresholds, "count": len(thresholds)}
def tool_set_threshold(sensor: str, operator_str: str, threshold: float, enabled: bool = True,notify_email: bool = True, email_to: Optional[str] = None,) -> Dict[str, Any]:
    resolved = resolve_sensor_name(sensor)
    if not resolved:
        return {"error": f"Sensor '{sensor}' was not found. Available sensors: {', '.join(get_sensor_names())}"}
    op = normalize_operator(operator_str)
    if not op:
        return {"error": f"Unsupported operator '{operator_str}'. Use >, <, >=, <=, = or !=."}
    if notify_email and not (email_to or ALERT_EMAIL_TO):
        return {"error": "Email notification is enabled but no recipient email was provided."}
    # A threshold selection represents the current Alert Agent context.
    # Clear all previous threshold/alert context before installing the new
    # sensor + operator + threshold so the UI can never show stale alerts
    # from a previously selected sensor or threshold.
    thresholds.clear()
    alert_history.clear()
    alert_keys_seen.clear()
    clear_threshold_state()
    config = {"operator": op,"threshold": float(threshold),"enabled": bool(enabled),"notify_email": bool(notify_email),
        "email_to": email_to.strip() if isinstance(email_to, str) and email_to.strip() else ALERT_EMAIL_TO,}
    thresholds[resolved] = config
    # Do not pre-read a live sample here. The first incoming sample is the
    # initial FALSE state, and the first FALSE -> TRUE transition is the alert.
    return {"sensor": resolved,"threshold": config,
        "message": (
            "Threshold configured. Notification is sent only when the threshold "
            "changes from FALSE to TRUE. Static mode stops at the first crossing; "
            "live mode stops plotting/monitoring at the first crossing."),
        "notification": { "email_requested": bool(notify_email),
            "email_to": config.get("email_to") or ALERT_EMAIL_TO or None,}, }
def tool_threshold_analysis(sensor: str, operator_str: str, threshold: float) -> Dict[str, Any]:
    return analyze_threshold(sensor, operator_str, threshold)
def tool_threshold_timestamps(sensor: str,operator_str: Optional[str] = None,threshold: Optional[float] = None,
    first_only: bool = False,last_only: bool = False,view: Optional[str] = None,) -> Dict[str, Any]:
    """Find threshold matches using the same stop-at-first-match rule as List View."""
    resolved = resolve_sensor_name(sensor)
    if not resolved:
        return {"error": f"Sensor '{sensor}' was not found."}
    config = get_threshold(resolved)
    if threshold is None or operator_str is None:
        if not config:
            return {"error": f"No configured threshold exists for sensor '{resolved}'."}
        threshold = float(config["threshold"])
        operator_str = config["operator"]
    analysis = analyze_threshold(resolved, operator_str, float(threshold))
    if "error" in analysis:
        return analysis
    crossings = list(analysis.get("crossings", []))
    if first_only:
        crossings = crossings[:1]
    elif last_only:
        # Only the first satisfying row is evaluated by design, so the first
        # match is also the last available match for this calculation.
        crossings = crossings[:1]
    return {
        "sensor": resolved,"operator": analysis["operator"],"threshold": analysis["threshold"],"mode": execution_mode,
        "view": view or "graph","crossed_count": analysis.get("condition_checked_count", 0),"condition_checked_count": analysis.get("condition_checked_count", 0),
        "matching_readings_count": analysis.get("matching_readings_count", 0),"not_crossed_count": analysis.get("not_crossed_count", 0),
        "timestamps": [x["timestamp"] for x in crossings],"crossings": crossings,"first_only": first_only,"last_only": last_only,}
def tool_threshold_counts(sensor: str,operator_str: Optional[str] = None,threshold: Optional[float] = None,count_type: str = "both",
    view: Optional[str] = None,) -> Dict[str, Any]:
    """Count rows only until the first row satisfies the threshold condition."""
    result = tool_threshold_timestamps(sensor, operator_str, threshold, view=view)
    if "error" in result:
        return result
    if count_type not in {"crossed", "not_crossed", "both"}:
        return {"error": "count_type must be crossed, not_crossed, or both."}
    return {
        "sensor": result["sensor"],"operator": result["operator"],"threshold": result["threshold"],"mode": result["mode"],
        "view": result["view"],"crossed_count": result.get("condition_checked_count", 0),"condition_checked_count": result.get("condition_checked_count", 0),
        "matching_readings_count": result.get("matching_readings_count", 0),"not_crossed_count": result.get("not_crossed_count", 0),
        "count_type": count_type, }
def _notification_lines(events: List[Dict[str, Any]]) -> str:
    """Build the common alert details for one or more events."""
    lines: List[str] = []
    for index, event in enumerate(events, 1):
        lines.extend([
            f"{index}. Sensor: {event.get('sensor')}",
            f"   Threshold: {event.get('operator')} {event.get('threshold')}",
            f"   Value: {event.get('value')}",
            f"   Timestamp: {event.get('timestamp')}",
            f"   Mode: {event.get('mode')}",
            "",
        ])
    return "\n".join(lines).rstrip()
def send_email_notification( event: Dict[str, Any], recipient_override: Optional[str] = None, body_override: Optional[str] = None, subject_override: Optional[str] = None,) -> Dict[str, Any]:
    recipients = [x.strip() for x in (recipient_override or ALERT_EMAIL_TO).split(",") if x.strip()]
    print(f"[EMAIL] Attempting send | host={SMTP_HOST or '<empty>'} | port={SMTP_PORT} | from={ALERT_EMAIL_FROM or '<empty>'} | to={recipients}")
    if not recipients:
        result = {"sent": False, "channel": "email", "reason": "No recipient email address was provided."}
        print("[EMAIL] FAILED:", result["reason"])
        return result
    if not (SMTP_HOST and SMTP_USERNAME and SMTP_PASSWORD and ALERT_EMAIL_FROM):
        result = {
            "sent": False,
            "channel": "email",
            "reason": "SMTP configuration is incomplete. Required: SMTP_HOST, SMTP_USERNAME, SMTP_PASSWORD, ALERT_EMAIL_FROM.",
        }
        print("[EMAIL] FAILED:", result["reason"])
        return result
    message = EmailMessage()
    message["Subject"] = subject_override or f"Sensor threshold alert: {event['sensor']}"
    message["From"] = ALERT_EMAIL_FROM
    message["To"] = ", ".join(recipients)
    message.set_content(body_override or (
        "THRESHOLD ALERT\n\n"
        f"Sensor: {event['sensor']}\n"
        f"Threshold: {event['operator']} {event['threshold']}\n"
        f"Crossed Value: {event['value']}\n"
        f"Timestamp: {event.get('timestamp')}\n"
        f"Mode: {event.get('mode')}\n"))
    try:
        context = ssl.create_default_context()
        if SMTP_PORT == 465:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=NOTIFICATION_TIMEOUT, context=context) as smtp:
                smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
                smtp.send_message(message)
        else:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=NOTIFICATION_TIMEOUT) as smtp:
                smtp.ehlo()
                smtp.starttls(context=context)
                smtp.ehlo()
                smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
                smtp.send_message(message)
    except Exception as exc:
        print(f"[EMAIL] FAILED: {type(exc).__name__}: {exc}")
        return {"sent": False, "channel": "email", "recipients": recipients, "error": f"{type(exc).__name__}: {exc}"}
    print(f"[EMAIL] SUCCESS | sent to {recipients}")
    return {"sent": True, "channel": "email", "recipients": recipients}
async def dispatch_alert_event_via_mcp(event: Dict[str, Any]) -> Dict[str, Any]:
    """Send one threshold-crossing event to the Alert Agent through MCP.
    The JSON/threshold logic stays in this FastAPI process, while MCP is the
    explicit communication layer used to hand the event to the Alert Agent.
    """
    try:
        async with Client(MCP_SERVER_URL) as client:
            result = await asyncio.wait_for(
                client.call_tool(
                    "create_alert",
                    {
                        "sensor": str(event.get("sensor", "")),
                        "value": float(event.get("value", 0.0)),
                        "operator": str(event.get("operator", ">")),
                        "threshold": float(event.get("threshold", 0.0)),
                        "timestamp": str(event.get("timestamp")),
                        "notify_email": bool(event.get("notify_email", True)),
                        "email_to": event.get("email_to") or ALERT_EMAIL_TO,
                    },
                ),
                timeout=MCP_TIMEOUT,)
        if getattr(result, "is_error", False):
            raise RuntimeError(f"MCP create_alert returned an error: {result}")
        # MCP transport succeeded. Keep the complete result so the caller can
        # inspect Alert Agent / SMTP delivery status.
        return {"success": True, "result": result}
    except Exception as exc:
        print(f"[MCP] Alert dispatch failed: {type(exc).__name__}: {exc}")
        return {"success": False, "error": f"{type(exc).__name__}: {exc}"}
async def dispatch_alert_events_via_mcp(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Dispatch all generated crossing events through MCP."""
    results: List[Dict[str, Any]] = []
    for event in events:
        results.append(await dispatch_alert_event_via_mcp(event))
    return results
def dispatch_alert_events_via_mcp_sync(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Synchronous wrapper for FastAPI sync endpoints."""
    if not events:
        return []
    return asyncio.run(dispatch_alert_events_via_mcp(events))
def notify_crossing_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Send one notification for one FALSE -> TRUE crossing event."""
    config = get_threshold(event.get("sensor", ""))
    if not config:
        return {"notifications": []}
    results: List[Dict[str, Any]] = []
    # The requirement is one email for the actual FALSE -> TRUE crossing only.
    # The MCP event carries the notification settings so the Alert Agent does
    # not lose them while the event travels JSON Agent -> MCP -> Alert Agent.
    notify_email = bool(event.get("notify_email", config.get("notify_email", True)))
    recipient = event.get("email_to") or config.get("email_to") or ALERT_EMAIL_TO
    if notify_email:
        try:
            results.append(
                send_email_notification( event, recipient,
                    body_override=(
                        "SENSOR THRESHOLD CROSSED\n\n"
                        f"Sensor: {event.get('sensor')}\n"
                        f"Threshold: {event.get('operator')} {event.get('threshold')}\n"
                        f"Crossed Value: {event.get('value')}\n"
                        f"Crossed Timestamp: {event.get('timestamp')}\n"
                        f"Mode: {event.get('mode')}\n"
                    ),
                    subject_override=f"Sensor threshold crossed: {event.get('sensor')}",
                )
            )
        except Exception as exc:
            results.append({"sent": False, "channel": "email", "error": str(exc)})
    return {"notifications": results}
class AlertAgent:
    """Deterministic agent that notifies only on FALSE -> TRUE crossings."""
    def __init__(self) -> None:
        self.enabled = True
        self.processed_events = 0
        self.notification_jobs = 0
        self.last_event: Optional[Dict[str, Any]] = None
        self.last_notification_results: List[Dict[str, Any]] = []
    def status(self) -> Dict[str, Any]:
        return {
            "agent": "alert_agent",
            "enabled": self.enabled,
            "thresholds": len(thresholds),
            "alert_history_count": len(alert_history),
            "processed_events": self.processed_events,
            "notification_jobs": self.notification_jobs,
            "email_enabled": ALERT_EMAIL_ENABLED,
            "last_event": self.last_event,
            "last_notification_results": self.last_notification_results,
            "alerts": alert_history[-100:],
        }
    def handle_events(self, events: List[Dict[str, Any]]) -> None:
        if not self.enabled:
            return
        for event in events:
            self.processed_events += 1
            self.last_event = event
alert_agent = AlertAgent()
def schedule_event_notifications(events: List[Dict[str, Any]]) -> None:
    """Notify only for actual FALSE -> TRUE crossing events."""
    if not events or not alert_agent.enabled:
        return
    alert_agent.handle_events(events)
    for event in events:
        config = get_threshold(event.get("sensor", ""))
        if not config or not config.get("notify_email"):
            continue
        alert_agent.notification_jobs += 1
        async def deliver( current_event: Dict[str, Any], current_config: Dict[str, Any],) -> None:
            try:
                result = await asyncio.to_thread(notify_crossing_event, current_event)
                alert_agent.last_notification_results = result.get("notifications", [])
                await manager.broadcast({
                    "type": "alert_notification_result",
                    "data": {
                        "event": current_event,
                        "notifications": alert_agent.last_notification_results,
                    },
                })
                print(
                    "[THRESHOLD CROSSING NOTIFICATION]",
                    json.dumps(alert_agent.last_notification_results, default=str),
                )
            except Exception as exc:
                alert_agent.last_notification_results = [
                    {"sent": False, "error": str(exc)}
                ]
                print("[THRESHOLD NOTIFICATION ERROR]", str(exc))
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(deliver(event, config))
        except RuntimeError:
            try:
                result = notify_crossing_event(event)
                alert_agent.last_notification_results = result.get("notifications", [])
            except Exception as exc:
                alert_agent.last_notification_results = [
                    {"sent": False, "error": str(exc)}
                ]
def extract_number(question: str) -> Optional[float]:
    """Extract standalone numeric values; do not extract digits in Temp3/Sensor12."""
    matches = re.findall(r"(?<![A-Za-z0-9_])[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?![A-Za-z0-9_])",question,)
    return float(matches[-1]) if matches else None
def extract_operator(question: str) -> Optional[str]:
    q = question.lower()
    symbolic = re.search(r"(>=|<=|!=|==|>|<|=)", q)
    if symbolic:
        return normalize_operator(symbolic.group(1))
    phrases = [
        (r"greater than or equal to|at least", ">="),
        (r"less than or equal to|at most", "<="),
        (r"greater than|above|over|exceed(?:s|ed)?", ">"),
        (r"less than|below|under", "<"),
        (r"not equal", "!="),
        (r"equal to|equals", "="),]
    for pattern, op in phrases:
        if re.search(pattern, q):
            return op
    if any(x in q for x in ["cross", "crossed", "breach", "breached", "exceed", "exceeded"]):
        return ">"
    return None
def choose_metric(question: str) -> Optional[str]:
    q = question.lower()
    aliases = [
        ("standard deviation", "stddev"),("std dev", "stddev"),("stddev", "stddev"),("variance", "variance"),("maximum", "max"),
        ("highest", "max"),("max", "max"),("minimum", "min"),("lowest", "min"),("min", "min"),("average", "average"),("mean", "average"),
        ("avg", "average"),("median", "median"),("range", "range"),("sum", "sum"),("count", "count"),]
    for phrase, metric in aliases:
        if phrase in q:
            return metric
    return None
def is_threshold_sensor_query(q: str) -> bool:
    """True when the user is asking which sensor has the active threshold."""
    patterns = [
        r"\bwhich\s+sensor(?:\s+label)?\s+(?:has|is|has the|is the)?\s*threshold\b",
        r"\bon\s+which\s+sensor(?:\s+label)?\s+(?:is|has)\s+(?:the\s+)?threshold\b",
        r"\bon\s+which\s+sensor(?:\s+label)?\s+(?:is\s+)?threshold\s+applied\b",
        r"\bwhich\s+sensor(?:\s+label)?\s+(?:has|is)\s+(?:the\s+)?threshold\s+applied\b",
        r"\bwhere\s+is\s+(?:the\s+)?threshold\s+applied\b",
        r"\bthreshold\s+(?:is\s+)?applied\s+(?:to|on)\s+which\s+sensor\b",
        r"\bon\s+which\s+sensor(?:\s+label)?\s+is\s+the\s+threshold\s+configured\b",
        r"\bwhat\s+is\s+the\s+sensor\s+(?:where|on\s+which)\s+(?:the\s+)?threshold\s+is\s+applied\b",
        r"\bwhat\s+sensor\s+(?:has|is)\s+(?:the\s+)?threshold\s+applied\b",
    ]
    return any(re.search(pattern, q) for pattern in patterns)
def is_configuration_query(q: str) -> bool:
    return is_threshold_sensor_query(q) or any(phrase in q for phrase in [
        "where is the threshold","where threshold is applied","threshold applied","threshold is applied","threshold configured",
        "threshold is configured","threshold configuration","what threshold is set","what threshold is configured","what is the threshold value",
        "what's the threshold value","what is the threshold","what's the threshold","threshold value for","threshold set on",
    ])
def is_crossing_query(q: str) -> bool:
    return any(word in q for word in ["cross", "crossed", "crossing", "breach", "breached", "exceed", "exceeded", "timestamp", "timestamps",])
def extract_explicit_sensor(question: str) -> Optional[str]:
    """Find a sensor explicitly named in the question, including unknown labels."""
    q = str(question or "")
    registry = get_sensor_registry()
    for entry in registry:
        for alias in entry.get("aliases", []):
            if re.search(r"(?<![A-Za-z0-9_])" + re.escape(str(alias)) + r"(?![A-Za-z0-9_])", q, re.I):
                return entry["label"]
    m = re.search(r"\b(?:temp|temperature|pressure|sensor)[ _-]*\d+\b", q, re.I)
    return m.group(0).strip() if m else None
def requested_view(question: str, supplied_view: Optional[str]) -> Optional[str]:
    q = str(question or "").lower()
    if re.search(r"\b(graph|chart|plot|graph view)\b", q): return "graph"
    if re.search(r"\b(list|table|list view)\b", q): return "list"
    return supplied_view if supplied_view in {"list", "graph"} else None
def requested_scope(question: str) -> str:
    q = str(question or "").lower()
    if "before threshold crossing" in q or "before crossing" in q: return "before"
    if "at threshold crossing" in q or "at the crossing" in q or "at crossing" in q: return "at"
    if "after threshold crossing" in q or "after crossing" in q: return "after"
    return "view"
def extract_requested_operator(question: str, configured: Optional[Dict[str, Any]] = None) -> Optional[str]:
    q = str(question or "").lower()
    if "greater than or equal" in q or "at least" in q or ">=" in q: return ">="
    if "less than or equal" in q or "at most" in q or "<=" in q: return "<="
    if "not equal" in q or "not equals" in q or "!=" in q: return "!="
    if "equal to" in q or "equals" in q: return "="
    if "greater than" in q or "above" in q or "over" in q or ">" in q: return ">"
    if "less than" in q or "below" in q or "under" in q or "<" in q: return "<"
    return normalize_operator(configured["operator"]) if configured else None
def tool_selected_sensor(sensor: Optional[str] = None) -> Dict[str, Any]:
    if not sensor: return {"error": "No sensor is currently selected."}
    resolved = resolve_sensor_name(sensor)
    if not resolved: return {"error": f"Sensor '{sensor}' was not found."}
    return {"sensor": resolved}
def _configured_chat_sensor(explicit_sensor: Optional[str], selected_sensor: Optional[str]) -> Optional[str]:
    """Resolve a chatbot sensor without guessing from arbitrary row fields."""
    if explicit_sensor:
        resolved = resolve_sensor_name(explicit_sensor)
        if resolved:
            return resolved
        return explicit_sensor
    if selected_sensor:
        resolved = resolve_sensor_name(selected_sensor)
        if resolved:
            return resolved
    # If exactly one threshold is configured, it is an unambiguous fallback.
    configured = [name for name, cfg in thresholds.items() if cfg.get("enabled", True)]
    if len(configured) == 1:
        return configured[0]
    return None
def _first_threshold_crossing(sensor: str, config: Dict[str, Any], view: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Return the first FALSE -> TRUE threshold transition."""
    resolved = resolve_sensor_name(sensor) or sensor
    rows = get_active_data_rows()
    previous_match = False
    for row in rows:
        field = resolve_data_field(resolved, row)
        if not field or not is_numeric(row.get(field)):
            continue
        value = float(row[field])
        current_match = threshold_matches(value, config)
        if current_match and not previous_match:
            return {
                "sensor": resolved,
                "value": value,
                "timestamp": get_timestamp_from_row(row),
                "threshold": float(config["threshold"]),
                "operator": normalize_operator(config["operator"]),
            }
        previous_match = current_match
    return None
def tool_crossing_detail(sensor: str, detail: str, view: Optional[str] = None) -> Dict[str, Any]:
    resolved = resolve_sensor_name(sensor)
    if not resolved:
        return {"error": f"Sensor '{sensor}' was not found."}
    config = get_threshold(resolved)
    if not config or not config.get("enabled", True):
        return {"error": f"No threshold is configured for sensor '{resolved}'."}
    crossing = _first_threshold_crossing(resolved, config, view)
    if not crossing:
        return {"error": f"No threshold crossing was found for sensor '{resolved}'."}
    crossing["detail"] = detail
    return crossing
TOOLS = {
    "list_sensors": tool_list_sensors,
    "threshold_sensor": tool_threshold_sensor,
    "selected_sensor": tool_selected_sensor,
    "sensor_metadata": tool_sensor_metadata,
    "latest_value": tool_latest_value,
    "statistics": tool_statistics,
    "values": tool_values,
    "reading_count": tool_reading_count,
    "threshold_configuration": tool_threshold_configuration,
    "set_threshold": tool_set_threshold,
    "threshold_analysis": tool_threshold_analysis,
    "threshold_timestamps": tool_threshold_timestamps,
    "threshold_counts": tool_threshold_counts,
    "crossing_detail": tool_crossing_detail,}
def build_agent_tool_plan(question: str, selected_sensor: Optional[str] = None, view: Optional[str] = None) -> Dict[str, Any]:
    """Build a deterministic chatbot plan for both static and live sensor data.
    Only the chatbot routing is handled here. Existing threshold, alert,
    telemetry, and live-buffer logic is intentionally left unchanged.
    """
    q = question.strip().lower()
    explicit = extract_explicit_sensor(question)
    # Explicit sensor > UI-selected sensor > the only configured threshold sensor.
    sensor = _configured_chat_sensor(explicit, selected_sensor)
    active_view = requested_view(question, view)
    scope = requested_scope(question)
    # --------------------------------------------------------
    # SELECTED SENSOR / SENSOR LIST
    # --------------------------------------------------------
    if re.search(r"\b(which sensor is selected|what sensor is selected|selected sensor)\b", q):
        return {"tool": "selected_sensor", "args": {"sensor": selected_sensor}}
    if (
        re.search(r"\b(list|available)\b.*\bsensors?\b", q)
        or re.search(r"\bwhat\s+(?:are|is)\s+(?:the\s+)?(?:available\s+)?sensors?\s*[?!.]*$", q)
        or re.search(r"\bsensors?\s*(are|available|list)", q)
        or re.search(r"\bwhat\s+(?:are|is)\s+(?:the\s+)?sensor\s+labels?(?:\s+available)?\s*[?!.]*$", q)
        or re.search(r"\bwhat\s+sensor\s+labels?\s+(?:are|is)\s+available\b", q)
        or re.search(r"\b(?:list|show)\s+(?:the\s+)?sensor\s+labels?\b", q)
    ):
        return {"tool": "list_sensors", "args": {}}
    # --------------------------------------------------------
    # SET THRESHOLD
    # --------------------------------------------------------
    if any(phrase in q for phrase in ["set threshold", "configure threshold", "apply threshold", "add threshold"]):
        if not sensor:
            return {"error": "Please specify a sensor for the threshold."}
        if not resolve_sensor_name(sensor):
            return {"error": f"Sensor '{sensor}' was not found."}
        value = extract_number(question)
        op = extract_operator(question) or ">"
        if value is None:
            return {"error": "Please provide the threshold value."}
        return {
            "tool": "set_threshold",
            "args": {"sensor": sensor, "operator_str": op, "threshold": value},
        }
    # --------------------------------------------------------
    # EXACT CROSSING-EVENT QUESTIONS
    # --------------------------------------------------------
    timestamp_phrases = [
        "timestamp at threshold crossing",
        "timestamp when the threshold was crossed",
        "when did the sensor cross",
        "when was the threshold crossed",
        "at what time was the threshold crossed",
        "what was the timestamp at threshold crossing",]
    value_phrases = [
        "value at threshold crossing",
        "sensor value at threshold crossing",
        "what was the sensor value at threshold crossing",
        "value when the threshold was crossed",
        "reading at the crossing point",
        "what value crossed the threshold",]
    if any(p in q for p in timestamp_phrases):
        if not sensor:
            return {"error": "Please specify which sensor the threshold query applies to."}
        return {"tool": "crossing_detail", "args": {"sensor": sensor, "detail": "timestamp", "view": active_view}}
    if any(p in q for p in value_phrases):
        if not sensor:
            return {"error": "Please specify which sensor the threshold query applies to."}
        return {"tool": "crossing_detail", "args": {"sensor": sensor, "detail": "value", "view": active_view}}
    if "which sensor crossed" in q:
        if not sensor and selected_sensor:
            sensor = resolve_sensor_name(selected_sensor)
        if sensor:
            return {"tool": "crossing_detail", "args": {"sensor": sensor, "detail": "sensor", "view": active_view}}
        return {"error": "Please specify which sensor the threshold query applies to."}
    # --------------------------------------------------------
    # SENSOR WITH ACTIVE THRESHOLD
    # These questions ask for the sensor label, not the operator/value.
    # --------------------------------------------------------
    if is_threshold_sensor_query(q) and not is_crossing_query(q):
        return {"tool": "threshold_sensor", "args": {}}
    # --------------------------------------------------------
    # THRESHOLD CONFIGURATION
    # --------------------------------------------------------
    if is_configuration_query(q) and not is_crossing_query(q):
        return {"tool": "threshold_configuration", "args": {"sensor": sensor} if sensor else {}}
    # --------------------------------------------------------
    # LATEST TIMESTAMP / LATEST VALUE
    # --------------------------------------------------------
    if any(p in q for p in ["latest timestamp", "most recent timestamp", "current timestamp"]):
        if not sensor:
            return {"error": "Please specify which sensor you want the timestamp for."}
        return {"tool": "latest_value", "args": {"sensor": sensor, "timestamp_only": True}}
    if scope == "view" and any(p in q for p in [
        "latest value",
        "current value",
        "last value",
        "most recent value",
        "sensor value",
        "current sensor value",
        "latest sensor value",]):
        if not sensor:
            return {"error": "Please specify which sensor/field you want the value for."}
        return {"tool": "latest_value", "args": {"sensor": sensor}}
    # --------------------------------------------------------
    # BEFORE / AT / AFTER THRESHOLD CROSSING
    # Handle these before generic threshold words so that questions
    # such as "maximum before threshold crossing" reach statistics.
    # --------------------------------------------------------
    if scope in {"before", "at", "after"}:
        if not sensor:
            return {"error": "Please specify which sensor the question applies to."}
        if any(phrase in q for phrase in ["how many readings","how many values","number of readings","number of values","count of readings","count of values",]):
            return {
                "tool": "reading_count",
                "args": {
                    "sensor": sensor,
                    "view": active_view,
                    "scope": scope,
                },
            }
        metric = choose_metric(question)
        if metric:
            return {
                "tool": "statistics",
                "args": {
                    "sensor": sensor,
                    "metric": metric,
                    "view": active_view or "graph",
                    "scope": scope,
                }, }
        if any(phrase in q for phrase in ["what values", "which values", "values", "readings", "data"]):
            return {
                "tool": "values",
                "args": {
                    "sensor": sensor,
                    "view": active_view,
                    "scope": scope,
                }, }
    # --------------------------------------------------------
    # STATISTICS
    # Handle before generic threshold detection so phrases such as
    # "maximum value at threshold" do not get routed incorrectly.
    # --------------------------------------------------------
    metric = choose_metric(question)
    if metric:
        if not sensor:
            return {"error": "Please specify which sensor/field the calculation should use."}
        return {
            "tool": "statistics",
            "args": {
                "sensor": sensor,
                "metric": metric,
                "view": active_view or "graph",
                "scope": scope,
            }, }
    # --------------------------------------------------------
    # GENERIC THRESHOLD QUESTIONS
    # Existing threshold analysis/count/timestamp tools are preserved.
    # --------------------------------------------------------
    threshold_words = [
        "threshold", "cross", "crossed", "crossing", "breach",
        "breached", "exceed", "exceeded", "below", "above",]
    if any(word in q for word in threshold_words):
        if not sensor:
            if is_configuration_query(q):
                return {"tool": "threshold_configuration", "args": {}}
            return {"error": "Please specify which sensor the threshold query applies to."}
        configured = get_threshold(sensor)
        value = extract_number(question)
        if configured and value is None:
            value = float(configured["threshold"])
        op = extract_requested_operator(question, configured)
        if value is None:
            return {"error": f"No threshold value was provided and no configured threshold exists for '{sensor}'."}
        if op is None:
            op = ">"
        if any(x in q for x in ["how many", "count", "number of"]):
            not_crossed = any(x in q for x in ["not", "did not", "didn't"])
            count_type = "not_crossed" if not_crossed else "crossed"
            return {
                "tool": "threshold_counts",
                "args": {
                    "sensor": sensor,
                    "operator_str": op,
                    "threshold": value,
                    "count_type": count_type,
                    "view": active_view,
                },}
        if any(x in q for x in ["timestamp", "timestamps", "when"]):
            first_only = True
            last_only = "last" in q
            return {
                "tool": "threshold_timestamps",
                "args": {
                    "sensor": sensor,
                    "operator_str": op,
                    "threshold": value,
                    "first_only": first_only and not last_only,
                    "last_only": last_only,
                    "view": active_view,
                },}
    # --------------------------------------------------------
    # METADATA
    # --------------------------------------------------------
    if any(phrase in q for phrase in ["metadata", "unit", "type of sensor", "information about sensor"]):
        if not sensor:
            return {"error": "Please specify a sensor."}
        return {"tool": "sensor_metadata", "args": {"sensor": sensor}}
    # --------------------------------------------------------
    # VALUES / READINGS
    # --------------------------------------------------------
    if any(p in q for p in ["readings", "values", "data"]):
        if not sensor:
            return {"error": "Please specify a sensor."}
        return {
            "tool": "values",
            "args": {
                "sensor": sensor,
                "view": active_view or "graph",
                "scope": scope,
            }, }
    return {
        "error": (
            "I can answer sensor-data questions using the available tools. "
            "Specify a sensor and the operation, such as max, min, average, "
            "counts, timestamps, threshold crossings, or latest value."
        ) }
def _extract_mcp_tool_result(result: Any) -> Any:
    """Convert an MCP CallToolResult into the JSON result returned by our tool."""
    structured = getattr(result, "structuredContent", None)
    if structured is None:
        structured = getattr(result, "structured_content", None)
    if structured is not None:
        if isinstance(structured, dict) and "result" in structured and len(structured) == 1:
            return structured["result"]
        return structured
    content = getattr(result, "content", None) or []
    for item in content:
        text_value = getattr(item, "text", None)
        if text_value:
            try:
                parsed = json.loads(text_value)
                if isinstance(parsed, dict) and "result" in parsed and len(parsed) == 1:
                    return parsed["result"]
                return parsed
            except json.JSONDecodeError:
                return text_value
    return {}
async def _execute_tool_plan_via_mcp(plan: Dict[str, Any]) -> Dict[str, Any]:
    tool_name = plan["tool"]
    try:
        async with Client(MCP_SERVER_URL) as client:
            result = await asyncio.wait_for(
                client.call_tool(tool_name, plan.get("args", {})),
                timeout=MCP_TIMEOUT,)
        if getattr(result, "is_error", False):
            return {"tool": tool_name, "error": f"MCP tool '{tool_name}' returned an error."}
        return {"tool": tool_name, "result": _extract_mcp_tool_result(result)}
    except Exception as exc:
        return {
            "tool": tool_name,
            "error": f"MCP tool '{tool_name}' failed: {type(exc).__name__}: {exc}", }
def execute_tool_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
    """Execute chatbot tools through the MCP server, not by direct local calls."""
    if "error" in plan:
        return plan
    return asyncio.run(_execute_tool_plan_via_mcp(plan))
def format_number(value: Any) -> str:
    if isinstance(value, (int, float)):
        return f"{value:.6g}"
    return str(value)
def format_tool_result(executed: Dict[str, Any]) -> str:
    if "error" in executed: return executed["error"]
    result = executed.get("result", {})
    tool = executed.get("tool")
    if "error" in result: return result["error"]
    if tool == "list_sensors": return ", ".join(result["sensors"])
    if tool == "threshold_sensor":
        sensors = result.get("sensors", [])
        if not sensors:
            return "No threshold is currently applied to any sensor."
        return ", ".join(sensors)
    if tool == "selected_sensor": return result["sensor"]
    if tool == "statistics": return format_number(result["result"])
    if tool == "latest_value": return str(result.get("timestamp")) if result.get("timestamp_only") else format_number(result.get("value"))
    if tool == "threshold_configuration":
        if "sensor" in result:
            config = result.get("threshold")
            return f"{config['operator']} {config['threshold']}" if config else f"No threshold is configured for sensor '{result['sensor']}'."
        configs = result.get("thresholds", {})
        return "; ".join(f"{s} {c['operator']} {c['threshold']}" for s,c in configs.items()) if configs else "No thresholds are currently configured."
    if tool == "set_threshold":
        config = result.get("threshold", {})
        return f"{config.get('operator')} {config.get('threshold')}"
    if tool == "threshold_analysis": return format_number(result["crossed_count"])
    if tool == "threshold_counts": return str(result.get("condition_checked_count", result["crossed_count"]) if result.get("count_type") == "crossed" else result["not_crossed_count"] if result.get("count_type") == "not_crossed" else result.get("condition_checked_count", result["crossed_count"]))
    if tool == "threshold_timestamps": return ", ".join(map(str, result.get("timestamps", []))) if result.get("timestamps") else "No matching timestamp found."
    if tool == "reading_count": return str(result.get("count", 0))
    if tool == "crossing_detail":
        return str(result.get("timestamp")) if result.get("detail") == "timestamp" else format_number(result.get("value")) if result.get("detail") == "value" else result.get("sensor", "")
    if tool == "sensor_metadata": return json.dumps(result["metadata"], ensure_ascii=False)
    if tool == "values":
        values = result.get("values", [])
        if not values:
            return "No readings found."
        return ", ".join(
            f"{item.get('timestamp')}: {format_number(item.get('value'))}"
            for item in values
        )
    return json.dumps(result, ensure_ascii=False)
def answer_chat(question: str, selected_sensor: Optional[str] = None, view: Optional[str] = None) -> Dict[str, Any]:
    plan = build_agent_tool_plan(question, selected_sensor, view)
    executed = execute_tool_plan(plan)
    return {"answer": format_tool_result(executed),"tool": executed.get("tool"),
        "result": executed.get("result"),"mode": execution_mode,"view": view,"llm": False,}
# ============================================================
# API ENDPOINTS
# ============================================================
@app.get("/")
def root():
    return {"name": "Dynamic Sensor Tools Agent", "llm": False, "tools": list(TOOLS.keys()), "modes": ["static", "live"]}
@app.get("/api/health")
def health():
    return {
        "status": "healthy",
        "execution_mode": execution_mode,
        "static_loaded": current_json_data is not None,
        "static_rows": len(current_json_data.get("data", [])) if current_json_data and isinstance(current_json_data.get("data", []), list) else 0,
        "live_buffer_size": len(live_telemetry_buffer),
        "sensor_count": len(get_sensor_names()),
        "threshold_count": len(thresholds),
        "alert_count": len(alert_history),
        "llm": False,
    }
@app.post("/api/mode")
def set_execution_mode(req: ModeSwitchRequest):
    global execution_mode
    mode = req.mode.lower().strip()
    if mode not in {"static", "live"}:
        raise HTTPException(status_code=400, detail="Mode must be 'static' or 'live'")
    execution_mode = mode
    if mode == "live":
        live_telemetry_buffer.clear()
    clear_threshold_state()
    invalidate_sensor_registry_cache()
    return {"message": f"Execution mode updated to '{execution_mode}'", "sensors": get_sensor_names()}
# ---------------- LIVE DATA ----------------
def switch_to_live_automatically() -> None:
    """Enter live mode when telemetry arrives, without a frontend mode selector."""
    global execution_mode
    if execution_mode != "live":
        execution_mode = "live"
        live_telemetry_buffer.clear()
        clear_threshold_state()
async def process_live_row(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    switch_to_live_automatically()
    clean_row = dict(row)
    # If the active threshold has already crossed, live plotting/monitoring is
    # intentionally stopped until the threshold is configured/reset again.
    active_stopped = any(
        is_live_monitoring_stopped(sensor, config)
        for sensor, config in list(thresholds.items())
        if config.get("enabled", True))
    if active_stopped:
        await manager.broadcast({
            "type": "live_monitoring_stopped",
            "data": {"reason": "threshold_crossed"},})
        return []
    # Append before evaluating so the crossing sample itself appears as the
    # final point on the live graph.
    live_telemetry_buffer.append(clean_row)
    # Do NOT discover sensors from arbitrary live-row keys. The sensor
    # dropdown is driven only by the declared sensor labels in the loaded
    # JSON sensor definitions. Telemetry fields are used only to resolve the
    # already-declared sensor labels.
    events = evaluate_row_for_alerts(clean_row)
    await manager.broadcast({"type": "telemetry", "data": clean_row})
    for event in events:
        await manager.broadcast({"type": "threshold_alert", "data": event})
    if events:
        mcp_results = await dispatch_alert_events_via_mcp(events)
        for event, mcp_result in zip(events, mcp_results):
            if not mcp_result.get("success"):
                alert_history.append(event)
                alert_keys_seen.add(make_alert_key(event["sensor"], thresholds[event["sensor"]], {
                    event.get("field") or event["sensor"]: event.get("value"),
                    "timestamp": event.get("timestamp"),
                }, event.get("field")))
                schedule_event_notifications([event])
        await manager.broadcast({
            "type": "live_monitoring_stopped",
            "data": {
                "reason": "threshold_crossed",
                "event": events[0],
            },
        })
    return events
@app.post("/api/telemetry/stream")
async def ingest_live_telemetry(packet: TelemetryPacket):
    row = dict(packet.readings)
    if packet.timestamp is not None:
        row["timestamp"] = packet.timestamp
    events = await process_live_row(row)
    return {"status": "ingested", "buffer_size": len(live_telemetry_buffer), "alerts": events}
@app.websocket("/ws/telemetry")
async def websocket_telemetry_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                payload = json.loads(data)
                if isinstance(payload, dict):
                    if isinstance(payload.get("readings"), dict):
                        row = dict(payload["readings"])
                        if payload.get("timestamp") is not None:
                            row["timestamp"] = payload["timestamp"]
                    else:
                        row = payload
                    await process_live_row(row)
            except json.JSONDecodeError:
                await websocket.send_json({"type": "error", "message": "Invalid JSON telemetry packet."})
    except WebSocketDisconnect:
        manager.disconnect(websocket)
# ---------------- JSON NORMALIZATION ----------------
def normalize_sensor_document(parsed: Any) -> Dict[str, Any]:
    """Accept root arrays and common object wrappers while preserving sensor metadata."""
    if isinstance(parsed, list):
        rows = [dict(row) for row in parsed if isinstance(row, dict)]
        return {"data": rows}
    if not isinstance(parsed, dict):
        raise ValueError("JSON must be an object or an array of reading objects.")
    result = dict(parsed)
    rows = None
    for key in ("data", "readings", "records", "rows", "items"):
        value = result.get(key)
        if isinstance(value, list):
            rows = [dict(row) for row in value if isinstance(row, dict)]
            break
    if rows is None:
        # A single reading object is also accepted.
        if any(not isinstance(v, (dict, list)) for v in result.values()):
            rows = [dict(result)]
        else:
            rows = []
    result["data"] = rows
    return result
# ---------------- MQTT LIVE INPUT ----------------
def _mqtt_payload_to_row(payload: bytes) -> Optional[Dict[str, Any]]:
    try:
        decoded = json.loads(payload.decode("utf-8"))
    except Exception as exc:
        print(f"[MQTT] Ignoring non-JSON payload: {exc}")
        return None
    if isinstance(decoded, list):
        # If a publisher sends a one-element batch, process the first object.
        decoded = decoded[0] if decoded and isinstance(decoded[0], dict) else None
    if not isinstance(decoded, dict):
        return None
    # Accept either {readings: {...}, timestamp: ...} or a flat sensor row.
    if isinstance(decoded.get("readings"), dict):
        row = dict(decoded["readings"])
        if decoded.get("timestamp") is not None:
            row["timestamp"] = decoded["timestamp"]
    elif isinstance(decoded.get("data"), dict):
        row = dict(decoded["data"])
        if decoded.get("timestamp") is not None:
            row["timestamp"] = decoded["timestamp"]
    else:
        row = dict(decoded)
    return row
def _mqtt_on_connect(client, userdata, flags, reason_code, properties=None):
    if getattr(reason_code, "is_failure", False):
        print(f"[MQTT] Connection failed: {reason_code}")
        return
    print(f"[MQTT] Connected to {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}; subscribing to {MQTT_TOPIC}")
    client.subscribe(MQTT_TOPIC)
def _mqtt_on_message(client, userdata, message):
    row = _mqtt_payload_to_row(message.payload)
    if row is None:
        return
    print("[MQTT] Received live data:", row)
    try:
        if _main_event_loop is not None and _main_event_loop.is_running():
            future = asyncio.run_coroutine_threadsafe(process_live_row(row), _main_event_loop)
            future.result(timeout=15)
        else:
            print("[MQTT] FastAPI event loop is not ready; telemetry was ignored.")
    except Exception as exc:
        # MQTT failures must never terminate FastAPI or the subscriber thread.
        print(f"[MQTT] Telemetry processing failed: {type(exc).__name__}: {exc}")
def start_mqtt_listener() -> None:
    global _mqtt_client, _mqtt_thread, _mqtt_started
    if _mqtt_started or not MQTT_ENABLED:
        return
    if mqtt is None:
        print("[MQTT] MQTT_ENABLED=true but paho-mqtt is not installed. Run: pip install \"paho-mqtt>=2.0,<3\"")
        return
    _mqtt_started = True
    def runner():
        global _mqtt_client
        try:
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
            if MQTT_USERNAME:
                client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
            client.on_connect = _mqtt_on_connect
            client.on_message = _mqtt_on_message
            _mqtt_client = client
            client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, MQTT_KEEPALIVE)
            client.loop_forever()
        except Exception as exc:
            print(f"[MQTT] Listener stopped: {type(exc).__name__}: {exc}")
            _mqtt_started = False

    _mqtt_thread = threading.Thread(target=runner, name="mqtt-subscriber", daemon=True)
    _mqtt_thread.start()
# ---------------- STATIC DATA ----------------
@app.post("/api/load-json")
def load_json_file():
    global current_json_data, execution_mode
    if not DEFAULT_JSON_PATH.exists():
        raise HTTPException(status_code=404, detail="Default data file not found")
    try:
        with DEFAULT_JSON_PATH.open("r", encoding="utf-8") as f:
            parsed = normalize_sensor_document(json.load(f))
        current_json_data = parsed
        execution_mode = "static"
        live_telemetry_buffer.clear()
        clear_threshold_state()
        invalidate_sensor_registry_cache()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}")
    return {"message": "Static JSON reloaded", "rows": len(current_json_data.get("data", [])), "sensors": get_sensor_names()}
@app.post("/api/upload-json")
async def upload_json_file(file: UploadFile = File(...)):
    global current_json_data, execution_mode
    content = await file.read()
    try:
        parsed = normalize_sensor_document(json.loads(content.decode("utf-8")))
        current_json_data = parsed
        execution_mode = "static"
        live_telemetry_buffer.clear()
        clear_threshold_state()
        invalidate_sensor_registry_cache()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}")
    rows = current_json_data.get("data", [])
    return {"message": "Custom JSON uploaded", "rows": len(rows) if isinstance(rows, list) else 0, "sensors": get_sensor_names()}
@app.get("/api/json-info")
def json_info():
    return {
        "mode": execution_mode,
        "rows": len(get_active_data_rows()),
        "sensors": get_sensor_names(),
        "static_loaded": current_json_data is not None,
        "live_buffer_size": len(live_telemetry_buffer),
    }
@app.get("/api/sensors")
def sensors():
    names = get_sensor_names()
    return {"mode": execution_mode, "sensors": names, "count": len(names)}
@app.get("/api/sensors/{sensor_name}/values")
def sensor_values(sensor_name: str):
    result = tool_values(sensor_name)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result
@app.get("/api/sensors/{sensor_name}/latest")
def sensor_latest(sensor_name: str):
    result = tool_latest_value(sensor_name)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result
def get_view_rows(sensor_name: str, view: str = "graph") -> List[Dict[str, Any]]:
    """Return rows according to the active List/Graph semantics."""
    view = (view or "graph").strip().lower()
    if view not in {"list", "graph"}:
        view = "graph"
    rows = get_active_data_rows()
    # Live data is always limited by the first crossing because monitoring
    # stops there. Static Graph View intentionally remains the full dataset.
    if execution_mode == "live":
        return rows
    if view == "graph":
        return rows
    config = get_threshold(sensor_name)
    if not config or not config.get("enabled", True):
        return rows
    resolved = resolve_sensor_name(sensor_name) or sensor_name
    limited: List[Dict[str, Any]] = []
    for row in rows:
        limited.append(dict(row))
        field = resolve_data_field(resolved, row)
        if field and is_numeric(row.get(field)) and threshold_matches(row.get(field), config):
            break
    return limited
def get_view_points(sensor_name: str, view: str = "graph") -> List[Dict[str, Any]]:
    resolved = resolve_sensor_name(sensor_name) or sensor_name
    points: List[Dict[str, Any]] = []
    for row in get_view_rows(resolved, view):
        field = resolve_data_field(resolved, row)
        if field and is_numeric(row.get(field)):
            points.append({
                "timestamp": get_timestamp_from_row(row),
                "value": float(row[field]),
            })
    return points
@app.get("/api/data")
def data_view(view: str = "graph", sensor: Optional[str] = None):
    """Frontend data contract for List and Graph views."""
    names = get_sensor_names()
    if not sensor:
        sensor = names[0] if names else None
    if not sensor:
        return {
            "data": [], "rows": [], "points": [], "threshold_points": [],
            "sensor": None, "view": view, "mode": execution_mode,
            "threshold": None, "crossing_event": None, "crossing_index": None,
            "live_monitoring_stopped": False, "count": 0,}
    resolved = resolve_sensor_name(sensor)
    if not resolved:
        raise HTTPException(status_code=404, detail=f"Sensor '{sensor}' was not found.")
    normalized_view = view if view in {"list", "graph"} else "graph"
    rows = get_view_rows(resolved, normalized_view)
    points = get_view_points(resolved, normalized_view)
    config = get_threshold(resolved)
    threshold_points: List[Dict[str, Any]] = []
    crossing_event: Optional[Dict[str, Any]] = None
    crossing_index: Optional[int] = None
    if execution_mode == "static" and config and config.get("enabled", True):
        for index, row in enumerate(get_active_data_rows()):
            field = resolve_data_field(resolved, row)
            if field and is_numeric(row.get(field)) and threshold_matches(row.get(field), config):
                crossing_index = index
                crossing_event = {
                    "sensor": resolved,
                    "field": field,
                    "operator": normalize_operator(config.get("operator", ">")),
                    "threshold": float(config["threshold"]),
                    "value": float(row[field]),
                    "timestamp": get_timestamp_from_row(row),
                    "mode": "static",
                    "crossing": True,}
                break
    if normalized_view == "graph" and config and config.get("enabled", True):
        threshold_value = float(config["threshold"])
        threshold_points = [
            {"timestamp": get_timestamp_from_row(row), "value": threshold_value}
            for row in rows]
    stopped = bool(config and is_live_monitoring_stopped(resolved, config))
    live_crossing_event = (
        live_monitoring_stopped.get(make_threshold_state_key(resolved, config))
        if config and stopped else None)
    return {
        "sensor": resolved,"view": normalized_view,"mode": execution_mode,"data": rows,
        "rows": rows,"points": points,"threshold_points": threshold_points,"threshold": config,
        "crossing_event": crossing_event if execution_mode == "static" else live_crossing_event,
        "crossing_index": crossing_index,"live_monitoring_stopped": stopped,"count": len(rows),
    }
@app.get("/api/view-data/{sensor_name}")
def view_data(sensor_name: str, view: str = "graph"):
    """Backward-compatible view endpoint using the same List/Graph snapshot."""
    return data_view(view=view, sensor=sensor_name)
# ---------------- THRESHOLDS & ALERTS ----------------
@app.post("/api/threshold")
def set_threshold(req: ThresholdRequest):
    result = tool_set_threshold(req.sensor, req.operator, req.threshold, req.enabled, req.notify_email, req.email_to)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    generated: List[Dict[str, Any]] = []
    rows_processed = 0
    stopped_at_crossing = False
    if execution_mode == "static" and req.enabled:
        clear_threshold_state(req.sensor)
        # Static mode reads sequentially and stops immediately at the first
        # FALSE -> TRUE crossing. Only that crossing timestamp is notified.
        for row in get_active_data_rows():
            rows_processed += 1
            events = evaluate_row_for_alerts(row)
            if events:
                generated.extend(events)
                stopped_at_crossing = True
                break
        if generated:
            mcp_results = dispatch_alert_events_via_mcp_sync(generated)
            for event, mcp_result in zip(generated, mcp_results):
                if not mcp_result.get("success"):
                    # MCP is the primary path. If MCP is temporarily unavailable,
                    # keep the Alert Agent functional through a local fallback.
                    alert_history.append(event)
                    alert_keys_seen.add(make_alert_key(event["sensor"], thresholds[event["sensor"]], {
                        event.get("field") or event["sensor"]: event.get("value"),
                        "timestamp": event.get("timestamp"),
                    }, event.get("field")))
                    schedule_event_notifications([event])
    result["alerts_generated"] = len(generated)
    result["alerts"] = generated
    result["rows_processed"] = rows_processed
    result["stopped_at_crossing"] = stopped_at_crossing
    result["crossed"] = bool(generated)
    result["crossing_event"] = generated[0] if generated else None
    result["message"] = (
        f"Threshold crossed at {generated[0].get('value')} for {generated[0].get('sensor')} "
        f"at {generated[0].get('timestamp')}."
        if generated
        else "Threshold not crossed in the available data."
    )
    return result
@app.get("/api/thresholds")
def list_thresholds():
    return {"count": len(thresholds), "thresholds": thresholds}
@app.delete("/api/threshold/{sensor_name}")
def delete_threshold(sensor_name: str):
    actual = next((s for s in thresholds if normalize_name(s) == normalize_name(resolve_sensor_name(sensor_name) or sensor_name)), None)
    if not actual:
        raise HTTPException(status_code=404, detail=f"No threshold configured for '{sensor_name}'.")
    del thresholds[actual]
    clear_threshold_state(actual)
    for key in list(alert_keys_seen):
        if key.startswith(normalize_name(actual) + "|"):
            alert_keys_seen.discard(key)
    return {"message": f"Threshold removed for '{actual}'."}
@app.get("/api/alerts")
async def get_alerts(limit: Optional[int] = None):
    """Return Alert Agent history.
    The complete history is returned by default so static datasets are not
    silently truncated at 100 alerts. A positive limit returns only the
    newest N events.
    """
    alerts = list(alert_history)
    if limit is not None:
        if limit <= 0:
            raise HTTPException(status_code=400, detail="limit must be greater than 0")
        alerts = alerts[-limit:]
    return {
        "mode": execution_mode,
        "count": len(alerts),
        "total_count": len(alert_history),
        "alerts": alerts,
    }
@app.post("/api/mcp/alert")
async def receive_mcp_alert(event: Dict[str, Any]):
    """Receive a crossing event from the MCP server and hand it to Alert Agent."""
    required = ("sensor", "value", "operator", "threshold", "timestamp")
    missing = [name for name in required if name not in event]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing alert fields: {', '.join(missing)}")
    sensor = resolve_sensor_name(str(event.get("sensor"))) or str(event.get("sensor"))
    event = dict(event)
    event["sensor"] = sensor
    event["value"] = float(event["value"])
    event["threshold"] = float(event["threshold"])
    event["operator"] = normalize_operator(str(event["operator"])) or str(event["operator"])
    event["crossing"] = True
    event.setdefault("mode", execution_mode)
    config = get_threshold(sensor)
    if config:
        key = make_alert_key(
            sensor,
            config,
            {
                event.get("field") or sensor: event.get("value"),
                "timestamp": event.get("timestamp"),
            },
            event.get("field"),
        )
        if key in alert_keys_seen:
            return {"status": "duplicate", "event": event}
        alert_keys_seen.add(key)
    alert_history.append(event)
    # The MCP callback is the Alert Agent entry point. Send the email here
    # and await the result so the MCP response proves whether SMTP delivery
    # succeeded. This prevents a background task from hiding an email failure.
    alert_agent.handle_events([event])
    notification_result = {"notifications": []}
    if event.get("crossing", False):
        notification_result = await asyncio.to_thread(notify_crossing_event, event)
        alert_agent.notification_jobs += 1 if notification_result.get("notifications") else 0
        alert_agent.last_notification_results = notification_result.get("notifications", [])
    await manager.broadcast({"type": "threshold_alert", "data": event})
    await manager.broadcast({
        "type": "alert_notification_result",
        "data": {
            "event": event,
            "notifications": alert_agent.last_notification_results,
        },})
    print("[ALERT AGENT] Notification result:", json.dumps(alert_agent.last_notification_results, default=str))
    return {
        "status": "accepted",
        "event": event,
        "notification": notification_result,}
@app.get("/api/alert-agent/status")
def alert_agent_status():
    return alert_agent.status()
@app.post("/api/alert-agent/process-static")
def alert_agent_process_static():
    if execution_mode != "static":
        raise HTTPException(status_code=400, detail="Switch to static mode before processing static data.")
    # Rebuild history from the currently configured sensor/threshold only.
    # This keeps manual static processing consistent with /api/threshold.
    alert_history.clear()
    alert_keys_seen.clear()
    clear_threshold_state()
    generated: List[Dict[str, Any]] = []
    rows_processed = 0
    for row in get_active_data_rows():
        rows_processed += 1
        events = evaluate_row_for_alerts(row)
        if events:
            generated.extend(events)
            break
    if generated:
        mcp_results = dispatch_alert_events_via_mcp_sync(generated)
        for event, mcp_result in zip(generated, mcp_results):
            if not mcp_result.get("success"):
                alert_history.append(event)
                alert_keys_seen.add(make_alert_key(event["sensor"], thresholds[event["sensor"]], {
                    event.get("field") or event["sensor"]: event.get("value"),
                    "timestamp": event.get("timestamp"),
                }, event.get("field")))
                schedule_event_notifications([event])
    return {
        "mode": execution_mode, "rows_checked": rows_processed, "new_alerts": len(generated), "alerts": generated, "stopped_at_crossing": bool(generated),}
@app.get("/api/email-config-status")
def email_config_status():
    """Show safe email configuration diagnostics without exposing the password."""
    return {
        "smtp_host_configured": bool(SMTP_HOST),
        "smtp_port": SMTP_PORT,
        "smtp_username_configured": bool(SMTP_USERNAME),
        "smtp_password_configured": bool(SMTP_PASSWORD),
        "email_from_configured": bool(ALERT_EMAIL_FROM),
        "email_recipient": ALERT_EMAIL_TO,
        "email_enabled": ALERT_EMAIL_ENABLED,
        "ready": bool(SMTP_HOST and SMTP_USERNAME and SMTP_PASSWORD and ALERT_EMAIL_FROM and ALERT_EMAIL_TO),
    }
@app.post("/api/test-email")
async def test_email():
    """Send a real diagnostic email using the configured SMTP settings."""
    event = {
        "sensor": "TEST_SENSOR","value": 999.0,"operator": ">","threshold": 100.0,"timestamp": "diagnostic-test",
        "mode": execution_mode,"crossing": True,}
    print("[EMAIL TEST] Starting diagnostic email test")
    result = await asyncio.to_thread(send_email_notification, event, ALERT_EMAIL_TO,
        body_override=( "SENSOR ALERT EMAIL TEST\n\n" "This is a diagnostic email from the Sensor Alert Agent.\n\n" "If you received this message, SMTP email delivery is working.\n"
        ),
        subject_override="Sensor Alert Agent - Email Test",
    )
    print("[EMAIL TEST] Result:", json.dumps(result, default=str))
    return result
@app.post("/api/alert-agent/test")
async def alert_agent_test(req: NotificationTestRequest):
    event = {"sensor": "TEST_SENSOR", "value": 999.0, "operator": ">", "threshold": 100.0, "timestamp": "test", "mode": execution_mode, "crossing": True}
    results = []
    if req.email:
        try: results.append(await asyncio.to_thread(send_email_notification, event, req.email_to))
        except Exception as exc: results.append({"sent": False, "channel": "email", "error": str(exc)})
    if not results:
        return {"message": "Select email and provide the destination address if needed."}
    return {"results": results}
# ---------------- TOOL / CHAT ----------------
@app.get("/api/tools")
def tools():
    return {
        "llm": False,
        "tools": [
            {"name": name, "description": getattr(fn, "__doc__", "") or "Deterministic sensor-data tool."}
            for name, fn in TOOLS.items()
        ],}
@app.post("/api/mcp/tool")
def mcp_tool(request: MCPToolRequest):
    """Internal JSON Agent tool endpoint used by the MCP Server wrappers."""
    tool = TOOLS.get(request.tool)
    if not tool:
        raise HTTPException(status_code=404, detail=f"Unknown tool '{request.tool}'.")
    try:
        result = tool(**request.args)
        return {"tool": request.tool, "result": result}
    except TypeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid arguments for tool '{request.tool}': {exc}")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Tool '{request.tool}' failed: {type(exc).__name__}: {exc}")
@app.post("/api/chat")
def chat(request: ChatRequest):
    return answer_chat(request.question, request.selected_sensor, request.view)
@app.post("/api/check-threshold")
def check_threshold(req: CheckThresholdRequest):
    result = tool_threshold_analysis(req.sensor, req.operator, req.threshold)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result
@app.post("/api/test-notification")
async def test_notification(req: NotificationTestRequest):
    # Intentionally synthetic test event. It is not used by the real sensor agent.
    event = {
        "sensor": "TEST_SENSOR",
        "value": 999.0,
        "operator": ">",
        "threshold": 100.0,
        "timestamp": "test",
        "mode": execution_mode,
    }
    results = []
    if req.email:
        try:
            results.append(await asyncio.to_thread(send_email_notification, event, req.email_to))
        except Exception as exc:
            results.append({"sent": False, "channel": "email", "error": str(exc)})
    if not results:
        return {"message": "Select email for the test."}
    return {"results": results}
@app.post("/api/reset")
def reset():
    thresholds.clear()
    alert_history.clear()
    alert_keys_seen.clear()
    clear_threshold_state()
    live_monitoring_stopped.clear()
    live_telemetry_buffer.clear()
    alert_agent.processed_events = 0
    alert_agent.notification_jobs = 0
    alert_agent.last_event = None
    alert_agent.last_notification_results = []
    return {"message": "Thresholds, crossing state, and alert history reset."}
# ============================================================
# STARTUP
# ============================================================
def load_default_json():
    global current_json_data
    if DEFAULT_JSON_PATH.exists():
        try:
            with DEFAULT_JSON_PATH.open("r", encoding="utf-8") as f:
                parsed = normalize_sensor_document(json.load(f))
            current_json_data = parsed
            invalidate_sensor_registry_cache()
        except Exception as exc:
            print(f"Error loading initial static file: {exc}")
load_default_json()
@app.on_event("startup")
async def application_startup():
    global _main_event_loop
    _main_event_loop = asyncio.get_running_loop()
    start_mqtt_listener()
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)