
from __future__ import annotations

import json
import os
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from mcp.server import MCPServer


mcp = MCPServer("Sensor Alert MCP Server")

ALERT_AGENT_URL = os.getenv(
    "ALERT_AGENT_URL",
    "http://127.0.0.1:8000/api/mcp/alert",
)
MCP_CALLBACK_TIMEOUT = float(os.getenv("MCP_CALLBACK_TIMEOUT", "10"))


@mcp.tool()
def create_alert(
    sensor: str,
    value: float,
    operator: str,
    threshold: float,
    timestamp: str,
    notify_email: bool = True,
    email_to: str | None = None,
) -> dict:
    """
    Receive a threshold-crossing event from the JSON Agent and pass it to the
    Alert Agent through the Alert Agent HTTP endpoint.

    MCP is the communication layer between the JSON Agent and Alert Agent.
    """
    alert = {
        "sensor": str(sensor),
        "value": float(value),
        "operator": str(operator),
        "threshold": float(threshold),
        "timestamp": str(timestamp),
        "crossing": True,
        "notify_email": bool(notify_email),
        "email_to": str(email_to).strip() if email_to else None,
        "message": (
            f"Threshold crossed: sensor '{sensor}' "
            f"value {float(value)} {operator} {float(threshold)}."
        ),
    }

    print("\n[MCP] Alert received from JSON Agent")
    print(f"[MCP] Sensor: {alert['sensor']}")
    print(f"[MCP] Value: {alert['value']}")
    print(f"[MCP] Condition: {alert['operator']} {alert['threshold']}")
    print(f"[MCP] Timestamp: {alert['timestamp']}")
    print(f"[MCP] Forwarding to Alert Agent: {ALERT_AGENT_URL}")

    payload = json.dumps(alert).encode("utf-8")
    request = Request(
        ALERT_AGENT_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlopen(request, timeout=MCP_CALLBACK_TIMEOUT) as response:
            response_body = response.read().decode("utf-8")
            try:
                callback_result = json.loads(response_body) if response_body else {}
            except json.JSONDecodeError:
                callback_result = {"raw_response": response_body}

        print("[MCP] Alert Agent accepted the alert")
        return {
            "status": "forwarded",
            "alert": alert,
            "alert_agent": callback_result,
        }

    except HTTPError as exc:
        error_body = ""
        try:
            error_body = exc.read().decode("utf-8")
        except Exception:
            pass
        print(f"[MCP] Alert Agent HTTP error {exc.code}: {error_body}")
        return {
            "status": "alert_agent_unavailable",
            "alert": alert,
            "error": f"HTTP {exc.code}: {error_body or exc.reason}",
        }

    except URLError as exc:
        print(f"[MCP] Alert Agent connection error: {exc}")
        return {
            "status": "alert_agent_unavailable",
            "alert": alert,
            "error": f"Connection error: {exc}",
        }

    except Exception as exc:
        print(f"[MCP] Alert Agent forwarding failed: {type(exc).__name__}: {exc}")
        return {
            "status": "alert_agent_unavailable",
            "alert": alert,
            "error": f"{type(exc).__name__}: {exc}",
        }


JSON_AGENT_TOOL_URL = os.getenv(
    "JSON_AGENT_TOOL_URL",
    "http://127.0.0.1:8000/api/mcp/tool",
)

def _call_json_agent_tool(tool_name: str, args: dict | None = None) -> dict:
    payload = json.dumps({"tool": tool_name, "args": args or {}}).encode("utf-8")
    request = Request(
        JSON_AGENT_TOOL_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=MCP_CALLBACK_TIMEOUT) as response:
            body = response.read().decode("utf-8")
            result = json.loads(body) if body else {}
        if result.get("error"):
            return result
        return result.get("result", result)
    except Exception as exc:
        return {"error": f"JSON Agent tool '{tool_name}' failed: {type(exc).__name__}: {exc}"}


def _register_json_tool(name: str):
    def decorator(fn):
        return fn
    return decorator


@mcp.tool()
def list_sensors() -> dict:
    return _call_json_agent_tool("list_sensors")

@mcp.tool()
def threshold_sensor() -> dict:
    return _call_json_agent_tool("threshold_sensor")

@mcp.tool()
def selected_sensor(sensor: str | None = None) -> dict:
    return _call_json_agent_tool("selected_sensor", {"sensor": sensor})

@mcp.tool()
def sensor_metadata(sensor: str) -> dict:
    return _call_json_agent_tool("sensor_metadata", {"sensor": sensor})

@mcp.tool()
def latest_value(sensor: str, timestamp_only: bool = False) -> dict:
    return _call_json_agent_tool("latest_value", {"sensor": sensor, "timestamp_only": timestamp_only})

@mcp.tool()
def statistics(sensor: str, metric: str, view: str | None = None, scope: str = "view") -> dict:
    return _call_json_agent_tool("statistics", {"sensor": sensor, "metric": metric, "view": view, "scope": scope})

@mcp.tool()
def values(sensor: str, view: str | None = None, scope: str = "view") -> dict:
    return _call_json_agent_tool("values", {"sensor": sensor, "view": view, "scope": scope})

@mcp.tool()
def reading_count(sensor: str, view: str | None = None, scope: str = "view") -> dict:
    return _call_json_agent_tool("reading_count", {"sensor": sensor, "view": view, "scope": scope})

@mcp.tool()
def threshold_configuration(sensor: str | None = None) -> dict:
    return _call_json_agent_tool("threshold_configuration", {"sensor": sensor})

@mcp.tool()
def set_threshold(sensor: str, operator_str: str, threshold: float, enabled: bool = True, notify_email: bool = True, email_to: str | None = None) -> dict:
    return _call_json_agent_tool("set_threshold", {"sensor": sensor, "operator_str": operator_str, "threshold": threshold, "enabled": enabled, "notify_email": notify_email, "email_to": email_to})

@mcp.tool()
def threshold_analysis(sensor: str, operator_str: str, threshold: float) -> dict:
    return _call_json_agent_tool("threshold_analysis", {"sensor": sensor, "operator_str": operator_str, "threshold": threshold})

@mcp.tool()
def threshold_timestamps(sensor: str, operator_str: str | None = None, threshold: float | None = None, first_only: bool = False, last_only: bool = False, view: str | None = None) -> dict:
    return _call_json_agent_tool("threshold_timestamps", {"sensor": sensor, "operator_str": operator_str, "threshold": threshold, "first_only": first_only, "last_only": last_only, "view": view})

@mcp.tool()
def threshold_counts(sensor: str, operator_str: str | None = None, threshold: float | None = None, count_type: str = "both", view: str | None = None) -> dict:
    return _call_json_agent_tool("threshold_counts", {"sensor": sensor, "operator_str": operator_str, "threshold": threshold, "count_type": count_type, "view": view})

@mcp.tool()
def crossing_detail(sensor: str, detail: str, view: str | None = None) -> dict:
    return _call_json_agent_tool("crossing_detail", {"sensor": sensor, "detail": detail, "view": view})


if __name__ == "__main__":
    mcp.run(
        transport="streamable-http",
        host="127.0.0.1",
        port=8001,
        json_response=True,
    )
