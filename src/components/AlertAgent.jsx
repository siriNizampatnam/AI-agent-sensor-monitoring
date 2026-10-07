import { useCallback, useEffect, useRef, useState } from "react";

const API_BASE_URL = "http://127.0.0.1:8000";
const WS_URL = "ws://127.0.0.1:8000/ws/telemetry";

function AlertAgent() {
  const [alerts, setAlerts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [mode, setMode] = useState("static");
  const [lastUpdated, setLastUpdated] = useState(null);

  const [emailConfigured, setEmailConfigured] = useState(false);

  const socketRef = useRef(null);
  const reconnectTimerRef = useRef(null);
  const mountedRef = useRef(true);

  const fetchAlerts = useCallback(async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/api/alerts`);

      if (!response.ok) {
        throw new Error(`Backend returned ${response.status}`);
      }

      const data = await response.json();

      const receivedAlerts = Array.isArray(data.alerts)
        ? data.alerts
        : [];

      if (!mountedRef.current) {
        return;
      }

      setAlerts(receivedAlerts);
      setMode(data.mode || "static");
      setError("");
      setLastUpdated(new Date());
    } catch (err) {
      console.error("Failed to fetch alerts:", err);

      if (mountedRef.current) {
        setError(
          "Unable to connect to the Alert Agent backend."
        );
      }
    } finally {
      if (mountedRef.current) {
        setLoading(false);
      }
    }
  }, []);

  const fetchNotificationStatus = useCallback(async () => {
    try {
      const response = await fetch(
        `${API_BASE_URL}/api/email-config-status`
      );

      if (!response.ok) {
        throw new Error(
          `Backend returned ${response.status}`
        );
      }

      const data = await response.json();

      if (!mountedRef.current) {
        return;
      }

      setEmailConfigured(Boolean(data.ready));
    } catch (err) {
      console.error(
        "Failed to fetch email configuration:",
        err
      );

      if (mountedRef.current) {
        setEmailConfigured(false);
      }
    }
  }, []);

  const closeWebSocket = useCallback(() => {
    if (reconnectTimerRef.current) {
      clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }

    if (socketRef.current) {
      try {
        socketRef.current.close();
      } catch (err) {
        console.error(
          "Failed to close Alert Agent WebSocket:",
          err
        );
      }

      socketRef.current = null;
    }
  }, []);

  const connectWebSocket = useCallback(() => {
    if (!mountedRef.current) {
      return;
    }

    if (
      socketRef.current &&
      (
        socketRef.current.readyState === WebSocket.OPEN ||
        socketRef.current.readyState === WebSocket.CONNECTING
      )
    ) {
      return;
    }

    try {
      const socket = new WebSocket(WS_URL);

      socketRef.current = socket;

      socket.onopen = () => {
        console.log(
          "Alert Agent WebSocket connected."
        );
      };

      socket.onmessage = (event) => {
        let message;

        try {
          message = JSON.parse(event.data);
        } catch (err) {
          console.error(
            "Invalid WebSocket message:",
            err
          );
          return;
        }

        /*
         * Backend sends:
         *
         * {
         *   "type": "threshold_alert",
         *   "data": {
         *      ...
         *   }
         * }
         *
         * When this happens, the backend has already created
         * the alert in alert_history.
         *
         * We therefore refresh /api/alerts instead of
         * creating a second alert in the frontend.
         */
        if (message.type === "threshold_alert") {
          console.log(
            "Threshold alert received:",
            message.data
          );

          fetchAlerts();
          fetchNotificationStatus();

          return;
        }

        /*
         * The backend also sends this event when live
         * monitoring stops at the threshold crossing.
         *
         * The alert itself is handled by threshold_alert,
         * so we only refresh the history here.
         */
        if (
          message.type === "live_monitoring_stopped"
        ) {
          fetchAlerts();
          return;
        }
      };

      socket.onerror = (event) => {
        console.error(
          "Alert Agent WebSocket error:",
          event
        );
      };

      socket.onclose = () => {
        socketRef.current = null;

        if (!mountedRef.current) {
          return;
        }

        /*
         * Reconnect after a short delay if the backend
         * WebSocket connection is lost.
         */
        reconnectTimerRef.current = setTimeout(() => {
          connectWebSocket();
        }, 3000);
      };
    } catch (err) {
      console.error(
        "Failed to connect Alert Agent WebSocket:",
        err
      );
    }
  }, [fetchAlerts, fetchNotificationStatus]);

  /*
   * Initial data load.
   *
   * Alert Agent gets:
   * 1. Existing alert history.
   * 2. Current email configuration.
   */
  useEffect(() => {
    mountedRef.current = true;

    fetchAlerts();
    fetchNotificationStatus();
    connectWebSocket();

    return () => {
      mountedRef.current = false;
      closeWebSocket();
    };
  }, [
    fetchAlerts,
    fetchNotificationStatus,
    connectWebSocket,
    closeWebSocket,
  ]);

  const latestAlert =
    alerts.length > 0
      ? alerts[alerts.length - 1]
      : null;

  const formatValue = (value) => {
    if (
      value === null ||
      value === undefined ||
      value === ""
    ) {
      return "--";
    }

    return String(value);
  };

  const formatTimestamp = (timestamp) => {
    if (!timestamp) {
      return "--";
    }

    return String(timestamp);
  };

  const getAlertStatus = (alert) => {
    if (!alert) {
      return "No Alert";
    }

    return "Threshold Crossed";
  };

  return (
    <div className="agent-page">
      <div className="page-header">
        <div>
          <h1>Alert Agent</h1>

          <p>
            Monitor threshold alerts and notifications.
          </p>
        </div>

        <div
          className={`agent-status ${
            error
              ? "error"
              : alerts.length > 0
              ? "active"
              : "waiting"
          }`}
        >
          <span className="status-dot"></span>

          {error
            ? "Disconnected"
            : alerts.length > 0
            ? "Alert Active"
            : "Waiting"}
        </div>
      </div>

      {error && (
        <section className="card">
          <div className="error-message">
            {error}
          </div>
        </section>
      )}

      <section className="card">
        <h2>Alert Monitoring</h2>

        <p className="section-description">
          Alert Agent monitors threshold events generated by
          the backend.
        </p>

        <div className="alert-details">
          <div className="detail-box">
            <span>Sensor</span>

            <strong>
              {formatValue(latestAlert?.sensor)}
            </strong>
          </div>

          <div className="detail-box">
            <span>Current Value</span>

            <strong>
              {formatValue(
                latestAlert?.value ??
                latestAlert?.current_value
              )}
            </strong>
          </div>

          <div className="detail-box">
            <span>Threshold</span>

            <strong>
              {formatValue(latestAlert?.threshold)}
            </strong>
          </div>

          <div className="detail-box">
            <span>Status</span>

            <strong>
              {getAlertStatus(latestAlert)}
            </strong>
          </div>
        </div>

        {latestAlert && (
          <div className="alert-latest-info">
            <p>
              <strong>Operator:</strong>{" "}
              {formatValue(latestAlert.operator)}
            </p>

            <p>
              <strong>Timestamp:</strong>{" "}
              {formatTimestamp(
                latestAlert.timestamp
              )}
            </p>

            <p>
              <strong>Mode:</strong>{" "}
              {formatValue(
                latestAlert.mode || mode
              )}
            </p>
          </div>
        )}

        <div className="agent-refresh-info">
          <span>
            Mode: <strong>{mode}</strong>
          </span>

          <span>
            Alerts: <strong>{alerts.length}</strong>
          </span>

          {lastUpdated && (
            <span>
              Updated:{" "}
              <strong>
                {lastUpdated.toLocaleTimeString()}
              </strong>
            </span>
          )}
        </div>
      </section>

      <section className="card">
        <div className="card-header">
          <div>
            <h2>Alert History</h2>

            <p>
              Previously triggered alerts will appear here.
            </p>
          </div>

          <button
            type="button"
            onClick={fetchAlerts}
            className="refresh-button"
            disabled={loading}
          >
            {loading ? "Loading..." : "Refresh"}
          </button>
        </div>

        {loading && alerts.length === 0 ? (
          <div className="empty-state">
            <div className="placeholder-icon">
              🔄
            </div>

            <h3>Loading alerts...</h3>

            <p>
              Connecting to the Alert Agent backend.
            </p>
          </div>
        ) : alerts.length === 0 ? (
          <div className="empty-state">
            <div className="placeholder-icon">
              🔔
            </div>

            <h3>No alerts yet</h3>

            <p>
              Alert history will appear when a threshold
              is crossed.
            </p>
          </div>
        ) : (
          <div className="alert-history">
            {alerts.map((alert, index) => (
              <div
                className="alert-item"
                key={
                  alert.alert_id ||
                  alert.id ||
                  `${alert.sensor}-${alert.timestamp}-${index}`
                }
              >
                <div className="alert-item-icon">
                  🔔
                </div>

                <div className="alert-item-content">
                  <div className="alert-item-header">
                    <strong>
                      {formatValue(alert.sensor)}
                    </strong>

                    <span className="alert-status">
                      Threshold Crossed
                    </span>
                  </div>

                  <div className="alert-item-details">
                    <span>
                      Value:{" "}
                      <strong>
                        {formatValue(alert.value)}
                      </strong>
                    </span>

                    <span>
                      Operator:{" "}
                      <strong>
                        {formatValue(
                          alert.operator
                        )}
                      </strong>
                    </span>

                    <span>
                      Threshold:{" "}
                      <strong>
                        {formatValue(
                          alert.threshold
                        )}
                      </strong>
                    </span>
                  </div>

                  <div className="alert-item-time">
                    Timestamp:{" "}
                    {formatTimestamp(
                      alert.timestamp
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </section>

      <section className="notification-grid">
        <div className="card notification-card">
          <span className="notification-icon">
            📧
          </span>

          <h3>Email Notification</h3>

          <p>
            Email notifications are{" "}
            <strong>
              {emailConfigured
                ? "configured"
                : "not configured"}
            </strong>
          </p>
        </div>
      </section>
    </div>
  );
}

export default AlertAgent;