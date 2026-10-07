import { useEffect, useMemo, useRef, useState } from "react";

const API_URL = "http://127.0.0.1:8000";
const WS_URL = "ws://127.0.0.1:8000/ws/telemetry";

const SERIES_COLORS = [
  "#8E44AD",
  "#4B0082",
  "#2563EB",
  "#16A34A",
  "#D4A72C",
  "#F97316",
  "#DC2626",
  "#EC4899",
  "#0D9488",
  "#7C3AED",
  "#92400E",
  "#0891B2",
];

function categoryOf(label) {
  const raw = String(label || "").trim().toLowerCase();
  const compact = raw.replace(/[^a-z0-9]/g, "");

  if (
    /temperature/.test(raw) ||
    /temp\d*/.test(compact) ||
    /(^|\D)t\d+$/.test(raw)
  ) {
    return "Temperature";
  }

  if (
    /pressure/.test(raw) ||
    /press\d*/.test(compact) ||
    /(^|\D)p\d+$/.test(raw)
  ) {
    return "Pressure";
  }

  if (
    /humidity/.test(raw) ||
    /hum\d*/.test(compact) ||
    /(^|\D)h\d+$/.test(raw)
  ) {
    return "Humidity";
  }

  return null;
}

function timestampOf(row, index) {
  return (
    row?.timestamp ??
    row?.time ??
    row?.x ??
    row?.dateTime ??
    row?.datetime ??
    row?.DateTime ??
    row?.X ??
    String(index + 1)
  );
}

function valueOf(row, sensor) {
  if (!row || typeof row !== "object") return null;

  if (Object.prototype.hasOwnProperty.call(row, sensor)) {
    return row[sensor];
  }

  const wanted = String(sensor).toLowerCase();

  const key = Object.keys(row).find(
    (k) => k.toLowerCase() === wanted
  );

  return key ? row[key] : null;
}

function numericValue(row, sensor) {
  const n = Number(valueOf(row, sensor));
  return Number.isFinite(n) ? n : null;
}

function rowsFrom(result) {
  if (Array.isArray(result)) return result;

  if (!result || typeof result !== "object") return [];

  for (const key of [
    "data",
    "readings",
    "rows",
    "points",
    "values",
    "records",
    "items",
  ]) {
    if (Array.isArray(result[key])) {
      return result[key];
    }
  }

  return [];
}

function condition(value, op, threshold) {
  const v = Number(value);
  const t = Number(threshold);

  if (!Number.isFinite(v) || !Number.isFinite(t)) {
    return false;
  }

  if (op === ">") return v > t;
  if (op === "<") return v < t;
  if (op === ">=") return v >= t;
  if (op === "<=") return v <= t;
  if (op === "=") return v === t;
  if (op === "!=") return v !== t;

  return false;
}

function firstCrossing(rows, op, threshold) {
  let previous = false;

  for (let i = 0; i < rows.length; i += 1) {
    const current = condition(
      rows[i].value,
      op,
      threshold
    );

    if (current && !previous) {
      return i;
    }

    previous = current;
  }

  return -1;
}

function JsonAgent() {
  const [sensors, setSensors] = useState([]);
  const [selectedSensor, setSelectedSensor] = useState("");
  const [operator, setOperator] = useState(">");
  const [thresholdValue, setThresholdValue] = useState("");

  const [chatQuestion, setChatQuestion] = useState("");
  const [chatMessages, setChatMessages] = useState([]);
  const [chatOpen, setChatOpen] = useState(false);

  const [loadingSensors, setLoadingSensors] = useState(true);
  const [sendingMessage, setSendingMessage] = useState(false);
  const [settingThreshold, setSettingThreshold] = useState(false);

  const [backendOnline, setBackendOnline] = useState(false);
  const [thresholdStatus, setThresholdStatus] = useState(null);

  const [activeView, setActiveView] = useState("list");

  const [listData, setListData] = useState([]);
  const [graphData, setGraphData] = useState([]);

  const [listLoading, setListLoading] = useState(false);
  const [graphLoading, setGraphLoading] = useState(false);

  const [listLoaded, setListLoaded] = useState(false);
  const [graphLoaded, setGraphLoaded] = useState(false);

  const [crossingIndex, setCrossingIndex] = useState(-1);

  const [graphType, setGraphType] = useState("Temperature");

  const socketRef = useRef(null);

  const selectedSensorRef = useRef("");
  const operatorRef = useRef(">");
  const thresholdRef = useRef("");

  const liveRowsRef = useRef([]);
  const liveModeRef = useRef(false);
  const liveStoppedRef = useRef(false);
  const previousConditionRef = useRef(false);

  useEffect(() => {
    selectedSensorRef.current = selectedSensor;
    operatorRef.current = operator;
    thresholdRef.current = thresholdValue;
  }, [selectedSensor, operator, thresholdValue]);

  useEffect(() => {
    loadSensors();

    return () => {
      closeSocket();
    };
  }, []);

  async function loadSensors() {
    setLoadingSensors(true);

    try {
      const response = await fetch(`${API_URL}/api/sensors`);

      if (!response.ok) {
        throw new Error("Unable to load sensors.");
      }

      const result = await response.json();

      const labels = Array.isArray(result.sensors)
        ? result.sensors
            .map((s) => {
              if (typeof s === "string") {
                return s.trim();
              }

              return (
                s?.SensorLabelName ||
                s?.sensorLabelName ||
                s?.label ||
                s?.name ||
                ""
              ).trim();
            })
            .filter(Boolean)
        : [];

      const unique = [...new Set(labels)];

      setSensors(unique);
      setSelectedSensor("");

      selectedSensorRef.current = "";

      setBackendOnline(true);
      setThresholdStatus(null);

      setListData([]);
      setGraphData([]);

      setListLoaded(false);
      setGraphLoaded(false);

      setCrossingIndex(-1);

      resetLiveState();
    } catch (error) {
      console.error("Sensor loading error:", error);

      setSensors([]);
      setBackendOnline(false);
    } finally {
      setLoadingSensors(false);
    }
  }

  function resetLiveState() {
    liveRowsRef.current = [];
    liveModeRef.current = false;
    liveStoppedRef.current = false;
    previousConditionRef.current = false;
  }

  function closeSocket() {
    const socket = socketRef.current;

    if (!socket) return;

    try {
      socket.onopen = null;
      socket.onmessage = null;
      socket.onerror = null;
      socket.onclose = null;

      socket.close();
    } catch (error) {
      console.error("WebSocket close error:", error);
    }

    socketRef.current = null;
  }

  async function loadSensorData(sensor) {
    const response = await fetch(
      `${API_URL}/api/view-data/${encodeURIComponent(sensor)}?view=graph`
    );

    if (!response.ok) {
      throw new Error("Unable to load sensor data.");
    }

    const result = await response.json();

    const raw = rowsFrom(result);

    const rows = raw
      .map((row, index) => ({
        original: row,
        index,
        timestamp: timestampOf(row, index),
        value: numericValue(row, sensor),
      }))
      .filter((r) => r.value !== null);

    return {
      rows,

      mode: result.mode || "static",

      stopped: Boolean(result.live_monitoring_stopped),

      crossingIndex: Number.isInteger(result.crossing_index)
        ? result.crossing_index
        : -1,

      crossingEvent: result.crossing_event || null,
    };
  }

  function processLiveRow(row) {
    if (liveStoppedRef.current) {
      return;
    }

    const sensor = selectedSensorRef.current;

    if (!sensor) {
      return;
    }

    const value = numericValue(row, sensor);

    if (value === null) {
      return;
    }

    if (!liveModeRef.current) {
      liveModeRef.current = true;

      liveRowsRef.current = [];

      previousConditionRef.current = false;

      setListData([]);
      setGraphData([]);

      setCrossingIndex(-1);

      setListLoaded(true);
      setGraphLoaded(true);
    }

    const rows = liveRowsRef.current;

    const next = [
      ...rows,
      {
        original: row,
        index: rows.length,
        timestamp: timestampOf(row, rows.length),
        value,
      },
    ];

    const t = Number(thresholdRef.current);

    const matched =
      thresholdRef.current !== "" &&
      Number.isFinite(t)
        ? condition(
            value,
            operatorRef.current,
            t
          )
        : false;

    const crossed =
      matched && !previousConditionRef.current;

    previousConditionRef.current = matched;

    liveRowsRef.current = next;

    /*
     * GRAPH:
     * Always keep every live reading received before
     * the first threshold crossing.
     */
    setGraphData(next);
    setGraphLoaded(true);

    setBackendOnline(true);

    if (crossed) {
      const index = next.length - 1;

      /*
       * LIST:
       * Only show the first threshold-crossing row.
       */
      setListData([next[index]]);

      setListLoaded(true);

      setCrossingIndex(index);

      liveStoppedRef.current = true;

      setThresholdStatus({
        type: "crossed",
        message: `Threshold crossed at ${next[index].timestamp}, Value: ${value}`,
      });

      /*
       * Stop receiving live telemetry after
       * the first FALSE -> TRUE crossing.
       */
      closeSocket();
    } else {
      /*
       * Before crossing, do not display the live rows
       * in List View.
       */
      setListData([]);
      setCrossingIndex(-1);
      setListLoaded(true);
    }
  }

  function connectSocket() {
    const sensor = selectedSensorRef.current;

    if (
      !sensor ||
      thresholdRef.current === "" ||
      liveStoppedRef.current
    ) {
      return;
    }

    const existing = socketRef.current;

    if (
      existing &&
      (
        existing.readyState === WebSocket.OPEN ||
        existing.readyState === WebSocket.CONNECTING
      )
    ) {
      return;
    }

    try {
      const socket = new WebSocket(WS_URL);

      socketRef.current = socket;

      socket.onopen = () => {
        setBackendOnline(true);
      };

      socket.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);

          if (message.type === "telemetry") {
            processLiveRow(message.data);
          }

          else if (message.type === "threshold_alert") {
            const data = message.data || {};

            if (
              data.sensor &&
              String(data.sensor).toLowerCase() !==
                String(sensor).toLowerCase()
            ) {
              return;
            }

            setThresholdStatus({
              type: "crossed",
              message:
                data.timestamp !== undefined
                  ? `Threshold crossed at ${data.timestamp}, Value: ${data.value}`
                  : "Threshold crossed.",
            });
          }

          else if (
            message.type === "live_monitoring_stopped"
          ) {
            liveStoppedRef.current = true;

            const e = message.data?.event;

            if (e) {
              setThresholdStatus({
                type: "crossed",
                message:
                  e.timestamp !== undefined
                    ? `Threshold crossed at ${e.timestamp}, Value: ${e.value}`
                    : "Threshold crossed.",
              });
            }

            closeSocket();
          }
        } catch (error) {
          console.error(
            "Invalid WebSocket message:",
            error
          );
        }
      };

      socket.onerror = (error) => {
        console.error(
          "Live WebSocket error:",
          error
        );
      };

      socket.onclose = () => {
        if (socketRef.current === socket) {
          socketRef.current = null;
        }
      };
    } catch (error) {
      console.error(
        "Unable to connect to live WebSocket:",
        error
      );
    }
  }

  async function checkThreshold(
    sensor,
    op,
    threshold
  ) {
    try {
      const response = await fetch(
        `${API_URL}/api/check-threshold`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            sensor,
            operator: op,
            threshold,
          }),
        }
      );

      const result = await response.json();

      if (!response.ok) {
        throw new Error(
          result.detail ||
            "Threshold check failed."
        );
      }

      setBackendOnline(true);

      return result;
    } catch (error) {
      console.error(
        "Threshold check error:",
        error
      );

      return null;
    }
  }

  async function processSensorData() {
    if (!selectedSensor) {
      return;
    }

    try {
      const result =
        await loadSensorData(selectedSensor);

      /*
       * LIVE DATA
       *
       * Live data must come from the backend live
       * buffer / telemetry stream, not from the static
       * JSON file.
       */
      if (result.mode === "live") {
        liveModeRef.current = true;

        liveRowsRef.current = result.rows;

        liveStoppedRef.current = result.stopped;

        setGraphData(result.rows);

        setGraphLoaded(true);

        setCrossingIndex(
          result.crossingIndex
        );

        /*
         * Only the first crossing row goes into List View.
         */
        if (
          result.crossingIndex >= 0 &&
          result.rows[result.crossingIndex]
        ) {
          setListData([
            result.rows[result.crossingIndex],
          ]);
        } else {
          setListData([]);
        }

        setListLoaded(true);

        if (result.stopped) {
          setThresholdStatus({
            type: "crossed",
            message:
              result.crossingEvent?.timestamp !==
              undefined
                ? `Threshold crossed at ${result.crossingEvent.timestamp}, Value: ${result.crossingEvent.value}`
                : "Live monitoring stopped after threshold crossing.",
          });
        }
      }

      /*
       * STATIC DATA
       */
      else {
        resetLiveState();

        const crossing =
          thresholdValue === ""
            ? -1
            : firstCrossing(
                result.rows,
                operator,
                Number(thresholdValue)
              );

        setCrossingIndex(crossing);

        /*
         * LIST:
         * Only first crossing row.
         *
         * If there is no crossing:
         * show all readings.
         */
        if (
          crossing >= 0 &&
          result.rows[crossing]
        ) {
          setListData([
            result.rows[crossing],
          ]);
        } else {
          setListData(result.rows);
        }

        /*
         * GRAPH:
         * Always show complete uploaded file.
         */
        setGraphData(result.rows);

        setListLoaded(true);
        setGraphLoaded(true);
      }

      setBackendOnline(true);

      /*
       * Only connect WebSocket for live mode.
       */
      if (result.mode === "live") {
        connectSocket();
      } else {
        closeSocket();
      }
    } catch (error) {
      console.error(
        "Sensor data processing error:",
        error
      );

      setThresholdStatus({
        type: "error",
        message:
          error.message ||
          "Unable to process sensor data.",
      });
    }
  }

  async function handleSetThreshold() {
    if (!selectedSensor) {
      setThresholdStatus({
        type: "error",
        message:
          "Please select a sensor first.",
      });

      return;
    }

    if (thresholdValue === "") {
      setThresholdStatus({
        type: "error",
        message:
          "Please enter a threshold value.",
      });

      return;
    }

    const threshold = Number(
      thresholdValue
    );

    if (!Number.isFinite(threshold)) {
      setThresholdStatus({
        type: "error",
        message:
          "Please enter a valid numeric threshold.",
      });

      return;
    }

    setSettingThreshold(true);
    setThresholdStatus(null);
    setCrossingIndex(-1);

    resetLiveState();
    closeSocket();

    try {
      const response = await fetch(
        `${API_URL}/api/threshold`,
        {
          method: "POST",
          headers: {
            "Content-Type":
              "application/json",
          },
          body: JSON.stringify({
            sensor: selectedSensor,
            operator,
            threshold,
          }),
        }
      );

      const result =
        await response.json();

      if (!response.ok) {
        throw new Error(
          result.detail ||
            "Unable to set threshold."
        );
      }

      setBackendOnline(true);

      await processSensorData();

      const check =
        await checkThreshold(
          selectedSensor,
          operator,
          threshold
        );

      if (check?.crossed) {
        const c =
          check.crossings?.[0];

        setThresholdStatus({
          type: "crossed",
          message: c
            ? `Threshold crossed at ${c.timestamp}, Value: ${c.value}`
            : "Threshold crossed.",
        });
      } else {
        setThresholdStatus(
          (old) =>
            old?.type === "crossed"
              ? old
              : {
                  type: "success",
                  message:
                    "Threshold set. Threshold not crossed.",
                }
        );
      }

      /*
       * processSensorData() already decides whether
       * WebSocket is required.
       */
    } catch (error) {
      console.error(
        "Threshold error:",
        error
      );

      setThresholdStatus({
        type: "error",
        message:
          error.message ||
          "Unable to set threshold.",
      });
    } finally {
      setSettingThreshold(false);
    }
  }

  async function refreshListView() {
    if (!selectedSensor) {
      setThresholdStatus({
        type: "error",
        message:
          "Please select a sensor first.",
      });

      return;
    }

    setListLoading(true);

    try {
      const result =
        await loadSensorData(
          selectedSensor
        );

      if (result.mode === "live") {
        liveModeRef.current = true;

        liveRowsRef.current =
          result.rows;

        liveStoppedRef.current =
          result.stopped;

        setCrossingIndex(
          result.crossingIndex
        );

        /*
         * Live List View:
         * only first crossing row.
         */
        if (
          result.crossingIndex >= 0 &&
          result.rows[
            result.crossingIndex
          ]
        ) {
          setListData([
            result.rows[
              result.crossingIndex
            ],
          ]);
        } else {
          setListData([]);
        }

        /*
         * Do NOT replace graphData when
         * refreshing List View.
         */
      } else {
        liveModeRef.current = false;
        liveRowsRef.current = [];

        const crossing =
          thresholdValue === ""
            ? -1
            : firstCrossing(
                result.rows,
                operator,
                Number(thresholdValue)
              );

        setCrossingIndex(crossing);

        /*
         * Static List View:
         * only crossing row, otherwise all.
         */
        if (
          crossing >= 0 &&
          result.rows[crossing]
        ) {
          setListData([
            result.rows[crossing],
          ]);
        } else {
          setListData(result.rows);
        }
      }

      setListLoaded(true);
      setBackendOnline(true);
    } catch (error) {
      setThresholdStatus({
        type: "error",
        message:
          error.message ||
          "Unable to refresh list.",
      });
    } finally {
      setListLoading(false);
    }
  }

  async function refreshGraphView() {
    if (!selectedSensor) {
      setThresholdStatus({
        type: "error",
        message:
          "Please select a sensor first.",
      });

      return;
    }

    setGraphLoading(true);

    try {
      const result =
        await loadSensorData(
          selectedSensor
        );

      /*
       * Graph View always gets all available
       * readings.
       */
      setGraphData(result.rows);
      setGraphLoaded(true);

      setBackendOnline(true);

      if (result.mode === "live") {
        liveModeRef.current = true;

        liveRowsRef.current =
          result.rows;

        liveStoppedRef.current =
          result.stopped;

        setCrossingIndex(
          result.crossingIndex
        );

        /*
         * Do not show all live rows in List View.
         */
        if (
          result.crossingIndex >= 0 &&
          result.rows[
            result.crossingIndex
          ]
        ) {
          setListData([
            result.rows[
              result.crossingIndex
            ],
          ]);
        } else {
          setListData([]);
        }

        setListLoaded(true);
      } else {
        liveModeRef.current = false;

        liveRowsRef.current = [];

        const crossing =
          thresholdValue === ""
            ? -1
            : firstCrossing(
                result.rows,
                operator,
                Number(thresholdValue)
              );

        setCrossingIndex(crossing);

        if (
          crossing >= 0 &&
          result.rows[crossing]
        ) {
          setListData([
            result.rows[crossing],
          ]);
        } else {
          setListData(result.rows);
        }

        setListLoaded(true);
      }
    } catch (error) {
      setThresholdStatus({
        type: "error",
        message:
          error.message ||
          "Unable to refresh graph.",
      });
    } finally {
      setGraphLoading(false);
    }
  }

  function handleSensorChange(sensor) {
    closeSocket();

    setSelectedSensor(sensor);

    selectedSensorRef.current =
      sensor;

    setThresholdStatus(null);

    setCrossingIndex(-1);

    setListData([]);
    setGraphData([]);

    setListLoaded(false);
    setGraphLoaded(false);

    resetLiveState();
  }

  async function handleSendMessage(event) {
    event?.preventDefault();

    const question =
      chatQuestion.trim();

    if (
      !question ||
      sendingMessage
    ) {
      return;
    }

    setChatMessages((old) => [
      ...old,
      {
        role: "user",
        text: question,
      },
    ]);

    setChatQuestion("");
    setSendingMessage(true);

    try {
      /*
       * Chatbot receives exactly the data
       * currently displayed by the active view.
       */
      const viewData =
        activeView === "list"
          ? listData
          : graphData;

      const response = await fetch(
        `${API_URL}/api/chat`,
        {
          method: "POST",
          headers: {
            "Content-Type":
              "application/json",
          },
          body: JSON.stringify({
            question,

            selected_sensor:
              selectedSensor || null,

            view: activeView,

            view_data: viewData,

            threshold:
              thresholdValue === ""
                ? null
                : Number(thresholdValue),

            operator,
          }),
        }
      );

      const result =
        await response.json();

      if (!response.ok) {
        throw new Error(
          result.detail ||
            "Chat request failed."
        );
      }

      setBackendOnline(true);

      setChatMessages((old) => [
        ...old,
        {
          role: "ai",
          text:
            result.answer ||
            "No answer was returned by the backend.",
        },
      ]);
    } catch (error) {
      setBackendOnline(false);

      setChatMessages((old) => [
        ...old,
        {
          role: "ai",
          text:
            "Unable to connect to the backend. Please make sure the FastAPI server is running on port 8000.",
        },
      ]);
    } finally {
      setSendingMessage(false);
    }
  }

  const categorySensors = useMemo(() => {
    const result = {
      Temperature: [],
      Pressure: [],
      Humidity: [],
    };

    sensors.forEach((sensor) => {
      const category =
        categoryOf(sensor);

      if (category) {
        result[category].push(sensor);
      }
    });

    return result;
  }, [sensors]);

  /*
   * GRAPH OPTIONS
   *
   * Only:
   *   Temperature
   *   Pressure
   *   Humidity
   *   Temperature + Pressure
   *
   * No:
   *   Temperature + Humidity
   *   Humidity + Pressure
   */
  const graphOptions = useMemo(() => {
    const options = [];

    if (
      categorySensors.Temperature.length
    ) {
      options.push("Temperature");
    }

    if (
      categorySensors.Pressure.length
    ) {
      options.push("Pressure");
    }

    if (
      categorySensors.Humidity.length
    ) {
      options.push("Humidity");
    }

    if (
      categorySensors.Temperature.length &&
      categorySensors.Pressure.length
    ) {
      options.push(
        "Temperature + Pressure"
      );
    }

    return options;
  }, [categorySensors]);

  useEffect(() => {
    if (
      graphOptions.length &&
      !graphOptions.includes(graphType)
    ) {
      setGraphType(
        graphOptions[0]
      );
    }
  }, [graphOptions, graphType]);

  function status() {
    if (
      !backendOnline &&
      !loadingSensors
    ) {
      return {
        className: "offline",
        text: "Offline",
      };
    }

    if (
      thresholdStatus?.type ===
      "crossed"
    ) {
      return {
        className: "crossed",
        text: "Threshold Crossed",
      };
    }

    if (selectedSensor) {
      return {
        className: "ready",
        text: "Ready",
      };
    }

    return {
      className: "waiting",
      text: "Waiting",
    };
  }

  const agentStatus = status();

  function SensorList() {
    if (listLoading) {
      return (
        <div className="data-empty">
          Loading list data...
        </div>
      );
    }

    if (!listData.length) {
      return (
        <div className="data-empty">
          <div className="empty-icon">
            ☷
          </div>

          <h3>No list data</h3>

          <p>
            Select a sensor and set a
            threshold to load sensor
            readings.
          </p>
        </div>
      );
    }

    /*
     * IMPORTANT:
     *
     * crossingIndex belongs to graphData,
     * not listData.
     *
     * Therefore we must NOT do:
     *
     * index === crossingIndex
     *
     * because List View contains only one
     * crossing row.
     */
    const hasCrossing =
      crossingIndex >= 0;

    return (
      <div className="list-wrapper">
        <div className="view-data-header">
          <div>
            <h3>
              {selectedSensor}
            </h3>

            <p>
              {hasCrossing
                ? "First threshold crossing"
                : "Sensor readings"}{" "}
              · {listData.length}{" "}
              {listData.length === 1
                ? "reading"
                : "readings"}
            </p>
          </div>

          <button
            className="refresh-button"
            onClick={refreshListView}
            disabled={listLoading}
          >
            ↻ Refresh
          </button>
        </div>

        <div className="table-container">
          <table className="sensor-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Timestamp</th>
                <th>
                  {selectedSensor}
                </th>

                {thresholdValue !== "" && (
                  <th>Status</th>
                )}
              </tr>
            </thead>

            <tbody>
              {listData.map(
                (row, index) => {
                  /*
                   * Since List View contains only
                   * the crossing row when crossed,
                   * the only row is the crossing.
                   */
                  const crossed =
                    hasCrossing;

                  return (
                    <tr
                      key={`${row.timestamp}-${index}`}
                      className={
                        crossed
                          ? "crossing-row"
                          : ""
                      }
                    >
                      <td>
                        {index + 1}
                      </td>

                      <td>
                        {String(
                          row.timestamp
                        )}
                      </td>

                      <td className="value-cell">
                        {row.value}
                      </td>

                      {thresholdValue !== "" && (
                        <td>
                          <span
                            className={`condition-badge ${
                              crossed
                                ? "crossed-badge"
                                : "normal-badge"
                            }`}
                          >
                            {crossed
                              ? "CROSSED"
                              : "FALSE"}
                          </span>
                        </td>
                      )}
                    </tr>
                  );
                }
              )}
            </tbody>
          </table>
        </div>

        <div className="list-footer">
          {hasCrossing
            ? "Only the first threshold-crossing timestamp is displayed."
            : "Threshold was not crossed. All available readings are displayed."}
        </div>
      </div>
    );
  }

  function SensorGraph() {
    if (graphLoading) {
      return (
        <div className="data-empty">
          Loading graph data...
        </div>
      );
    }

    if (!graphData.length) {
      return (
        <div className="data-empty">
          <div className="empty-icon">
            📈
          </div>

          <h3>No graph data</h3>

          <p>
            Select a sensor and set a
            threshold to load the
            sensor data.
          </p>
        </div>
      );
    }

    return (
      <MultiSensorGraph
        rows={graphData}
        categories={categorySensors}
        graphType={graphType}
        selectedSensor={selectedSensor}
        threshold={
          thresholdValue === ""
            ? null
            : Number(thresholdValue)
        }
        operator={operator}
        crossingIndex={
          crossingIndex
        }
        onRefresh={
          refreshGraphView
        }
      />
    );
  }

  return (
    <div className="agent-page">
      <div className="page-header">
        <div>
          <h1>JSON AGENT</h1>

          <p>
            Monitor sensor thresholds
            and analyze JSON data.
          </p>
        </div>

        <div
          className={`agent-status ${agentStatus.className}`}
        >
          <span className="status-dot"></span>
          {agentStatus.text}
        </div>
      </div>

      <section className="card threshold-card">
        <div className="card-header">
          <div>
            <h2>
              Threshold Monitoring
            </h2>

            <p>
              Select a sensor and
              configure its threshold
              condition.
            </p>
          </div>
        </div>

        <div className="form-grid">
          <div className="form-group">
            <label>Sensor</label>

            <select
              value={selectedSensor}
              onChange={(e) =>
                handleSensorChange(
                  e.target.value
                )
              }
              disabled={loadingSensors}
            >
              <option value="">
                {loadingSensors
                  ? "Loading sensors..."
                  : sensors.length === 0
                  ? "No sensors available"
                  : "Select Sensor"}
              </option>

              {sensors.map(
                (sensor) => (
                  <option
                    key={sensor}
                    value={sensor}
                  >
                    {sensor}
                  </option>
                )
              )}
            </select>
          </div>

          <div className="form-group">
            <label>
              Condition
            </label>

            <select
              value={operator}
              onChange={(e) =>
                setOperator(
                  e.target.value
                )
              }
            >
              <option value=">">
                &gt;
              </option>

              <option value="<">
                &lt;
              </option>

              <option value=">=">
                &gt;=
              </option>

              <option value="<=">
                &lt;=
              </option>

              <option value="=">
                =
              </option>

              <option value="!=">
                !=
              </option>
            </select>
          </div>

          <div className="form-group">
            <label>
              Threshold
            </label>

            <input
              type="number"
              step="any"
              value={thresholdValue}
              onChange={(e) =>
                setThresholdValue(
                  e.target.value
                )
              }
              placeholder="Enter value"
            />
          </div>
        </div>

        <button
          className="primary-button"
          onClick={
            handleSetThreshold
          }
          disabled={
            settingThreshold ||
            loadingSensors ||
            sensors.length === 0
          }
        >
          {settingThreshold
            ? "Processing..."
            : "Set Threshold"}
        </button>

        {thresholdStatus && (
          <div
            className={`monitoring-status ${thresholdStatus.type}`}
          >
            {thresholdStatus.message}
          </div>
        )}
      </section>

      <section className="data-view-card">
        <div className="view-tabs">
          <button
            type="button"
            className={`view-tab ${
              activeView === "list"
                ? "active"
                : ""
            }`}
            onClick={() =>
              setActiveView("list")
            }
          >
            <span className="tab-icon">
              ☷
            </span>

            <span>
              List View
            </span>
          </button>

          <button
            type="button"
            className={`view-tab ${
              activeView === "graph"
                ? "active"
                : ""
            }`}
            onClick={() =>
              setActiveView("graph")
            }
          >
            <span className="tab-icon">
              ⌁
            </span>

            <span>
              Graph View
            </span>
          </button>
        </div>

        {activeView === "graph" &&
          graphLoaded && (
            <div className="graph-selector-row">
              <div className="graph-selector-label">
                <label htmlFor="graph-type">
                  Graph Type
                </label>

                <span>
                  Choose a sensor
                  category or
                  Temperature +
                  Pressure
                  comparison.
                </span>
              </div>

              <select
                id="graph-type"
                className="graph-type-dropdown"
                value={graphType}
                onChange={(e) =>
                  setGraphType(
                    e.target.value
                  )
                }
                disabled={
                  !graphOptions.length
                }
              >
                {graphOptions.map(
                  (option) => (
                    <option
                      key={option}
                      value={option}
                    >
                      {option}
                    </option>
                  )
                )}
              </select>
            </div>
          )}

        <div className="view-content">
          {activeView === "list" ? (
            <SensorList />
          ) : (
            <SensorGraph />
          )}
        </div>
      </section>

      {!chatOpen && (
        <button
          className="chat-floating-button"
          onClick={() =>
            setChatOpen(true)
          }
          title="Open Sensor Chatbot"
          aria-label="Open Sensor Chatbot"
        >
          <span>+</span>
        </button>
      )}

      {chatOpen && (
        <div className="floating-chat-window">
          <div className="floating-chat-header">
            <div className="floating-chat-title">
              <div className="floating-bot-icon">
                🤖
              </div>

              <div>
                <strong>
                  Sensor Assistant
                </strong>

                <span>
                  {activeView ===
                  "list"
                    ? "List View"
                    : "Graph View"}
                </span>
              </div>
            </div>

            <div className="floating-chat-actions">
              <button
                type="button"
                onClick={() =>
                  setChatOpen(false)
                }
                title="Minimize"
              >
                −
              </button>

              <button
                type="button"
                onClick={() => {
                  setChatOpen(false);
                  setChatMessages(
                    []
                  );
                }}
                title="Close"
              >
                ×
              </button>
            </div>
          </div>

          <div className="floating-chat-context">
            <span className="context-dot"></span>

            {selectedSensor
              ? `${selectedSensor} · ${
                  activeView ===
                  "list"
                    ? "List data"
                    : "Graph data"
                }`
              : "Select a sensor to begin"}
          </div>

          <div className="floating-chat-messages">
            {chatMessages.length ===
              0 && (
              <div className="floating-welcome">
                <div className="welcome-bot">
                  🤖
                </div>

                <h3>
                  Sensor Assistant
                </h3>

                <p>
                  Ask questions about
                  the currently
                  selected sensor and
                  view.
                </p>
              </div>
            )}

            {chatMessages.map(
              (message, index) => (
                <div
                  key={`${message.role}-${index}`}
                  className={`floating-message ${
                    message.role ===
                    "user"
                      ? "floating-user-message"
                      : "floating-ai-message"
                  }`}
                >
                  <div className="message-role">
                    {message.role ===
                    "user"
                      ? "You"
                      : "AI"}
                  </div>

                  <div className="message-text">
                    {message.text}
                  </div>
                </div>
              )
            )}

            {sendingMessage && (
              <div className="typing-indicator">
                <span></span>
                <span></span>
                <span></span>
                AI is processing...
              </div>
            )}
          </div>

          <form
            className="floating-chat-input"
            onSubmit={
              handleSendMessage
            }
          >
            <input
              type="text"
              value={chatQuestion}
              onChange={(e) =>
                setChatQuestion(
                  e.target.value
                )
              }
              placeholder={
                activeView === "list"
                  ? "Ask about list values..."
                  : "Ask about graph data..."
              }
              disabled={
                sendingMessage
              }
            />

            <button
              type="submit"
              disabled={
                sendingMessage ||
                !chatQuestion.trim()
              }
            >
              ➤
            </button>
          </form>
        </div>
      )}

      <style>{`
        * {
          box-sizing: border-box;
        }

        .agent-page {
          min-height: 100vh;
          padding: 32px;
          background: #f6f7fb;
          color: #202334;
          font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        }

        .page-header {
          max-width: 1200px;
          margin: 0 auto 24px;
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 20px;
        }

        .page-header h1 {
          margin: 0 0 7px;
          font-size: 28px;
          font-weight: 750;
        }

        .page-header p {
          margin: 0;
          color: #6c7182;
          font-size: 14px;
        }

        .agent-status {
          display: flex;
          align-items: center;
          gap: 8px;
          padding: 8px 13px;
          border-radius: 999px;
          background: white;
          border: 1px solid #e4e6ed;
          font-size: 12px;
          font-weight: 700;
        }

        .status-dot {
          width: 8px;
          height: 8px;
          border-radius: 50%;
          background: #a5a9b5;
        }

        .agent-status.ready .status-dot {
          background: #6d5dfc;
        }

        .agent-status.crossed .status-dot {
          background: #dc6b35;
        }

        .agent-status.offline .status-dot {
          background: #d74c4c;
        }

        .card,
        .data-view-card {
          max-width: 1200px;
          margin: 0 auto 20px;
          background: white;
          border: 1px solid #e4e6ed;
          border-radius: 16px;
          box-shadow: 0 4px 18px rgba(25, 30, 50, 0.04);
        }

        .threshold-card {
          padding: 24px;
        }

        .card-header {
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 20px;
          margin-bottom: 22px;
        }

        .card-header h2 {
          margin: 0 0 5px;
          font-size: 18px;
        }

        .card-header p {
          margin: 0;
          color: #777d8e;
          font-size: 13px;
        }

        .form-grid {
          display: grid;
          grid-template-columns: minmax(220px, 1.4fr) minmax(130px, 0.6fr) minmax(220px, 1fr);
          gap: 16px;
          margin-bottom: 20px;
        }

        .form-group {
          display: flex;
          flex-direction: column;
          gap: 8px;
        }

        .form-group label {
          font-size: 12px;
          font-weight: 700;
          color: #555b6e;
        }

        .form-group select,
        .form-group input {
          width: 100%;
          height: 44px;
          padding: 0 13px;
          border: 1px solid #dfe2ea;
          border-radius: 9px;
          outline: none;
          background: white;
          color: #252a3a;
          font-size: 14px;
        }

        .form-group select:focus,
        .form-group input:focus {
          border-color: #7568ef;
          box-shadow: 0 0 0 3px rgba(117, 104, 239, 0.11);
        }

        .primary-button {
          height: 44px;
          padding: 0 22px;
          border: none;
          border-radius: 9px;
          background: #6859dc;
          color: white;
          font-size: 14px;
          font-weight: 700;
          cursor: pointer;
        }

        .primary-button:disabled {
          opacity: 0.55;
          cursor: not-allowed;
        }

        .monitoring-status {
          margin-top: 16px;
          padding: 12px 14px;
          border-radius: 9px;
          font-size: 13px;
          font-weight: 600;
          border: 1px solid #e3e5ed;
          background: #f8f8fb;
        }

        .monitoring-status.success {
          background: #f2f8f5;
          border-color: #d5e9de;
          color: #347252;
        }

        .monitoring-status.crossed {
          background: #fff6ed;
          border-color: #f1dcc8;
          color: #a45725;
        }

        .monitoring-status.error {
          background: #fff3f3;
          border-color: #f0d2d2;
          color: #a33e3e;
        }

        .data-view-card {
          overflow: hidden;
        }

        .view-tabs {
          display: flex;
          align-items: stretch;
          padding: 0 18px;
          border-bottom: 1px solid #e5e7ee;
          background: #fbfbfd;
        }

        .view-tab {
          position: relative;
          min-width: 145px;
          height: 55px;
          display: flex;
          align-items: center;
          justify-content: center;
          gap: 8px;
          border: none;
          background: transparent;
          color: #777d8e;
          font-size: 13px;
          font-weight: 700;
          cursor: pointer;
        }

        .view-tab::after {
          content: "";
          position: absolute;
          left: 12px;
          right: 12px;
          bottom: -1px;
          height: 3px;
          border-radius: 3px 3px 0 0;
          background: transparent;
        }

        .view-tab.active {
          color: #5e50c8;
          background: white;
        }

        .view-tab.active::after {
          background: #6859dc;
        }

        .tab-icon {
          font-size: 18px;
        }

        .view-content {
          min-height: 430px;
          padding: 22px;
        }

        .view-data-header {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 15px;
          margin-bottom: 18px;
        }

        .view-data-header h3 {
          margin: 0 0 4px;
          font-size: 16px;
        }

        .view-data-header p {
          margin: 0;
          color: #7b8090;
          font-size: 12px;
        }

        .refresh-button {
          height: 36px;
          padding: 0 13px;
          border: 1px solid #dedfea;
          border-radius: 8px;
          background: white;
          color: #5b536e;
          font-size: 12px;
          font-weight: 700;
          cursor: pointer;
        }

        .refresh-button:hover {
          background: #f7f6fc;
        }

        .table-container {
          max-height: 430px;
          overflow: auto;
          border: 1px solid #e5e7ee;
          border-radius: 10px;
        }

        .sensor-table {
          width: 100%;
          border-collapse: collapse;
          font-size: 13px;
        }

        .sensor-table th {
          position: sticky;
          top: 0;
          z-index: 2;
          padding: 12px 14px;
          background: #f8f8fb;
          border-bottom: 1px solid #e3e5ec;
          color: #646a7b;
          text-align: left;
          font-size: 11px;
          text-transform: uppercase;
        }

        .sensor-table td {
          padding: 11px 14px;
          border-bottom: 1px solid #f0f1f5;
          color: #353a4a;
        }

        .sensor-table tbody tr:hover {
          background: #fafaff;
        }

        .value-cell {
          font-weight: 700;
          color: #3e386e !important;
        }

        .crossing-row {
          background: #fff8ef;
        }

        .condition-badge {
          display: inline-flex;
          padding: 4px 8px;
          border-radius: 999px;
          font-size: 10px;
          font-weight: 800;
        }

        .normal-badge {
          background: #f1f3f7;
          color: #6f7482;
        }

        .crossed-badge {
          background: #ffe9d4;
          color: #a65724;
        }

        .list-footer {
          margin-top: 12px;
          color: #858a99;
          font-size: 11px;
        }

        .graph-selector-row {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 20px;
          margin: 18px 22px 0;
          padding: 14px 16px;
          border: 1px solid #e5e7ee;
          border-radius: 10px;
          background: #fbfbfd;
        }

        .graph-selector-label {
          display: flex;
          flex-direction: column;
          gap: 4px;
        }

        .graph-selector-label label {
          font-size: 12px;
          font-weight: 800;
          color: #555b6e;
        }

        .graph-selector-label span {
          font-size: 11px;
          color: #858a99;
        }

        .graph-type-dropdown {
          min-width: 245px;
          height: 40px;
          padding: 0 12px;
          border: 1px solid #dfe2ea;
          border-radius: 9px;
          background: white;
          color: #303445;
          font-size: 13px;
          outline: none;
        }

        .graph-type-dropdown:focus {
          border-color: #7568ef;
          box-shadow: 0 0 0 3px rgba(117, 104, 239, 0.11);
        }

        .graph-container {
          width: 100%;
          overflow-x: auto;
          border: 1px solid #e5e7ee;
          border-radius: 10px;
          background: white;
        }

        .sensor-svg {
          display: block;
          width: 100%;
          min-width: 700px;
          height: 390px;
        }

        .graph-grid {
          stroke: #eceef4;
          stroke-width: 1;
        }

        .graph-axis {
          stroke: #cfd2dc;
          stroke-width: 1.2;
        }

        .graph-label {
          fill: #858a99;
          font-size: 11px;
        }

        .graph-axis-value-left {
          font-weight: 700;
        }

        .graph-axis-value-right {
          font-weight: 700;
        }

        .graph-axis-title-left,
        .graph-axis-title-right {
          font-size: 11px;
          font-weight: 800;
        }

        .graph-footer {
          display: flex;
          flex-wrap: wrap;
          gap: 24px;
          margin-top: 12px;
          color: #777d8e;
          font-size: 12px;
        }

        .graph-footer strong {
          color: #363b4a;
        }

        .crossing-text {
          color: #a45725;
          font-weight: 700;
        }

        .graph-legend {
          display: flex;
          flex-wrap: wrap;
          align-items: center;
          gap: 5px 10px;
          margin: 8px 0 10px;
          padding: 7px 9px;
          border: 1px solid #e5e2ec;
          border-radius: 8px;
          background: #faf9fc;
        }

        .legend-item {
          display: inline-flex;
          align-items: center;
          gap: 4px;
          color: #50525f;
          font-size: 10px;
          line-height: 1.2;
          white-space: nowrap;
        }

        .legend-line {
          width: 18px;
          height: 3px;
          border-radius: 5px;
          display: inline-block;
          flex: none;
        }

        .legend-axis {
          font-size: 8px;
          color: #85808f;
          margin-left: 1px;
        }

        .graph-hover-line {
          stroke: #6d6679;
          stroke-width: 1.2;
          stroke-dasharray: 5 4;
          pointer-events: none;
        }

        .graph-hover-point {
          stroke: #fff;
          stroke-width: 2;
        }

        .graph-tooltip-box {
          fill: rgba(255, 255, 255, 0.98);
          stroke: #d9d4e3;
          stroke-width: 1;
        }

        .graph-tooltip-title {
          fill: #514a63;
          font-size: 12px;
          font-weight: 800;
        }

        .graph-tooltip-divider {
          stroke: #ece9f1;
        }

        .graph-tooltip-value {
          fill: #50525f;
          font-size: 11px;
          font-weight: 600;
        }

        .graph-note {
          margin-top: 10px;
          padding: 9px 12px;
          border-radius: 9px;
          background: #f7f5fa;
          color: #686676;
          font-size: 11px;
          line-height: 1.5;
        }

        .data-empty {
          min-height: 370px;
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          text-align: center;
          color: #74798a;
        }

        .empty-icon {
          width: 54px;
          height: 54px;
          display: flex;
          align-items: center;
          justify-content: center;
          margin-bottom: 12px;
          border-radius: 14px;
          background: #f0effa;
          font-size: 25px;
        }

        .data-empty h3 {
          margin: 0 0 5px;
          color: #363a4a;
          font-size: 15px;
        }

        .data-empty p {
          max-width: 430px;
          margin: 0;
          font-size: 12px;
          line-height: 1.6;
        }

        .chat-floating-button {
          position: fixed;
          right: 28px;
          bottom: 28px;
          width: 58px;
          height: 58px;
          display: flex;
          align-items: center;
          justify-content: center;
          border: none;
          border-radius: 50%;
          background: #6859dc;
          color: white;
          box-shadow: 0 10px 28px rgba(61, 49, 140, 0.27);
          cursor: pointer;
          z-index: 1000;
        }

        .chat-floating-button span {
          font-size: 30px;
          font-weight: 300;
        }

        .floating-chat-window {
          position: fixed;
          right: 28px;
          bottom: 28px;
          width: 390px;
          height: 560px;
          display: flex;
          flex-direction: column;
          overflow: hidden;
          background: white;
          border: 1px solid #dedfea;
          border-radius: 16px;
          box-shadow: 0 18px 55px rgba(26, 29, 49, 0.2);
          z-index: 1001;
        }

        .floating-chat-header {
          min-height: 68px;
          display: flex;
          align-items: center;
          justify-content: space-between;
          padding: 12px 14px;
          background: #6859dc;
          color: white;
        }

        .floating-chat-title {
          display: flex;
          align-items: center;
          gap: 10px;
        }

        .floating-bot-icon {
          width: 38px;
          height: 38px;
          display: flex;
          align-items: center;
          justify-content: center;
          border-radius: 11px;
          background: rgba(255, 255, 255, 0.16);
          font-size: 19px;
        }

        .floating-chat-title strong {
          display: block;
          font-size: 13px;
        }

        .floating-chat-title span {
          display: block;
          margin-top: 3px;
          opacity: 0.78;
          font-size: 10px;
        }

        .floating-chat-actions {
          display: flex;
          gap: 4px;
        }

        .floating-chat-actions button {
          width: 30px;
          height: 30px;
          border: none;
          border-radius: 7px;
          background: transparent;
          color: white;
          font-size: 20px;
          cursor: pointer;
        }

        .floating-chat-actions button:hover {
          background: rgba(255, 255, 255, 0.14);
        }

        .floating-chat-context {
          display: flex;
          align-items: center;
          gap: 7px;
          min-height: 34px;
          padding: 0 14px;
          background: #f8f8fc;
          border-bottom: 1px solid #e8e9ef;
          color: #727789;
          font-size: 10px;
        }

        .context-dot {
          width: 6px;
          height: 6px;
          border-radius: 50%;
          background: #6859dc;
        }

        .floating-chat-messages {
          flex: 1;
          padding: 16px;
          overflow-y: auto;
          background: #fbfbfd;
        }

        .floating-welcome {
          min-height: 280px;
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          text-align: center;
        }

        .welcome-bot {
          width: 52px;
          height: 52px;
          display: flex;
          align-items: center;
          justify-content: center;
          margin-bottom: 13px;
          border-radius: 15px;
          background: #eeecfb;
          font-size: 25px;
        }

        .floating-welcome h3 {
          margin: 0 0 6px;
          font-size: 14px;
        }

        .floating-welcome p {
          max-width: 270px;
          margin: 0;
          color: #808595;
          font-size: 11px;
          line-height: 1.6;
        }

        .floating-message {
          max-width: 88%;
          margin-bottom: 12px;
          padding: 10px 12px;
          border-radius: 11px;
          font-size: 12px;
          line-height: 1.55;
        }

        .floating-user-message {
          margin-left: auto;
          background: #6859dc;
          color: white;
          border-bottom-right-radius: 3px;
        }

        .floating-ai-message {
          margin-right: auto;
          background: white;
          border: 1px solid #e2e4eb;
          color: #383d4d;
          border-bottom-left-radius: 3px;
        }

        .message-role {
          margin-bottom: 4px;
          font-size: 9px;
          font-weight: 800;
          opacity: 0.65;
          text-transform: uppercase;
        }

        .message-text {
          white-space: pre-wrap;
          word-break: break-word;
        }

        .typing-indicator {
          display: flex;
          align-items: center;
          gap: 4px;
          color: #85899a;
          font-size: 10px;
        }

        .typing-indicator span {
          width: 5px;
          height: 5px;
          border-radius: 50%;
          background: #8b83c9;
        }

        .floating-chat-input {
          display: flex;
          gap: 8px;
          padding: 11px;
          background: white;
          border-top: 1px solid #e6e7ed;
        }

        .floating-chat-input input {
          flex: 1;
          min-width: 0;
          height: 40px;
          padding: 0 11px;
          border: 1px solid #dfe1e8;
          border-radius: 9px;
          outline: none;
          font-size: 12px;
        }

        .floating-chat-input button {
          width: 40px;
          height: 40px;
          border: none;
          border-radius: 9px;
          background: #6859dc;
          color: white;
          font-size: 16px;
          cursor: pointer;
        }

        .floating-chat-input button:disabled {
          opacity: 0.45;
          cursor: not-allowed;
        }

        @media (max-width: 800px) {
          .agent-page {
            padding: 18px;
          }

          .page-header {
            align-items: flex-start;
            flex-direction: column;
          }

          .card-header {
            flex-direction: column;
          }

          .form-grid {
            grid-template-columns: 1fr;
          }

          .view-tabs {
            padding: 0 8px;
          }

          .view-tab {
            min-width: 50%;
          }

          .view-content {
            padding: 15px;
          }

          .graph-selector-row {
            margin: 15px 15px 0;
            align-items: flex-start;
            flex-direction: column;
          }

          .graph-type-dropdown {
            width: 100%;
          }

          .floating-chat-window {
            right: 14px;
            bottom: 14px;
            width: calc(100vw - 28px);
            height: min(600px, calc(100vh - 28px));
          }

          .chat-floating-button {
            right: 18px;
            bottom: 18px;
          }
        }
      `}</style>
    </div>
  );
}

function MultiSensorGraph({
  rows,
  categories,
  graphType,
  selectedSensor,
  threshold,
  operator,
  crossingIndex,
  onRefresh,
}) {
  const [hoverIndex, setHoverIndex] =
    useState(null);

  const width = 1000;
  const height = 390;

  const left = 72;
  const right = 72;
  const top = 30;
  const bottom = 55;

  const plotWidth =
    width - left - right;

  const plotHeight =
    height - top - bottom;

  const selectedCategories =
    graphType.split(" + ");

  const primary =
    selectedCategories[0];

  const secondary =
    selectedCategories[1] || null;

  /*
   * Single category:
   *
   * Temperature -> Temp1, Temp2, ... Temp6
   * Pressure    -> Pressure1, ...
   * Humidity     -> Humidity1, ...
   *
   * Comparison:
   *
   * Temperature + Pressure
   */
  const primarySensors =
    categories[primary] || [];

  const secondarySensors =
    secondary
      ? categories[secondary] || []
      : [];

  const allSeries = [
    ...primarySensors.map(
      (sensor) => ({
        sensor,
        category: primary,
      })
    ),

    ...secondarySensors.map(
      (sensor) => ({
        sensor,
        category: secondary,
      })
    ),
  ];

  if (
    !allSeries.length &&
    selectedSensor
  ) {
    allSeries.push({
      sensor: selectedSensor,
      category:
        categoryOf(
          selectedSensor
        ) || primary,
    });
  }

  const ranges = useMemo(() => {
    const valuesFor = (
      category
    ) =>
      allSeries
        .filter(
          (series) =>
            series.category ===
            category
        )
        .flatMap((series) =>
          rows
            .map((row) =>
              numericValue(
                row.original ||
                  row,
                series.sensor
              )
            )
            .filter(
              (value) =>
                value !== null
            )
        );

    const range = (values) => {
      if (!values.length) {
        return {
          min: 0,
          max: 1,
        };
      }

      let min =
        Math.min(...values);

      let max =
        Math.max(...values);

      if (min === max) {
        min -= 1;
        max += 1;
      }

      return {
        min,
        max,
      };
    };

    return {
      primary: range(
        valuesFor(primary)
      ),

      secondary: secondary
        ? range(
            valuesFor(
              secondary
            )
          )
        : null,
    };
  }, [
    rows,
    primary,
    secondary,
    allSeries,
  ]);

  const x = (index) =>
    left +
    (index /
      Math.max(
        rows.length - 1,
        1
      )) *
      plotWidth;

  const y = (
    value,
    category
  ) => {
    const range =
      secondary &&
      category === secondary
        ? ranges.secondary
        : ranges.primary;

    if (!range) {
      return (
        top +
        plotHeight / 2
      );
    }

    return (
      top +
      (1 -
        (value - range.min) /
          (range.max -
            range.min)) *
        plotHeight
    );
  };

  const makePath = (
    series
  ) => {
    let path = "";
    let started = false;

    rows.forEach(
      (row, index) => {
        const value =
          numericValue(
            row.original ||
              row,
            series.sensor
          );

        if (value === null) {
          started = false;
          return;
        }

        const point = `${x(
          index
        )},${y(
          value,
          series.category
        )}`;

        path += started
          ? ` L ${point}`
          : `M ${point}`;

        started = true;
      }
    );

    return path;
  };

  const primaryRange =
    ranges.primary;

  const thresholdNumber =
    Number(threshold);

  const thresholdCategory =
    categoryOf(
      selectedSensor
    );

  const thresholdIsDisplayed =
    threshold !== null &&
    Number.isFinite(
      thresholdNumber
    ) &&
    allSeries.some(
      (series) =>
        series.category ===
        thresholdCategory
    );

  const thresholdRange =
    thresholdCategory ===
    secondary
      ? ranges.secondary
      : ranges.primary;

  const thresholdY =
    thresholdIsDisplayed &&
    thresholdRange &&
    thresholdNumber >=
      thresholdRange.min &&
    thresholdNumber <=
      thresholdRange.max
      ? y(
          thresholdNumber,
          thresholdCategory
        )
      : null;

  const hoveredRow =
    hoverIndex === null
      ? null
      : rows[hoverIndex];

  const tooltipSeries =
    hoverIndex === null
      ? []
      : allSeries
          .map(
            (
              series,
              index
            ) => ({
              ...series,

              value:
                numericValue(
                  rows[
                    hoverIndex
                  ]?.original ||
                    rows[
                      hoverIndex
                    ],
                  series.sensor
                ),

              color:
                SERIES_COLORS[
                  index %
                    SERIES_COLORS.length
                ],
            })
          )
          .filter(
            (series) =>
              series.value !==
              null
          );

  const tooltipW = 255;

  const tooltipH =
    45 +
    tooltipSeries.length *
      20;

  let tooltipX =
    hoverIndex === null
      ? 0
      : x(hoverIndex) + 14;

  if (
    tooltipX + tooltipW >
    width - 8
  ) {
    tooltipX =
      x(hoverIndex) -
      tooltipW -
      14;
  }

  const tooltipY = 12;

  function move(event) {
    const svg =
      event.currentTarget.getBoundingClientRect();

    const localX =
      ((event.clientX -
        svg.left) /
        svg.width) *
      width;

    const ratio =
      Math.max(
        0,
        Math.min(
          1,
          (localX - left) /
            plotWidth
        )
      );

    setHoverIndex(
      Math.round(
        ratio *
          Math.max(
            rows.length - 1,
            0
          )
      )
    );
  }

  const showRightAxis =
    Boolean(secondary);

  const axisColorLeft =
    primary === "Temperature"
      ? "#8E44AD"
      : primary === "Pressure"
      ? "#2563EB"
      : "#16A34A";

  const axisColorRight =
    secondary === "Pressure"
      ? "#DC2626"
      : secondary ===
        "Temperature"
      ? "#8E44AD"
      : "#16A34A";

  return (
    <div className="graph-wrapper">
      <div className="view-data-header">
        <div>
          <h3>
            {graphType}
          </h3>

          <p>
            {selectedSensor} ·{" "}
            {rows.length} readings
          </p>
        </div>

        <button
          className="refresh-button"
          onClick={onRefresh}
        >
          ↻ Refresh
        </button>
      </div>

      <div className="graph-legend">
        {allSeries.map(
          (
            series,
            index
          ) => (
            <span
              className="legend-item"
              key={`${series.category}-${series.sensor}`}
            >
              <span
                className="legend-line"
                style={{
                  background:
                    SERIES_COLORS[
                      index %
                        SERIES_COLORS.length
                    ],
                }}
              />

              <strong>
                {series.sensor}
              </strong>

              {secondary && (
                <span className="legend-axis">
                  {series.category ===
                  secondary
                    ? "R"
                    : "L"}
                </span>
              )}
            </span>
          )
        )}
      </div>

      <div className="graph-container">
        <svg
          viewBox={`0 0 ${width} ${height}`}
          className="sensor-svg"
          onMouseMove={move}
          onMouseLeave={() =>
            setHoverIndex(null)
          }
        >
          {[0, 1, 2, 3, 4].map(
            (step) => {
              const yy =
                top +
                (step / 4) *
                  plotHeight;

              const leftValue =
                primaryRange.max -
                (step / 4) *
                  (primaryRange.max -
                    primaryRange.min);

              const rightValue =
                ranges.secondary
                  ? ranges.secondary
                      .max -
                    (step / 4) *
                      (ranges.secondary.max -
                        ranges.secondary.min)
                  : null;

              return (
                <g key={step}>
                  <line
                    x1={left}
                    y1={yy}
                    x2={
                      width -
                      right
                    }
                    y2={yy}
                    className="graph-grid"
                  />

                  <text
                    x={left - 12}
                    y={yy + 4}
                    textAnchor="end"
                    className="graph-label graph-axis-value-left"
                    style={{
                      fill: axisColorLeft,
                    }}
                  >
                    {leftValue.toFixed(
                      2
                    )}
                  </text>

                  {showRightAxis && (
                    <text
                      x={
                        width -
                        right +
                        12
                      }
                      y={yy + 4}
                      textAnchor="start"
                      className="graph-label graph-axis-value-right"
                      style={{
                        fill: axisColorRight,
                      }}
                    >
                      {rightValue.toFixed(
                        2
                      )}
                    </text>
                  )}
                </g>
              );
            }
          )}

          <line
            x1={left}
            y1={top}
            x2={left}
            y2={
              height -
              bottom
            }
            className="graph-axis"
          />

          {showRightAxis && (
            <line
              x1={
                width -
                right
              }
              y1={top}
              x2={
                width -
                right
              }
              y2={
                height -
                bottom
              }
              className="graph-axis"
            />
          )}

          <line
            x1={left}
            y1={
              height -
              bottom
            }
            x2={
              width -
              right
            }
            y2={
              height -
              bottom
            }
            className="graph-axis"
          />

          <text
            x={left}
            y={16}
            textAnchor="start"
            className="graph-axis-title-left"
            style={{
              fill: axisColorLeft,
            }}
          >
            {primary}
          </text>

          {showRightAxis && (
            <text
              x={
                width -
                right
              }
              y={16}
              textAnchor="end"
              className="graph-axis-title-right"
              style={{
                fill: axisColorRight,
              }}
            >
              {secondary}
            </text>
          )}

          {thresholdY !==
            null && (
            <line
              x1={left}
              y1={thresholdY}
              x2={
                width -
                right
              }
              y2={thresholdY}
              className="threshold-line"
            />
          )}

          {allSeries.map(
            (
              series,
              index
            ) => (
              <path
                key={`${series.category}-${series.sensor}`}
                d={makePath(
                  series
                )}
                fill="none"
                stroke={
                  SERIES_COLORS[
                    index %
                      SERIES_COLORS.length
                  ]
                }
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            )
          )}

          {rows.length <=
            250 &&
            allSeries.map(
              (
                series,
                seriesIndex
              ) =>
                rows.map(
                  (
                    row,
                    index
                  ) => {
                    const value =
                      numericValue(
                        row.original ||
                          row,
                        series.sensor
                      );

                    if (
                      value ===
                      null
                    ) {
                      return null;
                    }

                    const isCrossing =
                      index ===
                        crossingIndex &&
                      series.sensor ===
                        selectedSensor;

                    const isHovered =
                      index ===
                      hoverIndex;

                    return (
                      <circle
                        key={`${series.sensor}-${index}`}
                        cx={x(
                          index
                        )}
                        cy={y(
                          value,
                          series.category
                        )}
                        r={
                          isCrossing
                            ? 6
                            : isHovered
                            ? 5
                            : 2.5
                        }
                        fill={
                          isCrossing
                            ? "#dc6b35"
                            : SERIES_COLORS[
                                seriesIndex %
                                  SERIES_COLORS.length
                              ]
                        }
                        className="graph-hover-point"
                      />
                    );
                  }
                )
            )}

          <text
            x={left}
            y={
              height -
              20
            }
            textAnchor="start"
            className="graph-label"
          >
            {String(
              rows[0]?.timestamp
            )}
          </text>

          {rows.length >
            1 && (
            <text
              x={
                width -
                right
              }
              y={
                height -
                20
              }
              textAnchor="end"
              className="graph-label"
            >
              {String(
                rows[
                  rows.length - 1
                ]?.timestamp
              )}
            </text>
          )}

          {hoverIndex !==
            null &&
            hoveredRow && (
              <g>
                <line
                  x1={x(
                    hoverIndex
                  )}
                  y1={top}
                  x2={x(
                    hoverIndex
                  )}
                  y2={
                    height -
                    bottom
                  }
                  className="graph-hover-line"
                />

                <g className="graph-tooltip">
                  <rect
                    x={tooltipX}
                    y={tooltipY}
                    width={
                      tooltipW
                    }
                    height={
                      tooltipH
                    }
                    rx="9"
                    className="graph-tooltip-box"
                  />

                  <text
                    x={
                      tooltipX +
                      12
                    }
                    y={
                      tooltipY +
                      19
                    }
                    className="graph-tooltip-title"
                  >
                    {String(
                      hoveredRow.timestamp
                    )}
                  </text>

                  <line
                    x1={
                      tooltipX +
                      10
                    }
                    y1={
                      tooltipY +
                      27
                    }
                    x2={
                      tooltipX +
                      tooltipW -
                      10
                    }
                    y2={
                      tooltipY +
                      27
                    }
                    className="graph-tooltip-divider"
                  />

                  {tooltipSeries.map(
                    (
                      series,
                      index
                    ) => (
                      <g
                        key={
                          series.sensor
                        }
                      >
                        <circle
                          cx={
                            tooltipX +
                            15
                          }
                          cy={
                            tooltipY +
                            40 +
                            index *
                              20
                          }
                          r="3.5"
                          fill={
                            series.color
                          }
                        />

                        <text
                          x={
                            tooltipX +
                            25
                          }
                          y={
                            tooltipY +
                            44 +
                            index *
                              20
                          }
                          className="graph-tooltip-value"
                        >
                          {
                            series.sensor
                          }
                          :{" "}
                          {
                            series.value
                          }
                        </text>
                      </g>
                    )
                  )}
                </g>
              </g>
            )}
        </svg>
      </div>

      <div className="graph-footer">
        <span>
          Minimum:{" "}
          <strong>
            {primaryRange.min.toFixed(
              2
            )}
          </strong>
        </span>

        <span>
          Maximum:{" "}
          <strong>
            {primaryRange.max.toFixed(
              2
            )}
          </strong>
        </span>

        <span>
          Readings:{" "}
          <strong>
            {rows.length}
          </strong>
        </span>

        {secondary && (
          <span>
            {secondary} on right
            axis
          </span>
        )}

        {crossingIndex >=
          0 && (
          <span className="crossing-text">
            ● Threshold crossing
            detected
          </span>
        )}
      </div>

      <div className="graph-note">
        {secondary
          ? `${primary} is shown on the left Y-axis and ${secondary} on the right Y-axis. Move the cursor over the graph to see the exact values at that timestamp.`
          : `${primary} is shown on the left Y-axis. Move the cursor over the graph to see the exact value at that timestamp.`}
      </div>
    </div>
  );
}

export default JsonAgent;