import { useState } from "react";
import Sidebar from "./components/Sidebar";
import JsonAgent from "./components/JsonAgent";
import AlertAgent from "./components/AlertAgent";
import ReportAgent from "./components/ReportAgent";
import "./App.css";

function App() {
  const [selectedAgent, setSelectedAgent] = useState("json");

  return (
    <div className="app">
      <Sidebar
        selectedAgent={selectedAgent}
        setSelectedAgent={setSelectedAgent}
      />

      <main className="main-content">

        {/* Keep all agents mounted.
            Only the selected agent is visible.
            This preserves JsonAgent React state when
            switching to Alert Agent or Report Agent. */}

        <div
          style={{
            display:
              selectedAgent === "json"
                ? "block"
                : "none",
          }}
        >
          <JsonAgent />
        </div>

        <div
          style={{
            display:
              selectedAgent === "alert"
                ? "block"
                : "none",
          }}
        >
          <AlertAgent />
        </div>

        <div
          style={{
            display:
              selectedAgent === "report"
                ? "block"
                : "none",
          }}
        >
          <ReportAgent />
        </div>

      </main>
    </div>
  );
}

export default App;