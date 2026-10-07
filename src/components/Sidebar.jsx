
function Sidebar({ selectedAgent, setSelectedAgent }) {
  return (
    <aside className="sidebar">
      <div className="sidebar-header">
        <h2>AI AGENT</h2>
        <p>Sensor System</p>
      </div>

      <nav className="sidebar-menu">
        <button
          className={selectedAgent === "json" ? "menu-item active" : "menu-item"}
          onClick={() => setSelectedAgent("json")}
        >
          <span className="menu-icon">🤖</span>
          <span>JSON Agent</span>
        </button>

        <button
          className={
            selectedAgent === "alert" ? "menu-item active" : "menu-item"
          }
          onClick={() => setSelectedAgent("alert")}
        >
          <span className="menu-icon">🚨</span>
          <span>Alert Agent</span>
        </button>

        <button
          className={
            selectedAgent === "report" ? "menu-item active" : "menu-item"
          }
          onClick={() => setSelectedAgent("report")}
        >
          <span className="menu-icon">📄</span>
          <span>Report Agent</span>
        </button>
      </nav>
    </aside>
  );
}

export default Sidebar;
