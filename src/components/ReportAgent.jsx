
function ReportAgent() {
  return (
    <div className="agent-page">
      <div className="page-header">
        <div>
          <h1>Report Agent</h1>
          <p>Generate reports from sensor data and alerts.</p>
        </div>

        <div className="agent-status waiting">
          <span className="status-dot"></span>
          Waiting
        </div>
      </div>

      <section className="card report-card">
        <div className="report-icon">📄</div>

        <h2>Generate Data Report</h2>

        <p>
          Generate a report from the available sensor data and
          threshold events.
        </p>

        <button className="primary-button">
          Generate Report
        </button>

        <div className="report-status">
          <strong>Report Status</strong>
          <span>Not available yet</span>
        </div>
      </section>
    </div>
  );
}

export default ReportAgent;
