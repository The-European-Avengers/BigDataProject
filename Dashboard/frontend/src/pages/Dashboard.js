import React from "react";
import "./Dashboard.css";
import PredictionsTimeline from "../components/PredictionsTimeline";

function Dashboard() {
  return (
    <div className="dashboard">
      <div className="graphs-page">
        <PredictionsTimeline />
      </div>
    </div>
  );
}

export default Dashboard;
