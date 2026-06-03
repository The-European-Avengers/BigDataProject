import React, { useState, useEffect } from "react";
import "./App.css";
import Dashboard from "./pages/Dashboard";
import HistoricalData from "./pages/HistoricalData";
import Navigation from "./components/Navigation";
import api from "./services/api";

function App() {
  const [health, setHealth] = useState(false);
  const [currentPage, setCurrentPage] = useState("dashboard");

  useEffect(() => {
    // Check backend health
    api
      .get("/health")
      .then(() => setHealth(true))
      .catch(() => setHealth(false));
  }, []);

  return (
    <div className="App">
      <Navigation currentPage={currentPage} setCurrentPage={setCurrentPage} />
      <main className="container">
        {currentPage === "dashboard" && <Dashboard />}
        {currentPage === "historical" && <HistoricalData />}
      </main>
    </div>
  );
}

export default App;
