import React from "react";
import "./Navigation.css";

function Navigation({ currentPage, setCurrentPage }) {
  return (
    <nav className="navbar">
      <div className="navbar-container">
        <button 
          className="navbar-brand"
          onClick={() => setCurrentPage("dashboard")}
        >
          Energy Analytics
        </button>
        <ul className="navbar-menu">
          <li>
            <button 
              className={`nav-link ${currentPage === "dashboard" ? "active" : ""}`}
              onClick={() => setCurrentPage("dashboard")}
            >
              Day Ahead Predictions
            </button>
          </li>
          <li>
            <button 
              className={`nav-link ${currentPage === "historical" ? "active" : ""}`}
              onClick={() => setCurrentPage("historical")}
            >
              Historical Data
            </button>
          </li>
        </ul>
      </div>
    </nav>
  );
}

export default Navigation;
