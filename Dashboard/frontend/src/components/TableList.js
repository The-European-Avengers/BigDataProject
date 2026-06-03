import React, { useState, useEffect } from "react";
import hiveService from "../services/hiveService";
import "./TableList.css";

function TableList({ onSelectTable }) {
  const [tables, setTables] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetchTables();
  }, []);

  const fetchTables = async () => {
    try {
      setLoading(true);
      const response = await hiveService.getTables();
      setTables(response.data || []);
      setError(null);
    } catch (err) {
      console.error("Error fetching tables:", err);
      setError("Failed to load tables");
      setTables([]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="table-list">
      <div className="table-list-header">
        <h3>📋 Tables</h3>
        <button
          className="refresh-btn"
          onClick={fetchTables}
          title="Refresh tables"
        >
          🔄
        </button>
      </div>

      {loading && <div className="loading">Loading...</div>}
      {error && <div className="error">{error}</div>}

      <ul className="tables">
        {tables.map((table, index) => (
          <li key={index}>
            <button className="table-item" onClick={() => onSelectTable(table)}>
              {table}
            </button>
          </li>
        ))}
      </ul>

      {tables.length === 0 && !loading && (
        <div className="no-tables">No tables found</div>
      )}
    </div>
  );
}

export default TableList;
