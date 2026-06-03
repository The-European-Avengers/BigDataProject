import React, { useState, useEffect } from "react";
import hiveService from "../services/hiveService";
import "./TableViewer.css";

function TableViewer({ tableName }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (tableName) {
      fetchTableData();
    }
  }, [tableName]);

  const fetchTableData = async () => {
    try {
      setLoading(true);
      const response = await hiveService.queryTable(tableName);
      setData(response.data);
      setError(null);
    } catch (err) {
      console.error("Error fetching table data:", err);
      setError("Failed to load table data");
    } finally {
      setLoading(false);
    }
  };

  const handleInvalidateCache = async () => {
    try {
      await hiveService.invalidateCache(tableName);
      await fetchTableData();
    } catch (err) {
      console.error("Error invalidating cache:", err);
      setError("Failed to invalidate cache");
    }
  };

  return (
    <div className="table-viewer">
      <div className="table-header">
        <h2>{tableName}</h2>
        <div className="table-actions">
          <button
            className="btn btn-refresh"
            onClick={fetchTableData}
            disabled={loading}
          >
            🔄 Refresh
          </button>
          <button
            className="btn btn-invalidate"
            onClick={handleInvalidateCache}
            disabled={loading}
          >
            ✕ Clear Cache
          </button>
        </div>
      </div>

      {loading && <div className="loading">Loading table data...</div>}
      {error && <div className="error">{error}</div>}

      {data && (
        <div className="table-content">
          <div className="table-info">
            <span>Rows: {data.rows?.length || 0}</span>
            <span>Columns: {data.columns?.length || 0}</span>
          </div>

          {data.rows && data.rows.length > 0 ? (
            <div className="table-scroll">
              <table className="data-table">
                <thead>
                  <tr>
                    {data.columns &&
                      data.columns.map((col, idx) => <th key={idx}>{col}</th>)}
                  </tr>
                </thead>
                <tbody>
                  {data.rows.map((row, idx) => (
                    <tr key={idx}>
                      {data.columns &&
                        data.columns.map((col, colIdx) => (
                          <td key={colIdx}>{row[col] || "-"}</td>
                        ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="no-data">No data available for this table</div>
          )}
        </div>
      )}
    </div>
  );
}

export default TableViewer;
