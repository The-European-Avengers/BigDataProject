import React, { useState, useEffect } from "react";
import "../components/HistoricalPrice.css";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import api from "../services/api";

function HistoricalPrice() {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedArea, setSelectedArea] = useState("all");
  const [selectedYear, setSelectedYear] = useState("2025");
  const [dateRange, setDateRange] = useState({ start: "", end: "" });

  const fetchHistoricalData = React.useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = { limit: 10000, sortDesc: true };
      if (selectedArea !== "all") {
        params.dkarea = parseInt(selectedArea, 10);
      }
      // Always include year parameter - default to current selection or 2025
      params.year =
        selectedYear && selectedYear !== "all" ? selectedYear : "2025";
      if (dateRange.start) {
        params.startDate = dateRange.start;
      }
      if (dateRange.end) {
        params.endDate = dateRange.end;
      }

      console.log("Fetching historical price data with params:", params);

      const response = await api.get("http://localhost:5001/historical-price", {
        params,
      });

      console.log("Response received:", response.data);

      if (!response.data.rows || response.data.rows.length === 0) {
        console.warn("No rows in response");
        setData([]);
        setLoading(false);
        return;
      }

      // Response rows are objects with keys like: ts, dkarea, price_eur_mwh, year
      // Note: ts has incorrect year (e.g., "53970-12-15 08:00:00"), use year attribute instead
      const sortedData = response.data.rows
        .filter((row) => {
          if (!row || !row.ts) {
            console.warn("Skipping invalid row:", row);
            return false;
          }
          return true;
        })
        .map((row, idx) => {
          // Extract month-day-time from ts (ignore the wrong year)
          // Format: "53970-12-15 08:00:000.0" -> extract "12-15 08:00"
          const tsParts = row.ts.split("-");
          let monthDayTime = "";
          if (tsParts.length >= 3) {
            // Get month and the rest (day + time)
            monthDayTime = tsParts.slice(1).join("-");
          }

          // Build correct date using year attribute
          const correctYear = row.year || "2025";
          const correctedDateStr = `${correctYear}-${monthDayTime}`;
          const timestamp = new Date(correctedDateStr);

          if (isNaN(timestamp.getTime())) {
            console.error(
              `Invalid date for row ${idx}:`,
              row.ts,
              "corrected:",
              correctedDateStr
            );
          }

          // Format timestamp for display - only month and day for x-axis
          const timestampForDisplay = timestamp.toLocaleString("en-US", {
            month: "short",
            day: "numeric",
          });

          return {
            timestamp: timestampForDisplay,
            fullTimestamp: correctedDateStr,
            dkArea: row.dkarea,
            price: parseFloat(row.price_eur_mwh),
            year: correctYear,
          };
        });

      console.log("Processed data length:", sortedData.length);
      console.log("First 3 records:", sortedData.slice(0, 3));

      // Filter by selected area if needed (client-side filtering as backup)
      let filteredData = sortedData;
      if (selectedArea !== "all") {
        const selectedAreaNum = parseInt(selectedArea, 10);
        filteredData = sortedData.filter(
          (item) => item.dkArea === selectedAreaNum
        );
        console.log(
          `Filtered data for DK${selectedAreaNum}:`,
          filteredData.length,
          "records"
        );
      }

      setData(filteredData);
    } catch (err) {
      console.error("Error fetching historical price data:", err);
      setError(`Failed to load historical price data: ${err.message}`);
    } finally {
      setLoading(false);
    }
  }, [selectedArea, selectedYear, dateRange]);

  // Initial load
  useEffect(() => {
    console.log("HistoricalPrice component mounted, fetching initial data");
    fetchHistoricalData();
  }, [fetchHistoricalData]);

  const handleRefresh = () => {
    fetchHistoricalData();
  };

  const chartData = data.reduce((acc, item) => {
    const existing = acc.find((d) => d.timestamp === item.timestamp);
    if (existing) {
      if (item.dkArea === 1) {
        existing.DK1 = item.price;
      } else if (item.dkArea === 2) {
        existing.DK2 = item.price;
      }
    } else {
      const newItem = {
        timestamp: item.timestamp,
        fullTimestamp: item.fullTimestamp,
      };
      if (selectedArea === "all") {
        if (item.dkArea === 1) {
          newItem.DK1 = item.price;
        } else if (item.dkArea === 2) {
          newItem.DK2 = item.price;
        }
      } else {
        newItem.price = item.price;
      }
      acc.push(newItem);
    }
    return acc;
  }, []);

  console.log("Chart data prepared:", chartData);
  console.log("Chart data length:", chartData.length);

  const stats = {
    avgPrice:
      data.length > 0
        ? (
            data.reduce((sum, item) => sum + item.price, 0) / data.length
          ).toFixed(2)
        : "0.00",
    maxPrice:
      data.length > 0
        ? Math.max(...data.map((item) => item.price)).toFixed(2)
        : "0.00",
    minPrice:
      data.length > 0
        ? Math.min(...data.map((item) => item.price)).toFixed(2)
        : "0.00",
    dataPoints: data.length,
  };

  return (
    <div className="historical-price-page">
      <div className="page-header">
        <h1>Historical Price Analysis</h1>
        <p>Energy prices from 2020 to 2025</p>
      </div>

      <div className="controls-panel">
        <div className="control-row">
          <div className="filter-controls">
            <div className="filter-group">
              <label htmlFor="area-select">DK Area:</label>
              <select
                id="area-select"
                className="filter-select"
                value={selectedArea}
                onChange={(e) => setSelectedArea(e.target.value)}
              >
                <option value="all">All Areas</option>
                <option value="1">DK1</option>
                <option value="2">DK2</option>
              </select>
            </div>

            <div className="filter-group">
              <label htmlFor="year-select">Year:</label>
              <select
                id="year-select"
                className="filter-select"
                value={selectedYear}
                onChange={(e) => setSelectedYear(e.target.value)}
              >
                <option value="all">All Years</option>
                <option value="2025">2025</option>
                <option value="2024">2024</option>
                <option value="2023">2023</option>
                <option value="2022">2022</option>
                <option value="2021">2021</option>
                <option value="2020">2020</option>
              </select>
            </div>

            <div className="filter-group">
              <label htmlFor="start-date">Start Date:</label>
              <input
                id="start-date"
                type="date"
                className="filter-input-date"
                value={dateRange.start}
                onChange={(e) =>
                  setDateRange({ ...dateRange, start: e.target.value })
                }
              />
            </div>

            <div className="filter-group">
              <label htmlFor="end-date">End Date:</label>
              <input
                id="end-date"
                type="date"
                className="filter-input-date"
                value={dateRange.end}
                onChange={(e) =>
                  setDateRange({ ...dateRange, end: e.target.value })
                }
              />
            </div>
          </div>

          <button
            className="btn btn-refresh"
            onClick={handleRefresh}
            disabled={loading}
          >
            {loading ? "Loading..." : "Refresh"}
          </button>
        </div>
      </div>

      {error && <div className="error">{error}</div>}

      {loading && (
        <div className="loading">Loading historical price data...</div>
      )}

      {!loading && data.length === 0 && !error && (
        <div className="error">
          No data available. Please check your filters or try refreshing.
        </div>
      )}

      {!loading && data.length > 0 && (
        <>
          <div className="stats-top">
            <div className="stats-grid">
              <div className="stat-card stat-price">
                <label>Average Price</label>
                <value>{stats.avgPrice}</value>
                <unit>EUR/MWh</unit>
              </div>
              <div className="stat-card stat-production">
                <label>Maximum Price</label>
                <value>{stats.maxPrice}</value>
                <unit>EUR/MWh</unit>
              </div>
              <div className="stat-card stat-consumption">
                <label>Minimum Price</label>
                <value>{stats.minPrice}</value>
                <unit>EUR/MWh</unit>
              </div>
              <div className="stat-card stat-max">
                <label>Data Points</label>
                <value>{stats.dataPoints}</value>
                <unit>Records</unit>
              </div>
            </div>
          </div>

          <div className="charts-grid">
            <div className="chart-section">
              <div className="chart-label">
                <h2>Price Trends</h2>
                <p>
                  {selectedArea === "all"
                    ? "DK1 and DK2 comparison"
                    : `DK${selectedArea} pricing over time`}
                </p>
              </div>

              <div className="chart-card">
                <ResponsiveContainer width="100%" height={400}>
                  <LineChart data={chartData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis
                      dataKey="timestamp"
                      tick={{ fill: "#cbd5e1", fontSize: 12 }}
                    />
                    <YAxis tick={{ fill: "#cbd5e1", fontSize: 12 }} />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: "#0f172a",
                        border: "1px solid rgba(51, 65, 85, 0.5)",
                        borderRadius: "8px",
                      }}
                      labelStyle={{ color: "#e2e8f0" }}
                    />
                    <Legend />
                    {selectedArea === "all" ? (
                      <>
                        <Line
                          type="monotone"
                          dataKey="DK1"
                          stroke="#60a5fa"
                          dot={false}
                          isAnimationActive={false}
                          strokeWidth={2}
                        />
                        <Line
                          type="monotone"
                          dataKey="DK2"
                          stroke="#fbbf24"
                          dot={false}
                          isAnimationActive={false}
                          strokeWidth={2}
                        />
                      </>
                    ) : (
                      <Line
                        type="monotone"
                        dataKey="price"
                        stroke="#60a5fa"
                        dot={false}
                        isAnimationActive={false}
                        strokeWidth={2}
                      />
                    )}
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>
          </div>
        </>
      )}

      {!loading && data.length === 0 && !error && (
        <div className="empty-state">
          <p>No historical price data available for the selected filters.</p>
        </div>
      )}
    </div>
  );
}

export default HistoricalPrice;
