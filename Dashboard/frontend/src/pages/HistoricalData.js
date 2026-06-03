import React, { useState, useEffect, useCallback } from "react";
import "./HistoricalData.css";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  AreaChart,
  Area,
} from "recharts";

function HistoricalData() {
  const [productionData, setProductionData] = useState([]);
  const [consumptionData, setConsumptionData] = useState([]);
  const [priceData, setPriceData] = useState([]);
  const [loading, setLoading] = useState({
    production: false,
    consumption: false,
    price: false,
  });
  const [error, setError] = useState(null);
  const [selectedArea, setSelectedArea] = useState("1");
  const [selectedYear, setSelectedYear] = useState("2023");
  const [selectedMonth, setSelectedMonth] = useState("");
  const [activeTab, setActiveTab] = useState("production");

  // Set default filters when tab changes
  useEffect(() => {
    if (activeTab === "production") {
      setSelectedYear("2023");
      setSelectedMonth("");
    } else if (activeTab === "consumption") {
      setSelectedYear("2023");
      setSelectedMonth("2"); // January
    } else if (activeTab === "price") {
      setSelectedYear("2023");
      setSelectedMonth("");
    }
  }, [activeTab]);

  const fetchProductionData = useCallback(async () => {
    setLoading((prev) => ({ ...prev, production: true }));
    try {
      const params = new URLSearchParams();
      params.append("limit", "10000");
      params.append("sortDesc", "true");
      params.append("dkarea", selectedArea);
      if (selectedYear) params.append("year", selectedYear);
      if (selectedMonth) params.append("month", selectedMonth);

      const url = `http://localhost:5001/historical-production?${params}`;
      console.log("Fetching production:", url);

      const response = await fetch(url);
      if (!response.ok)
        throw new Error(`HTTP error! status: ${response.status}`);

      const data = await response.json();
      console.log("Production data:", data);

      if (data && data.rows && data.rows.length > 0) {
        // Data is already aggregated by day from backend
        const transformed = data.rows
          .map((row) => {
            const dayValue = row["day"];
            const parsedDate = dayValue ? new Date(dayValue) : null;

            return {
              timestamp: parsedDate
                ? parsedDate.toLocaleString("en-US", {
                    month: "short",
                    day: "numeric",
                  })
                : "N/A",
              fullDate: dayValue,
              windProduction: parseFloat(row["totalwindproductionkwh"] || 0),
              sunProduction: parseFloat(row["totalsunproductionkwh"] || 0),
              totalProduction: parseFloat(row["totalproductionkwh"] || 0),
              dkarea: row["dkarea"],
              year: row["year"],
              month: row["month"],
            };
          })
          .filter((item) => item.fullDate !== null);

        transformed.sort((a, b) => new Date(a.fullDate) - new Date(b.fullDate));
        setProductionData(transformed);
      } else {
        setProductionData([]);
      }
    } catch (err) {
      console.error("Error fetching production:", err);
      setError(`Failed to load production data: ${err.message}`);
    } finally {
      setLoading((prev) => ({ ...prev, production: false }));
    }
  }, [selectedArea, selectedYear, selectedMonth]);

  const fetchConsumptionData = useCallback(async () => {
    setLoading((prev) => ({ ...prev, consumption: true }));
    try {
      const params = new URLSearchParams();
      params.append("limit", "10000");
      params.append("sortDesc", "true");
      params.append("dkarea", selectedArea);
      if (selectedYear) params.append("year", selectedYear);
      if (selectedMonth) params.append("month", selectedMonth);

      const url = `http://localhost:5001/historical-consumption?${params}`;
      console.log("Fetching consumption:", url);

      const response = await fetch(url);
      if (!response.ok)
        throw new Error(`HTTP error! status: ${response.status}`);

      const data = await response.json();
      console.log("Consumption data:", data);

      if (data && data.rows && data.rows.length > 0) {
        // Data is already aggregated by day from backend
        const transformed = data.rows
          .map((row) => {
            const dayValue = row["day"];
            const parsedDate = dayValue ? new Date(dayValue) : null;

            return {
              timestamp: parsedDate
                ? parsedDate.toLocaleString("en-US", {
                    month: "short",
                    day: "numeric",
                  })
                : "N/A",
              fullDate: dayValue,
              consumption: parseFloat(row["totalconsumptionkwh"] || 0),
              dkarea: row["dkarea"],
              year: row["year"],
              month: row["month"],
            };
          })
          .filter((item) => item.fullDate !== null);

        transformed.sort((a, b) => new Date(a.fullDate) - new Date(b.fullDate));
        setConsumptionData(transformed);
      } else {
        setConsumptionData([]);
      }
    } catch (err) {
      console.error("Error fetching consumption:", err);
      setError(`Failed to load consumption data: ${err.message}`);
    } finally {
      setLoading((prev) => ({ ...prev, consumption: false }));
    }
  }, [selectedArea, selectedYear, selectedMonth]);

  const fetchPriceData = useCallback(async () => {
    setLoading((prev) => ({ ...prev, price: true }));
    try {
      const params = new URLSearchParams();
      params.append("limit", "10000");
      params.append("sortDesc", "true");
      params.append("dkarea", selectedArea);
      // Always include year parameter
      params.append("year", selectedYear || "2025");

      const url = `http://localhost:5001/historical-price?${params}`;
      console.log("Fetching price:", url);

      const response = await fetch(url);
      if (!response.ok)
        throw new Error(`HTTP error! status: ${response.status}`);

      const data = await response.json();
      console.log("Price data:", data);

      if (data && data.rows && data.rows.length > 0) {
        const transformed = data.rows
          .map((row) => {
            const timeValue = row["ts"] || row["historical_price.ts"];
            const yearValue =
              row["year"] || row["historical_price.year"] || "2025";

            // Extract month-day-time from ts (ignore the wrong year)
            // Format: "53970-12-15 08:00:000.0" -> extract "12-15 08:00"
            let parsedDate = null;
            if (timeValue) {
              const tsParts = timeValue.split("-");
              if (tsParts.length >= 3) {
                const monthDayTime = tsParts.slice(1).join("-");
                const correctedDateStr = `${yearValue}-${monthDayTime}`;
                parsedDate = new Date(correctedDateStr);
              }
            }

            return {
              timestamp:
                parsedDate && !isNaN(parsedDate.getTime())
                  ? parsedDate.toLocaleString("en-US", {
                      month: "short",
                      day: "numeric",
                    })
                  : "N/A",
              fullDate:
                parsedDate && !isNaN(parsedDate.getTime())
                  ? parsedDate.toISOString()
                  : null,
              price: parseFloat(
                row["price_eur_mwh"] ||
                  row["historical_price.price_eur_mwh"] ||
                  0,
              ),
              dkarea: row["dkarea"] || row["historical_price.dkarea"],
              year: yearValue,
            };
          })
          .filter((item) => item.fullDate !== null);

        transformed.sort((a, b) => new Date(a.fullDate) - new Date(b.fullDate));
        setPriceData(transformed);
      } else {
        setPriceData([]);
      }
    } catch (err) {
      console.error("Error fetching price:", err);
      setError(`Failed to load price data: ${err.message}`);
    } finally {
      setLoading((prev) => ({ ...prev, price: false }));
    }
  }, [selectedArea, selectedYear]);

  useEffect(() => {
    setError(null);
    if (activeTab === "production") {
      fetchProductionData();
    } else if (activeTab === "consumption") {
      fetchConsumptionData();
    } else if (activeTab === "price") {
      fetchPriceData();
    }
  }, [activeTab, fetchProductionData, fetchConsumptionData, fetchPriceData]);

  const handleRefresh = () => {
    setError(null);
    if (activeTab === "production") {
      fetchProductionData();
    } else if (activeTab === "consumption") {
      fetchConsumptionData();
    } else if (activeTab === "price") {
      fetchPriceData();
    }
  };

  const productionStats = {
    totalWind: productionData.reduce(
      (sum, item) => sum + item.windProduction,
      0,
    ),
    totalSun: productionData.reduce((sum, item) => sum + item.sunProduction, 0),
    totalProduction: productionData.reduce(
      (sum, item) => sum + item.totalProduction,
      0,
    ),
    dataPoints: productionData.length,
  };

  const consumptionStats = {
    totalConsumption: consumptionData.reduce(
      (sum, item) => sum + item.consumption,
      0,
    ),
    avgConsumption:
      consumptionData.length > 0
        ? consumptionData.reduce((sum, item) => sum + item.consumption, 0) /
          consumptionData.length
        : 0,
    maxConsumption:
      consumptionData.length > 0
        ? Math.max(...consumptionData.map((item) => item.consumption))
        : 0,
    dataPoints: consumptionData.length,
  };

  const priceStats = {
    avgPrice:
      priceData.length > 0
        ? priceData.reduce((sum, item) => sum + item.price, 0) /
          priceData.length
        : 0,
    maxPrice:
      priceData.length > 0
        ? Math.max(...priceData.map((item) => item.price))
        : 0,
    minPrice:
      priceData.length > 0
        ? Math.min(...priceData.map((item) => item.price))
        : 0,
    dataPoints: priceData.length,
  };

  const formatNumber = (num) => {
    if (num >= 1000000) return (num / 1000000).toFixed(2) + "M";
    if (num >= 1000) return (num / 1000).toFixed(2) + "K";
    return num.toFixed(2);
  };

  const isLoading = loading.production || loading.consumption || loading.price;

  return (
    <div className="historical-data-page">
      <div className="page-header">
        <h1>Historical Data Analysis</h1>
        <p>Energy production, consumption, and price data for Denmark</p>
      </div>

      <div className="tab-container">
        <button
          className={`tab-btn ${activeTab === "production" ? "active" : ""}`}
          onClick={() => setActiveTab("production")}
        >
          ⚡ Production
        </button>
        <button
          className={`tab-btn ${activeTab === "consumption" ? "active" : ""}`}
          onClick={() => setActiveTab("consumption")}
        >
          📊 Consumption
        </button>
        <button
          className={`tab-btn ${activeTab === "price" ? "active" : ""}`}
          onClick={() => setActiveTab("price")}
        >
          💰 Price
        </button>
      </div>

      <div className="controls-panel">
        <div className="control-row">
          <div className="filter-controls">
            <div className="filter-group">
              <label htmlFor="area-select">DK Area</label>
              <select
                id="area-select"
                className="filter-select"
                value={selectedArea}
                onChange={(e) => setSelectedArea(e.target.value)}
              >
                <option value="1">DK1 (West)</option>
                <option value="2">DK2 (East)</option>
              </select>
            </div>

            <div className="filter-group">
              <label htmlFor="year-select">Year</label>
              <select
                id="year-select"
                className="filter-select"
                value={selectedYear}
                onChange={(e) => setSelectedYear(e.target.value)}
              >
                <option value="">All Years</option>
                <option value="2025">2025</option>
                <option value="2024">2024</option>
                <option value="2023">2023</option>
                <option value="2022">2022</option>
                <option value="2021">2021</option>
              </select>
            </div>

            {(activeTab === "production" || activeTab === "consumption") && (
              <div className="filter-group">
                <label htmlFor="month-select">Month</label>
                <select
                  id="month-select"
                  className="filter-select"
                  value={selectedMonth}
                  onChange={(e) => setSelectedMonth(e.target.value)}
                >
                  <option value="">All Months</option>
                  <option value="1">January</option>
                  <option value="2">February</option>
                  <option value="3">March</option>
                  <option value="4">April</option>
                  <option value="5">May</option>
                  <option value="6">June</option>
                  <option value="7">July</option>
                  <option value="8">August</option>
                  <option value="9">September</option>
                  <option value="10">October</option>
                  <option value="11">November</option>
                  <option value="12">December</option>
                </select>
              </div>
            )}
          </div>

          <button
            className="btn btn-refresh"
            onClick={handleRefresh}
            disabled={isLoading}
          >
            {isLoading ? "Loading..." : "🔄 Refresh"}
          </button>
        </div>
      </div>

      {error && <div className="error">{error}</div>}

      {isLoading && (
        <div className="loading">
          <div className="spinner"></div>
          Loading {activeTab} data...
        </div>
      )}

      {/* Production Tab */}
      {activeTab === "production" &&
        !loading.production &&
        productionData.length > 0 && (
          <>
            <div className="stats-grid">
              <div className="stat-card wind">
                <div className="stat-icon">💨</div>
                <div className="stat-content">
                  <label>Wind Production</label>
                  <value>{formatNumber(productionStats.totalWind)}</value>
                  <unit>kWh</unit>
                </div>
              </div>
              <div className="stat-card sun">
                <div className="stat-icon">☀️</div>
                <div className="stat-content">
                  <label>Solar Production</label>
                  <value>{formatNumber(productionStats.totalSun)}</value>
                  <unit>kWh</unit>
                </div>
              </div>
              <div className="stat-card total">
                <div className="stat-icon">⚡</div>
                <div className="stat-content">
                  <label>Total Production</label>
                  <value>{formatNumber(productionStats.totalProduction)}</value>
                  <unit>kWh</unit>
                </div>
              </div>
              <div className="stat-card records">
                <div className="stat-icon">📈</div>
                <div className="stat-content">
                  <label>Data Points</label>
                  <value>{productionStats.dataPoints}</value>
                  <unit>Records</unit>
                </div>
              </div>
            </div>

            <div className="chart-container">
              <div className="chart-header">
                <h2>Energy Production Over Time</h2>
                <p>Wind and Solar energy production in DK{selectedArea}</p>
              </div>
              <ResponsiveContainer width="100%" height={450}>
                <AreaChart
                  data={productionData}
                  margin={{ top: 20, right: 30, left: 20, bottom: 60 }}
                >
                  <defs>
                    <linearGradient
                      id="windGradient"
                      x1="0"
                      y1="0"
                      x2="0"
                      y2="1"
                    >
                      <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.8} />
                      <stop
                        offset="95%"
                        stopColor="#3b82f6"
                        stopOpacity={0.1}
                      />
                    </linearGradient>
                    <linearGradient
                      id="sunGradient"
                      x1="0"
                      y1="0"
                      x2="0"
                      y2="1"
                    >
                      <stop offset="5%" stopColor="#f59e0b" stopOpacity={0.8} />
                      <stop
                        offset="95%"
                        stopColor="#f59e0b"
                        stopOpacity={0.1}
                      />
                    </linearGradient>
                  </defs>
                  <CartesianGrid
                    strokeDasharray="3 3"
                    stroke="rgba(255,255,255,0.1)"
                  />
                  <XAxis
                    dataKey="timestamp"
                    tick={{ fill: "#94a3b8", fontSize: 11 }}
                    angle={-45}
                    textAnchor="end"
                    height={80}
                  />
                  <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: "rgba(15, 23, 42, 0.95)",
                      border: "1px solid rgba(59, 130, 246, 0.5)",
                      borderRadius: "12px",
                      padding: "12px",
                    }}
                    labelStyle={{ color: "#e2e8f0", fontWeight: 600 }}
                    formatter={(value) => [formatNumber(value) + " kWh", ""]}
                  />
                  <Legend wrapperStyle={{ paddingTop: "20px" }} />
                  <Area
                    type="monotone"
                    dataKey="windProduction"
                    stroke="#3b82f6"
                    fill="url(#windGradient)"
                    name="Wind Production"
                    strokeWidth={2}
                  />
                  <Area
                    type="monotone"
                    dataKey="sunProduction"
                    stroke="#f59e0b"
                    fill="url(#sunGradient)"
                    name="Solar Production"
                    strokeWidth={2}
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </>
        )}

      {/* Consumption Tab */}
      {activeTab === "consumption" &&
        !loading.consumption &&
        consumptionData.length > 0 && (
          <>
            <div className="stats-grid">
              <div className="stat-card consumption">
                <div className="stat-icon">🔌</div>
                <div className="stat-content">
                  <label>Total Consumption</label>
                  <value>
                    {formatNumber(consumptionStats.totalConsumption)}
                  </value>
                  <unit>kWh</unit>
                </div>
              </div>
              <div className="stat-card average">
                <div className="stat-icon">📊</div>
                <div className="stat-content">
                  <label>Avg Consumption</label>
                  <value>{formatNumber(consumptionStats.avgConsumption)}</value>
                  <unit>kWh</unit>
                </div>
              </div>
              <div className="stat-card peak">
                <div className="stat-icon">📈</div>
                <div className="stat-content">
                  <label>Peak Consumption</label>
                  <value>{formatNumber(consumptionStats.maxConsumption)}</value>
                  <unit>kWh</unit>
                </div>
              </div>
              <div className="stat-card records">
                <div className="stat-icon">📋</div>
                <div className="stat-content">
                  <label>Data Points</label>
                  <value>{consumptionStats.dataPoints}</value>
                  <unit>Records</unit>
                </div>
              </div>
            </div>

            <div className="chart-container">
              <div className="chart-header">
                <h2>Energy Consumption Over Time</h2>
                <p>Aggregated consumption data for DK{selectedArea}</p>
              </div>
              <ResponsiveContainer width="100%" height={450}>
                <LineChart
                  data={consumptionData}
                  margin={{ top: 20, right: 30, left: 20, bottom: 60 }}
                >
                  <CartesianGrid
                    strokeDasharray="3 3"
                    stroke="rgba(255,255,255,0.1)"
                  />
                  <XAxis
                    dataKey="timestamp"
                    tick={{ fill: "#94a3b8", fontSize: 11 }}
                    angle={-45}
                    textAnchor="end"
                    height={80}
                  />
                  <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: "rgba(15, 23, 42, 0.95)",
                      border: "1px solid rgba(139, 92, 246, 0.5)",
                      borderRadius: "12px",
                      padding: "12px",
                    }}
                    labelStyle={{ color: "#e2e8f0", fontWeight: 600 }}
                    formatter={(value) => [
                      formatNumber(value) + " kWh",
                      "Consumption",
                    ]}
                  />
                  <Legend wrapperStyle={{ paddingTop: "20px" }} />
                  <Line
                    type="monotone"
                    dataKey="consumption"
                    stroke="#8b5cf6"
                    strokeWidth={2.5}
                    dot={{ r: 2, fill: "#8b5cf6" }}
                    activeDot={{ r: 6, fill: "#a78bfa" }}
                    name="Consumption"
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </>
        )}

      {/* Price Tab */}
      {activeTab === "price" && !loading.price && priceData.length > 0 && (
        <>
          <div className="stats-grid">
            <div className="stat-card price-avg">
              <div className="stat-icon">💶</div>
              <div className="stat-content">
                <label>Average Price</label>
                <value>{priceStats.avgPrice.toFixed(2)}</value>
                <unit>EUR/MWh</unit>
              </div>
            </div>
            <div className="stat-card price-max">
              <div className="stat-icon">📈</div>
              <div className="stat-content">
                <label>Maximum Price</label>
                <value>{priceStats.maxPrice.toFixed(2)}</value>
                <unit>EUR/MWh</unit>
              </div>
            </div>
            <div className="stat-card price-min">
              <div className="stat-icon">📉</div>
              <div className="stat-content">
                <label>Minimum Price</label>
                <value>{priceStats.minPrice.toFixed(2)}</value>
                <unit>EUR/MWh</unit>
              </div>
            </div>
            <div className="stat-card records">
              <div className="stat-icon">📋</div>
              <div className="stat-content">
                <label>Data Points</label>
                <value>{priceStats.dataPoints}</value>
                <unit>Records</unit>
              </div>
            </div>
          </div>

          <div className="chart-container">
            <div className="chart-header">
              <h2>Energy Price Over Time</h2>
              <p>Historical electricity prices for DK{selectedArea}</p>
            </div>
            <ResponsiveContainer width="100%" height={450}>
              <AreaChart
                data={priceData}
                margin={{ top: 20, right: 30, left: 20, bottom: 60 }}
              >
                <defs>
                  <linearGradient
                    id="priceGradient"
                    x1="0"
                    y1="0"
                    x2="0"
                    y2="1"
                  >
                    <stop offset="5%" stopColor="#10b981" stopOpacity={0.8} />
                    <stop offset="95%" stopColor="#10b981" stopOpacity={0.1} />
                  </linearGradient>
                </defs>
                <CartesianGrid
                  strokeDasharray="3 3"
                  stroke="rgba(255,255,255,0.1)"
                />
                <XAxis
                  dataKey="timestamp"
                  tick={{ fill: "#94a3b8", fontSize: 11 }}
                  angle={-45}
                  textAnchor="end"
                  height={80}
                />
                <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} />
                <Tooltip
                  contentStyle={{
                    backgroundColor: "rgba(15, 23, 42, 0.95)",
                    border: "1px solid rgba(16, 185, 129, 0.5)",
                    borderRadius: "12px",
                    padding: "12px",
                  }}
                  labelStyle={{ color: "#e2e8f0", fontWeight: 600 }}
                  formatter={(value) => [
                    value.toFixed(2) + " EUR/MWh",
                    "Price",
                  ]}
                />
                <Legend wrapperStyle={{ paddingTop: "20px" }} />
                <Area
                  type="monotone"
                  dataKey="price"
                  stroke="#10b981"
                  fill="url(#priceGradient)"
                  name="Price (EUR/MWh)"
                  strokeWidth={2}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </>
      )}

      {/* Empty States */}
      {activeTab === "production" &&
        !loading.production &&
        productionData.length === 0 &&
        !error && (
          <div className="empty-state">
            <div className="empty-icon">📭</div>
            <p>No production data available for the selected filters.</p>
            <button className="btn btn-refresh" onClick={handleRefresh}>
              Try Again
            </button>
          </div>
        )}

      {activeTab === "consumption" &&
        !loading.consumption &&
        consumptionData.length === 0 &&
        !error && (
          <div className="empty-state">
            <div className="empty-icon">📭</div>
            <p>No consumption data available for the selected filters.</p>
            <button className="btn btn-refresh" onClick={handleRefresh}>
              Try Again
            </button>
          </div>
        )}

      {activeTab === "price" &&
        !loading.price &&
        priceData.length === 0 &&
        !error && (
          <div className="empty-state">
            <div className="empty-icon">📭</div>
            <p>No price data available for the selected filters.</p>
            <button className="btn btn-refresh" onClick={handleRefresh}>
              Try Again
            </button>
          </div>
        )}
    </div>
  );
}

export default HistoricalData;
