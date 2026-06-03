import React, { useState, useEffect } from "react";
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
import "./PredictionsTimeline.css";

function PredictionsTimeline() {
  const [chartData, setChartData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedMetric, setSelectedMetric] = useState("consumption");
  const [selectedDkArea, setSelectedDkArea] = useState("");
  const [dataRanges, setDataRanges] = useState({
    consumption: { min: 0, max: 0 },
    production: { min: 0, max: 0 },
    price: { min: 0, max: 0 },
  });
  const [precisionData, setPrecisionData] = useState(null);
  const [precisionLoading, setPrecisionLoading] = useState(false);

  const metricConfig = {
    consumption: {
      title: "Energy Consumption",
      label: "Consumption (kWh)",
      key: "consumptionkwh",
      color: "#8884d8",
      yAxis: "left",
    },
    price: {
      title: "Energy Price",
      label: "Price (DKK/MWh)",
      key: "price",
      color: "#ffc658",
      yAxis: "right",
    },
    production: {
      title: "Energy Production",
      label: "Production (kWh)",
      key: "productionkwh",
      color: "#82ca9d",
      yAxis: "left",
    },
  };

  useEffect(() => {
    console.log("Filters changed, fetching predictions...", {
      selectedDkArea,
    });
    fetchPredictions();
    fetchPrecisionData();
  }, [selectedDkArea]);

  const fetchPrecisionData = async () => {
    try {
      setPrecisionLoading(true);
      const url = `http://localhost:5001/predictions-precision`;
      console.log(`Fetching precision data from: ${url}`);

      const response = await fetch(url);
      console.log(`Precision response status: ${response.status}`);
      
      if (!response.ok) {
        const errorText = await response.text();
        console.error(`Precision API error: ${errorText}`);
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      console.log("Precision data received:", data);
      console.log("Overall average:", data.overallAverage);
      setPrecisionData(data);
    } catch (err) {
      console.error("Error fetching precision data:", err);
      // Set empty object with error flag so section still renders with "no data" message
      setPrecisionData({ error: true, overallAverage: { totalRecords: 0 } });
    } finally {
      setPrecisionLoading(false);
    }
  };

  const fetchPredictions = async () => {
    try {
      setLoading(true);
      const params = new URLSearchParams();
      params.append("limit", "10000"); // Fetch all ~4000 records

      if (selectedDkArea) {
        params.append("dkarea", selectedDkArea);
      }

      // Call hive-api directly on port 5001
      const url = `http://localhost:5001/predictions?${params}`;
      console.log(`Fetching from: ${url}`);

      const response = await fetch(url);
      console.log(`Response status: ${response.status}`);

      if (!response.ok) {
        const errorText = await response.text();
        console.error(
          `HTTP error! status: ${response.status}, body: ${errorText.substring(
            0,
            200
          )}`
        );
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      console.log("Predictions data received:", data);

      if (data && data.rows && data.rows.length > 0) {
        // Transform data for chart - rows are objects with "predictions." prefix
        const transformed = data.rows
          .map((row) => {
            // Parse timestamp - handle both string and date formats
            const timestampValue = row["predictions.timestamp"];
            let parsedDate;

            if (timestampValue) {
              // Try parsing the timestamp
              parsedDate = new Date(timestampValue);

              // If invalid date, log it
              if (isNaN(parsedDate.getTime())) {
                console.warn("Invalid timestamp:", timestampValue);
                parsedDate = null;
              }
            }

            const price = row["predictions.price"] 
              ? parseFloat(row["predictions.price"]) 
              : null;
            const consumption = row["predictions.consumptionkwh"]
              ? parseFloat(row["predictions.consumptionkwh"])
              : null;
            const production = row["predictions.productionkwh"]
              ? parseFloat(row["predictions.productionkwh"])
              : null;

            return {
              timestamp: parsedDate
                ? parsedDate.toLocaleString("en-US", {
                    month: "short",
                    day: "numeric",
                    hour: "2-digit",
                    minute: "2-digit",
                    hour12: true,
                  })
                : "Invalid Date",
              fullDate: parsedDate ? parsedDate.toISOString() : null,
              price: price && price > 0 ? price : null,
              consumptionkwh: consumption && consumption > 0 ? consumption : null,
              productionkwh: production && production > 0 ? production : null,
              dkarea: row["predictions.dkarea"],
            };
          })
          .filter((item) => item.fullDate !== null); // Filter out invalid dates

        // Aggregate entries by minute (truncate seconds):
        // - Sum consumption and production
        // - Average price
        const aggregatedMap = new Map();

        transformed.forEach((item) => {
          // Truncate to minute for cleaner aggregation (ignore seconds and milliseconds)
          const date = new Date(item.fullDate);
          date.setSeconds(0, 0);
          date.setMilliseconds(0);
          const minuteKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}_${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}_${item.dkarea}`;
          const minuteTimestamp = date.toLocaleString("en-GB", {
            month: "short",
            day: "numeric",
            hour: "2-digit",
            minute: "2-digit",
            hour12: false,
          });

          if (aggregatedMap.has(minuteKey)) {
            const existing = aggregatedMap.get(minuteKey);
            existing.consumptionkwh = (existing.consumptionkwh || 0) + (item.consumptionkwh || 0);
            existing.productionkwh = (existing.productionkwh || 0) + (item.productionkwh || 0);
            existing.priceSum += (item.price || 0);
            existing.priceCount += item.price ? 1 : 0;
          } else {
            aggregatedMap.set(minuteKey, {
              timestamp: minuteTimestamp,
              fullDate: date.toISOString(),
              consumptionkwh: item.consumptionkwh || 0,
              productionkwh: item.productionkwh || 0,
              priceSum: item.price || 0,
              priceCount: item.price ? 1 : 0,
              dkarea: item.dkarea,
            });
          }
        });

        // Calculate average price and convert back to array
        const aggregated = Array.from(aggregatedMap.values()).map((item) => ({
          timestamp: item.timestamp,
          fullDate: item.fullDate,
          price: item.priceCount > 0 ? item.priceSum / item.priceCount : null,
          consumptionkwh: item.consumptionkwh > 0 ? item.consumptionkwh : null,
          productionkwh: item.productionkwh > 0 ? item.productionkwh : null,
          dkarea: item.dkarea,
        }));

        // Sort by timestamp
        aggregated.sort(
          (a, b) =>
            new Date(a.fullDate).getTime() - new Date(b.fullDate).getTime()
        );

        console.log("Aggregated data:", aggregated.slice(0, 5));

        setChartData(aggregated);
        setError(null);
      } else {
        setChartData([]);
        setError("No data available for selected filters");
      }
    } catch (err) {
      console.error("Error fetching predictions:", err);
      setError(
        `Failed to load predictions data: ${
          err instanceof Error ? err.message : String(err)
        }`
      );
      setChartData([]);
    } finally {
      setLoading(false);
    }
  };

  // Calculate data ranges when chartData changes
  useEffect(() => {
    if (chartData.length > 0) {
      const consumptionValues = chartData.map(d => d.consumptionkwh).filter(v => v !== null && v > 0);
      const productionValues = chartData.map(d => d.productionkwh).filter(v => v !== null && v > 0);
      const priceValues = chartData.map(d => d.price).filter(v => v !== null && v > 0);

      setDataRanges({
        consumption: {
          min: consumptionValues.length > 0 ? Math.min(...consumptionValues) * 0.9 : 0.01,
          max: consumptionValues.length > 0 ? Math.max(...consumptionValues) * 1.1 : 100,
        },
        production: {
          min: productionValues.length > 0 ? Math.min(...productionValues) * 0.9 : 0.01,
          max: productionValues.length > 0 ? Math.max(...productionValues) * 1.1 : 100,
        },
        price: {
          min: priceValues.length > 0 ? Math.min(...priceValues) * 0.9 : 0.01,
          max: priceValues.length > 0 ? Math.max(...priceValues) * 1.1 : 100,
        },
      });
    }
  }, [chartData]);

  // Get date range string for display
  const getDateRangeString = () => {
    if (chartData.length === 0) return '';
    const sortedDates = chartData
      .map(d => new Date(d.fullDate))
      .filter(d => !isNaN(d.getTime()))
      .sort((a, b) => a - b);
    if (sortedDates.length === 0) return '';
    const startDate = sortedDates[0].toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    const endDate = sortedDates[sortedDates.length - 1].toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    return `${startDate} - ${endDate}`;
  };

  return (
    <div className="predictions-timeline-page">
      <div className="page-header">
        <h1>Day Ahead Price Predictions</h1>
        <p>Energy price and consumption forecasts</p>
      </div>

      <div className="controls-panel">
        <div className="control-row">
          <div className="filter-controls-inline">
            <label htmlFor="metricSelect" style={{ marginRight: '8px', fontWeight: '500' }}>Metric:</label>
            <select
              id="metricSelect"
              value={selectedMetric}
              onChange={(e) => setSelectedMetric(e.target.value)}
              className="filter-select-small"
              title="Select Metric"
            >
              <option value="consumption">Consumption (kWh)</option>
              <option value="production">Production (kWh)</option>
              <option value="price">Price (DKK/MWh)</option>
            </select>

            <label htmlFor="dkArea" style={{ marginLeft: '20px', marginRight: '8px', fontWeight: '500' }}>Area:</label>
            <select
              id="dkArea"
              value={selectedDkArea}
              onChange={(e) => setSelectedDkArea(e.target.value)}
              className="filter-select-small"
              title="DK Area"
            >
              <option value="">All Areas</option>
              <option value="1">DK1</option>
              <option value="2">DK2</option>
            </select>
            <button
              className="btn btn-refresh"
              onClick={fetchPredictions}
              disabled={loading}
            >
              {loading ? "Loading..." : "Refresh"}
            </button>
          </div>
        </div>
      </div>

      {loading && <div className="loading">Loading predictions...</div>}
      {error && <div className="error">{error}</div>}

      {/* Chart Section */}
      {chartData.length > 0 && (
        <div className="chart-section-full">
          <div className="large-chart-container">
            <h2>{metricConfig[selectedMetric].title}</h2>
            <p style={{ color: '#94a3b8', fontSize: '12px', marginBottom: '6px' }}>
              Date Range: {getDateRangeString()}
            </p>
            <ResponsiveContainer width="100%" height={400}>
              <LineChart
                data={chartData}
                margin={{ top: 10, right: 40, left: 50, bottom: 50 }}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
                <XAxis
                  dataKey="timestamp"
                  angle={-45}
                  textAnchor="end"
                  height={100}
                  tick={{ fontSize: 12 }}
                  interval="preserveStartEnd"
                />
                <YAxis
                  scale="log"
                  domain={[
                    dataRanges[selectedMetric].min > 0 ? dataRanges[selectedMetric].min : 0.01,
                    dataRanges[selectedMetric].max > 0 ? dataRanges[selectedMetric].max : 100
                  ]}
                  allowDataOverflow
                  tick={{ fontSize: 13 }}
                  tickFormatter={(value) => value >= 1000 ? `${(value / 1000).toFixed(1)}k` : value.toFixed(1)}
                />
                <Tooltip
                  formatter={(value) =>
                    value !== null && value !== undefined ? value.toFixed(2) : "-"
                  }
                  labelFormatter={(label) => `Time: ${label}`}
                  contentStyle={{
                    backgroundColor: "rgba(255, 255, 255, 0.95)",
                    border: "2px solid #ccc",
                    borderRadius: "6px",
                    padding: "10px",
                    fontSize: "13px",
                  }}
                />
                <Legend wrapperStyle={{ fontSize: "13px", paddingTop: "20px" }} />
                <Line
                  type="monotone"
                  dataKey={metricConfig[selectedMetric].key}
                  stroke={metricConfig[selectedMetric].color}
                  dot={false}
                  activeDot={{ r: 6 }}
                  name={metricConfig[selectedMetric].label}
                  isAnimationActive={true}
                  strokeWidth={2}
                  connectNulls={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* MODEL ACCURACY SECTION - Always visible when data exists */}
      {!loading && (chartData.length > 0 || precisionData) && (
        <div style={{
          margin: '20px 16px 30px 16px',
          padding: '24px 40px',
          background: 'linear-gradient(135deg, #1e293b 0%, #0f172a 100%)',
          borderRadius: '12px',
          border: '3px solid #10b981',
          boxShadow: '0 4px 20px rgba(16, 185, 129, 0.3)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          gap: '80px'
        }}>
          {precisionLoading ? (
            <p style={{ color: '#94a3b8', fontSize: '16px', margin: 0 }}>Loading precision data...</p>
          ) : precisionData && precisionData.overallAverage && precisionData.overallAverage.totalRecords > 0 ? (
            <>
              <div style={{
                display: 'flex',
                alignItems: 'center',
                gap: '20px'
              }}>
                <span style={{ fontSize: '40px' }}>⚡</span>
                <div>
                  <div style={{
                    color: '#94a3b8',
                    fontSize: '14px',
                    fontWeight: '600',
                    marginBottom: '6px',
                    textTransform: 'uppercase',
                    letterSpacing: '1px'
                  }}>
                    Consumption Accuracy
                  </div>
                  <div style={{
                    color: '#a78bfa',
                    fontSize: '42px',
                    fontWeight: '900',
                    lineHeight: '1'
                  }}>
                    {(precisionData.overallAverage.avgConsumptionPrecision * 100).toFixed(1)}%
                  </div>
                </div>
              </div>
              
              <div style={{
                width: '3px',
                height: '70px',
                background: 'linear-gradient(to bottom, transparent, #10b981, transparent)'
              }}></div>
              
              <div style={{
                display: 'flex',
                alignItems: 'center',
                gap: '20px'
              }}>
                <span style={{ fontSize: '40px' }}>💰</span>
                <div>
                  <div style={{
                    color: '#94a3b8',
                    fontSize: '14px',
                    fontWeight: '600',
                    marginBottom: '6px',
                    textTransform: 'uppercase',
                    letterSpacing: '1px'
                  }}>
                    Price Accuracy
                  </div>
                  <div style={{
                    color: '#fbbf24',
                    fontSize: '42px',
                    fontWeight: '900',
                    lineHeight: '1'
                  }}>
                    {(precisionData.overallAverage.avgPricePrecision * 100).toFixed(1)}%
                  </div>
                </div>
              </div>
            </>
          ) : (
            <p style={{ color: '#fca5a5', fontSize: '16px', margin: 0 }}>
              ⚠️ No precision data available
            </p>
          )}
        </div>
      )}

      {chartData.length === 0 && !loading && !error && (
        <div className="no-data">
          Click Refresh to load predictions data.
        </div>
      )}
    </div>
  );
}

export default PredictionsTimeline;
