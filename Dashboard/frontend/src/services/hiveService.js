import api from "./api";

export const hiveService = {
  getTables: async () => {
    const response = await api.get("/api/hive/tables");
    return { data: response.data };
  },

  queryTable: async (tableName, limit = 100) => {
    let endpoint;
    if (tableName === "predictions") {
      endpoint = "/api/hive/predictions";
    } else if (tableName === "consumption_data") {
      endpoint = "/api/hive/consumption";
    } else if (tableName === "weather_data") {
      endpoint = "/api/hive/weather";
    } else {
      endpoint = `/api/hive/table/${tableName}`;
    }
    
    const response = await api.get(endpoint, { params: { limit } });
    return { data: response.data };
  },

  invalidateCache: (tableName) =>
    api.post(
      "/api/hive/cache/invalidate",
      {},
      { params: { table: tableName } }
    ),
};

export default hiveService;
