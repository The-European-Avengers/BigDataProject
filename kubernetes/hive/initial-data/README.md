# DMI & Heating Data Collection

This Docker container collects historical weather and heating data from Danish APIs.

## Scripts Included

1. **dmi_wind_collection.py** - Collects wind speed data from 57 Danish weather stations (2020-2024)
2. **dmi_radiation_collection.py** - Collects solar radiation data from Danish stations (2020-current)
3. **Combined_Heating_Data_2022-2025.py** - Collects private heating consumption data (2022-2025)

## Usage

### Build the Docker image:
```bash
docker build -t dmi-data-collector .
```

### Run the container:
```bash
docker run -v $(pwd)/data:/app/data dmi-data-collector
```

This will:
- Run all three collection scripts sequentially
- Show real-time progress for each script
- Save all CSV files to the `./data` directory
- Display a summary when complete

### Expected Output Files

The container will generate CSV files in the `data/` directory:
- `2020_dmi_wind.csv` through `2024_dmi_wind.csv`
- `2020_dmi_radiation.csv` through `2025_dmi_radiation.csv`
- `private_heating_consumption_2022-2025_combined.csv`

## Monitoring

The orchestrator script provides:
- Clear section headers for each script
- Real-time output from data collection
- Duration tracking for each script
- Final summary with success/failure status

## Notes

- The scripts run **sequentially** to avoid overwhelming the APIs
- Total runtime: ~30-60 minutes depending on API response times
- All scripts handle API rate limiting with built-in delays
- Data is clean with no null values
