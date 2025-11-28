FROM python:3.11-slim
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all data collection scripts
COPY dmi_wind_collection.py .
COPY dmi_sun_collection.py .
COPY Combined_Heating_Data_2022-2025.py .
COPY run_all_collections.py .

# Copy any additional resources (shapefile data if needed)
COPY dk.* .

# Ensure output is not buffered for better logging
ENV PYTHONUNBUFFERED=1

# Run the orchestrator script that executes all three collections
CMD ["python", "run_all_collections.py"]
