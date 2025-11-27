FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
COPY dk.* .
RUN pip install --no-cache-dir -r requirements.txt
COPY dmi_wind_collection_short.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "dmi_wind_collection_short.py"]
