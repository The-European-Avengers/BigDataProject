FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
COPY dk.* .
RUN pip install --no-cache-dir -r requirements.txt
COPY producer-historical-kafka.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "producer-historical-kafka.py"]