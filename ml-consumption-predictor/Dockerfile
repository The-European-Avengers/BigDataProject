FROM apache/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /opt/spark/work-dir

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

ENV PYTHONUNBUFFERED=1
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=/opt/spark/work-dir:$PYTHONPATH

RUN cd /opt/spark/jars && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.0/spark-avro_2.12-3.5.0.jar

USER 185

ENTRYPOINT ["/opt/entrypoint.sh"]
CMD ["python", "-m", "src.main"]
