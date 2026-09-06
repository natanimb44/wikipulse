FROM apache/spark:3.5.1

USER root
WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY stream_processor.py .

CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.kafka:kafka-clients:3.5.1", \
     "--conf", "spark.sql.shuffle.partitions=4", \
     "/app/stream_processor.py"]
