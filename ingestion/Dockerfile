FROM apache/spark:3.5.1

USER root
WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY producer.py stream_processor.py entrypoint.sh .
RUN chmod +x entrypoint.sh

CMD ["./entrypoint.sh"]
