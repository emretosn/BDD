FROM python:3.8

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

ENV KAFKA_SERVER=kafka:9093
ENV SPARK_MASTER=spark://spark:4040

EXPOSE 9092 9093 4040

CMD ["python", "orchestrator.py"]
