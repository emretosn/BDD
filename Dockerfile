FROM python:3.8-slim

RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

RUN apt-get update && \
    apt-get install -y wget && \
    wget https://downloads.apache.org/kafka/2.8.0/kafka_2.13-2.8.0.tgz && \
    tar -xzf kafka_2.13-2.8.0.tgz && \
    mv kafka_2.13-2.8.0 /kafka && \
    rm kafka_2.13-2.8.0.tgz

ENV KAFKA_HOME=/kafka
ENV PATH="${KAFKA_HOME}/bin:${PATH}"

COPY requirements.txt /
RUN pip install -r /requirements.txt

COPY . /app
WORKDIR /app

EXPOSE 9092 2181 8080

CMD ["python", "producer.py"]
