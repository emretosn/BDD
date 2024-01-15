from subprocess import run

# Run producer
run(["python", "producer.py"])

# Run consumer
run(["python", "consumer.py"])