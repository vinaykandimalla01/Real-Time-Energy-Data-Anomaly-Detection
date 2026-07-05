FROM python:3.9-slim-bullseye

# Set the working directory
WORKDIR /app

# Install required Python library
RUN pip install --no-cache-dir confluent-kafka

# Copy the producer script
COPY kafka_producer.py .

# Run the producer
CMD ["python", "kafka_producer.py"]