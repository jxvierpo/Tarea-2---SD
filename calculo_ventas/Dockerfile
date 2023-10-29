# Use an official Python runtime as the parent image
FROM python:3.8-slim

# Set the working directory in the container to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org confluent-kafka

# Make port 3000 available to the world outside this container
EXPOSE 3000

# Define environment variable for Kafka broker
ENV KAFKA_BROKER=kafka:9092

# Run calculo_ventas.py when the container launches
CMD ["python", "calculo_ventas.py"]
