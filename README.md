# Kafka_streaming
Project Overview

This project sets up a Kafka-based streaming data pipeline using Docker, Kafka, and Python. The pipeline:

Ingests real-time login data via a Kafka topic (user-login).

Filters messages to only process Android logins.

Publishes processed data to another Kafka topic (processed-logins).

