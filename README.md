# Real Time Prediction with Spark

This project demonstrates an end-to-end pipeline for data preprocessing, model training, and real-time predictions using Apache Spark and Apache Kafka.

## Dataset

This project uses the **Diabetes Binary Health Indicators BRFSS2015** dataset, which includes various health indicators such as HighBP, HighChol, BMI, and more.

## Prerequisites

- Docker and Docker Compose
- Apache Kafka and Zookeeper (managed via Docker Compose)
- Apache Spark
- Python 3.x

## Setup

1. Start Zookeeper and Kafka using Docker Compose:
   ```bash
   docker-compose up -d
   ```

	2.	Train the model with the offline Spark application. The best model will be saved for the online (streaming) phase.
	3.	Run the online Spark streaming application to process incoming data and publish predictions.

Kafka Consumers

To verify the data streams, use these commands:
	•	Consume health indicators predictions:

`kafka-console-consumer --bootstrap-server localhost:9092 --topic health_indicators_predicted --from-beginning`

•	Consume health indicators data:

`kafka-console-consumer --bootstrap-server localhost:9092 --topic health_indicators --from-beginning`


## Project Structure
	•	docker-compose.yml: Sets up Zookeeper and Kafka.
	•	Offline Phase: Spark application for data preprocessing, training, and model selection.
	•	Online Phase: Spark Structured Streaming job for real-time predictions.
	•	Kafka Scripts: Producer and consumer scripts for streaming data.

Notes

Ensure your environment is properly configured and that all necessary dependencies are installed before running the applications. Additionally because of the size of the venv folder, you have to start a venv environment and install all packages in the notebooks.

