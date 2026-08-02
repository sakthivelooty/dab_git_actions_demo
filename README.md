# Wiki Data Analysis

This project builds a simple but practical streaming data pipeline for analyzing Wikimedia edit activity. It collects live event data from the Wikimedia recent changes stream, sends it to Kafka, processes it with Spark in Databricks, enriches the data with Wikidata information, and creates aggregate tables for reporting and dashboarding.

## Project goal

The main purpose of this project is to answer questions such as:

- Which wikis are generating the most activity?
- How often do edits happen over time?
- Are bot accounts contributing a large share of events?
- Can we enrich each event with human-readable entity information from Wikidata?

## High-level architecture

The pipeline follows a simple flow:

1. A Python producer reads the Wikimedia event stream.
2. Each event is published to a Kafka topic.
3. Spark Structured Streaming reads the Kafka topic.
4. The data is parsed and enriched with Wikidata metadata.
5. Aggregated results are stored in a gold table for reporting.

A simplified view of the flow looks like this:

```text
Wikimedia Stream -> Kafka -> Spark/Databricks -> Enriched Silver Table -> Aggregated Gold Table
```

## Data source

The source data comes from the Wikimedia recent changes stream.

- Source platform: Wikimedia
- Stream type: Server-Sent Events (SSE)
- Data format: JSON
- Typical event examples: page edits, content changes, metadata about the edit, and user information

Each incoming event contains important fields such as:

- wiki
- type
- timestamp
- title
- title_url
- user
- bot
- namespace
- revision details

## Components in the project

### 1. Kafka producer

### NOTE: 
- For windows version please use the file wikimedia_kafka_producer_windows_version.py
- For Mac or Linux version please use the file wikimedia_kafka_producer_mac_version.py


Location: [src/kafka_producer/wikimedia_kafka_producer.py](src/kafka_producer/wikimedia_kafka_producer.py)

This component is responsible for reading the live Wikimedia stream and sending the events to Kafka.

Main responsibilities:

- Read Kafka connection settings from [src/kafka_producer/client.properties](src/kafka_producer/client.properties)
- Connect to the Wikimedia event stream
- Stream events continuously
- Publish each event to the Kafka topic

Key functions:

- read_config(): loads Kafka connection settings from the properties file
- WikimediaKafkaProducer.on_message(): sends each event payload to Kafka
- WikimediaKafkaProducer.start_streaming(): reads the SSE stream and forwards events
- WikimediaKafkaProducer.__init__(): initializes the Kafka producer object

### 2. Spark ingestion and enrichment

Location: [src/spark_ingestion/spark_kafka_consumer.py](src/spark_ingestion/spark_kafka_consumer.py)

This is the core processing layer. It reads events from Kafka, parses the JSON payload, extracts useful information, enriches the data with Wikidata metadata, and writes the results into a structured table.

Main responsibilities:

- Read the Kafka topic using Spark Structured Streaming
- Parse JSON payloads into structured columns
- Extract QIDs from the page title URL
- Call the Wikidata API to fetch labels and descriptions
- Write the enriched results to a silver table

Key functions:

- fetch_qid_data(): calls the Wikidata API for a batch of QIDs and returns labels and descriptions
- enrich_data(): fills missing metadata using a local cache for repeated QIDs
- process_and_enrich_data(): processes each micro-batch, joins metadata, and writes the result

### 3. Gold aggregation layer

Location: [src/spark_ingestion/wikimedia_gold_agg.py](src/spark_ingestion/wikimedia_gold_agg.py)

This layer transforms the enriched data into reporting-friendly aggregate metrics.

Main responsibilities:

- Convert the raw event timestamp into a proper timestamp column
- Apply watermarking for late-arriving data
- Group events by time window, wiki, bot status, and event type
- Create a count-based summary table for dashboards

## Tables used in the project

### 1. Kafka topic: wikimedia_analisys

This is the raw streaming input topic.

- Purpose: temporary storage of incoming Wikimedia events before further processing
- Source: Python producer
- Format: JSON strings

### 2. Silver table: spark_kafka_projects.wikidata.wikimedia_data

This is the cleaned and enriched table produced after Spark processing.

It contains:

- raw event fields from the Wikimedia payload
- parsed fields such as message key and JSON payload structure
- extracted QID values
- Wikidata label and description for each QID

This layer is often considered the “business-ready” version of the raw data.

### 3. Gold table: spark_kafka_projects.wikidata.dashboard_aggregations

This is the reporting table produced by the aggregation step.

It contains:

- window_start_timestamp
- window_end_timestamp
- wiki
- is_bot
- type
- event_count

This table is well suited for dashboarding and trend analysis.

## Types of functions used

The project uses a mix of simple Python functions and Spark-based transformation logic.

### Python functions

These are used in the producer and enrichment workflow:

- Utility functions: read_config()
- Event processing functions: on_message()
- Streaming functions: start_streaming()
- Data enrichment functions: fetch_qid_data()
- Batch processing functions: process_and_enrich_data()

### Spark transformation logic

These are used in the Databricks processing layer:

- Column transformation functions such as withColumn(), select(), and cast()
- String extraction functions such as regexp_extract()
- JSON parsing functions such as from_json()
- Aggregation functions such as groupBy(), count(), and window()
- Streaming write operations using writeStream and foreachBatch()

## Requirements

The Python dependencies for this project are listed in [requirement.txt](requirement.txt).

Main libraries include:

- confluent-kafka
- requests
- sseclient-py

## How to run the project

### 1. Install Python dependencies

```bash
pip install -r requirement.txt
```

### 2. Configure Kafka access

Update [src/kafka_producer/client.properties](src/kafka_producer/client.properties) with the correct Kafka connection settings.

### 3. Start the producer

Run the producer script from the Kafka producer folder:

```bash
python3 wikimedia_kafka_producer.py
```

### 4. Process the data in Spark/Databricks

Run the Spark notebooks or equivalent Databricks workflow to:

- read from Kafka
- enrich the records
- write the silver table
- create the gold aggregation table

## Notes

- This project is a good example of a streaming data engineering workflow using real-world event data.
- The enrichment step depends on the Wikidata API and should be used responsibly with rate limits and error handling in mind.
- The pipeline can be extended with additional transformations, alerting, or dashboard visuals.

## Summary

In short, this project demonstrates how to build a practical streaming analytics pipeline from live Wikimedia data to meaningful business insights using Kafka, Spark, and Databricks.
