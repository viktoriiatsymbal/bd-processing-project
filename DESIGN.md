# Design of Wikipedia Analytics Platform

## 1. Project Overview

This project implements a real-time and batch analytics platform for Wikipedia page creation events.

The system collects events from Wikimedia EventStreams, sends them to Kafka, processes them with Spark Structured Streaming, stores data in Cassandra and MinIO, and exposes analytical results through a REST API

## 2. Data Source

The system uses the Wikimedia EventStreams API:

```text
https://stream.wikimedia.org/v2/stream/page-create
````

The ingestion service reads page creation events from this stream and extracts the main fields needed for analytics:

* domain
* database
* page_id
* page_title
* user_id
* user_name
* is_bot
* performer_is_registered
* user_edit_count
* namespace_id
* created_at
* ingested_at

Invalid events, canary events, and events without `page_id` or `page_title` are skipped

## 3. Architecture

```text
Wikimedia EventStreams
        |
        v
Ingestion Service
        |
        v
Kafka topic: page-create-events
        |
        |------------------------------|
        |                              |
        v                              v
Spark Structured Streaming        Kafka Consumers
        |                              |
        |                              |-------------------|
        v                              v                   v
Kafka Alert Topics              Cassandra              MinIO
        |                        operational tables      raw JSON files
        |
        v
breaking-news-alerts
bot-alerts
spam-alerts


Batch Spark Job reads raw data from MinIO
        |
        v
Cassandra batch report tables
        |
        v
REST API
```

## 4. Technology Choices

### Kafka

Kafka is used as the central event broker. It decouples ingestion from processing and allows multiple consumers to read the same stream independently

Topics used:

* `page-create-events`
* `breaking-news-alerts`
* `bot-alerts`
* `spam-alerts`

### Spark Structured Streaming

Spark Structured Streaming is used for real-time analytics because the project requires metrics with low latency and time-based windows

It is used for:

* breaking news detection
* bot vs human monitoring
* language activity metrics
* spam and vandalism detection

### Cassandra

Cassandra is used for storing query-oriented analytical tables. It is suitable because the API needs fast reads by domain, user, page, and time range

### MinIO

MinIO is used as S3-compatible object storage for raw event data. The batch job reads historical raw JSON files from MinIO

### FastAPI

FastAPI is used for the REST API because it is lightweight, simple to run in Docker, and suitable for exposing analytical endpoints

### Docker Compose

Docker Compose is used to run the full infrastructure locally:

* Kafka
* Spark master and worker
* Cassandra
* MinIO
* ingestion service
* consumers
* REST API

## 5. Streaming Analytics Design

### A1. Breaking News Detector

The system detects breaking news using two methods:

#### 1. Activity Spike Detection

For each domain, Spark calculates page creation activity in a sliding 1 hour window with 5 minute intervals

Main logic:

```text
spike_ratio = pages_last_5min / avg_pages_per_5min
```

If `spike_ratio > 3`, an alert is written to:

```text
breaking-news-alerts
```

Alert fields include:

* alert_time
* alert_type
* domain
* pages_last_5min
* avg_pages_per_5min
* spike_ratio
* sample_pages

#### 2. Keyword Burst Detection

Page titles are cleaned, tokenised, lowercased, and filtered using stop words from multiple languages

For each keyword, the system counts how many different pages contain this keyword during the last 10 minutes. If a keyword appears in 5 or more different page titles, an alert is written to:

```text
breaking-news-alerts
```

Alert fields include:

* alert_time
* alert_type
* keyword
* occurrences
* domains
* sample_pages

### A2. Bot vs Human Activity Monitor

Every minute, the system calculates for each domain:

* number of pages created by bots
* number of pages created by humans
* bot activity percentage
* top 5 bots
* top 5 human users

The results are written to Cassandra table:

```text
bot_activity_metrics
```

The system also sends alerts to:

```text
bot-alerts
```

Alerts are generated when:

* bot activity is above 80%
* one bot creates more than 50 pages in 10 minutes

### A3. Language Activity Dashboard

Every minute, the system calculates for each domain:

* number of created pages
* number of unique authors
* average title length
* trend compared to the previous minute

The results are written to Cassandra table:

```text
language_activity
```

### A4. Spam and Vandalism Detector

The system detects weird/suspicious activity using several rules:

1. A non-bot user creates more than 10 pages in 5 minutes
2. A page title contains a URL, phone-like pattern, or too many digits
3. A new user creates pages in multiple domains
4. A page title is very short or very long

Alerts are written to:

```text
spam-alerts
```

Each alert includes a severity level:

```text
low / medium / high
```

## 6. Batch Analytics Design

Batch analytics are generated from raw events stored in MinIO

Raw files are stored in this structure:

```text
s3a://wiki-events/raw/YYYY/MM/DD/HH/page_create_YYYYMMDD_HHMM.json
```

The batch job reads historical data from MinIO and writes aggregated results to Cassandra

### B1. Hourly Activity Report

For each domain and each full hour, the batch job calculates:

* number of created pages
* number of unique authors
* bot vs human ratio
* top 10 authors
* page counts by namespace

The results are stored in:

```text
hourly_activity_by_domain
```

The API endpoint is:

```text
GET /api/reports/hourly?domain=<domain>&hours=6
```

The report excludes the current incomplete hour

### B2. Editor Behavior Patterns

For users who created more than 5 pages, the batch job calculates:

* average time between page creations
* active hours
* number of domains where the user was active
* dominant domain

The results are stored in:

```text
editor_behavior_patterns
```

The API endpoint is:

```text
GET /api/analytics/editor-patterns?min_pages=5
```

## 7. REST API Design

The REST API reads data from Cassandra and exposes project results

Implemented endpoints:

```text
GET /health
GET /api/domains
GET /api/users/{user_id}/pages?limit=100
GET /api/pages/{page_id}
GET /api/domains/{domain}/pages?from=<timestamp>&to=<timestamp>&limit=100
GET /api/reports/hourly?domain=<domain>&hours=6
GET /api/analytics/editor-patterns?min_pages=5
```

## 8. Cassandra Data Model

Each table is designed for a specific API endpoint or analytical output

### page_details

Used for:

```text
GET /api/pages/{page_id}
```

Stores detailed information about a page by page ID

### pages_by_user

Used for:

```text
GET /api/users/{user_id}/pages
```

Stores pages created by a specific user

### pages_by_domain

Used for:

```text
GET /api/domains/{domain}/pages
```

Stores pages by domain and creation time

### domains_seen

Used for:

```text
GET /api/domains
```

Stores the list of domains observed in the stream

### language_activity

Used for A3 language activity dashboard.

Stores minute-level activity by domain

### bot_activity_metrics

Used for A2 bot vs human monitoring

Stores minute-level bot and human activity metrics by domain

### hourly_activity_by_domain

Used for B1 hourly activity report

Stores hourly historical aggregations

### editor_behavior_patterns

Used for B2 editor behavior analysis

Stores user-level behavior patterns

## 9. Data Flow by Component

### Ingestion Service

Folder:

```text
ingestion/
```

Responsibility:

```text
Wikimedia EventStreams -> Kafka page-create-events
```

### Cassandra Consumer

Folder:

```text
consumer-cassandra/
```

Responsibility:

```text
Kafka page-create-events -> Cassandra operational tables
```

### MinIO Consumer

Folder:

```text
consumer-minio/
```

Responsibility:

```text
Kafka page-create-events -> MinIO raw JSON files
```

### Streaming Job

Folder:

```text
shared/
```

Responsibility:

```text
Kafka page-create-events -> real-time metrics and alerts
```

### Batch Job

Folder:

```text
batch/
```

Responsibility:

```text
MinIO raw files -> Cassandra batch report tables
```

### REST API

Folder:

```text
api/
```

Responsibility:

```text
Cassandra -> HTTP API responses
```

## 10. Deployment Overview

The system is deployed locally using Docker Compose. It is responsible for running the full infrastructure:

- ingestion service
- Kafka
- Spark master and worker
- Cassandra
- Cassandra consumer
- MinIO
- MinIO consumer
- REST API