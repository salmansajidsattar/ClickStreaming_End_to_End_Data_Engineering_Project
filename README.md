# Clickstream Analytics Pipeline

An end-to-end Python solution for processing and analyzing user clickstream data in real-time.

## Project Architecture

```
┌─────────────┐    ┌───────────┐    ┌────────────────┐    ┌───────────────┐    ┌─────────────┐
│ Clickstream │    │           │    │                │    │               │    │             │
│ Data Source ├───►│ Kafka     ├───►│ Spark Streaming├───►│ PostgreSQL/   ├───►│ Dashboard   │
│             │    │           │    │                │    │ Redis         │    │             │
└─────────────┘    └───────────┘    └────────────────┘    └───────────────┘    └─────────────┘
```


## Overview

This pipeline consists of five main components:

1. **Data Generation Layer**: Simulates clickstream data from a web application
2. **Data Ingestion Layer**: Uses Apache Kafka for real-time data streaming
3. **Stream Processing Layer**: Leverages PySpark Streaming for data processing and analytics
4. **Storage Layer**: Stores processed data in PostgreSQL (structured data) and Redis (real-time metrics)

## Prerequisites

* Python 3.8+
* Docker and Docker Compose
* Basic knowledge of Kafka, Spark, SQL

## Quick Start

1. Clone the repository
   ```bash
   git clone https://github.com/yourusername/clickstream-analytics.git
   cd clickstream-analytics
   ```

2. Create a virtual environment (optional but recommended)
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

4. Start the services with Docker Compose
   ```bash
   docker-compose up -d
   ```

5. Run the data generator
   ```bash
   python -m data_generator.clickstream_generator
   ```

6. Start the Kafka producer
   ```bash
   python -m kafka_producer.producer
   ```

7. Launch the Spark streaming processor
   ```bash
   python -m spark_processor.stream_processor
   ```

8. Run the dashboard application
   ```bash
   python -m dashboard.app
   ```

9. Access the dashboard at http://localhost:8050

## Project Structure

```
clickstream-analytics/
├── docker-compose.yml          # Docker configuration for services
├── requirements.txt            # Python dependencies
├── data_generator/             # Synthetic clickstream data generation
│   ├── __init__.py
│   └── clickstream_generator.py
├── kafka_producer/             # Kafka producer for data ingestion
│   ├── __init__.py
│   └── producer.py
├── spark_processor/            # PySpark streaming for data processing
│   ├── __init__.py
│   └── stream_processor.py
├── storage/                    # Database connectors
│   ├── __init__.py
│   ├── postgres_connector.py
│   └── redis_connector.py
```

## Component Details

### Data Generator

The data generator simulates user clickstream events with realistic properties:
- User IDs and session IDs
- Timestamps
- Page URLs and referrers
- User agents and device information
- Event types (page view, click, scroll, etc.)
- Custom properties (product IDs, categories, etc.)

### Kafka Producer

Sends generated clickstream events to Kafka topics for real-time processing:
- Serializes events as JSON
- Implements retry logic and error handling
- Configurable batch sizes and compression

### Spark Streaming Processor

Processes the streaming data with the following features:
- Session identification and tracking
- User journey analysis
- Real-time metrics calculation
- Anomaly detection
- Data enrichment and transformation

### Storage Layer

- **PostgreSQL**: Stores structured data for historical analysis
  - User sessions
  - Aggregated metrics
  - Conversion funnels
  
- **Redis**: Caches real-time metrics for dashboard visualization
  - Active users count
  - Page popularity
  - Conversion rates
  - Performance metrics

### Dashboard

A web-based dashboard built with Flask and Plotly/Dash that visualizes:
- Real-time user activity
- Session metrics
- Conversion funnels
- User journey flows
- Anomaly detection results


## Troubleshooting

### Common Issues

1. **Kafka Connection Errors**
   - Ensure Kafka is running and accessible
   - Check network configurations if running in containers

2. **Spark Processing Delays**
   - Adjust batch interval and processing parameters
   - Monitor resource utilization

3. **Dashboard Performance**
   - Optimize Redis queries
   - Consider caching for frequently accessed visualizations
