# spark_processor/stream_processor.py
import os
import json
import logging
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Import configuration
import sys
import pathlib
# Get the project root directory
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent

# Add the project root to the Python path
sys.path.insert(0, str(PROJECT_ROOT))
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config
from storage.postgres_connector import PostgresConnector
from storage.redis_connector import RedisConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ClickstreamProcessor:
    """Process clickstream data using Spark Structured Streaming in a distributed environment."""
    
    def __init__(self):
        """Initialize the Spark Streaming processor with configuration from environment variables."""
        # Load configuration
        self.kafka_config = Config.get_kafka_config()
        self.postgres_config = Config.get_postgres_config()
        self.redis_config = Config.get_redis_config()
        self.spark_config = Config.get_spark_config()
        
        # Extract specific configs
        self.kafka_bootstrap_servers = self.kafka_config["bootstrap_servers"]
        self.kafka_topic = self.kafka_config["topic"]
        self.checkpoint_dir = self.spark_config["checkpoint_dir"]
        
        # Ensure checkpoint directory exists
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        
        # Initialize storage connectors
        self.postgres = PostgresConnector(**self.postgres_config)
        self.redis = RedisConnector(**self.redis_config)
        
        # Initialize Spark session with distributed configuration
        self.spark = self._create_spark_session()
        
        logger.info("Initialized Clickstream Processor")
    
    def _create_spark_session(self):
        """Create and configure a Spark session optimized for distributed processing."""
        spark = SparkSession.builder \
            .appName(self.spark_config["app_name"]) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.sql.shuffle.partitions", self.spark_config["shuffle_partitions"]) \
            .config("spark.streaming.kafka.consumer.cache.enabled", "false") \
            .config("spark.executor.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .config("spark.streaming.concurrentJobs", "4") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        # Register UDFs if needed
        # spark.udf.register("custom_function", custom_function)
        
        return spark
    
    def define_schema(self):
        """Define schema for the clickstream data."""
        return StructType([
            StructField("user_id", IntegerType(), True),
            StructField("session_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("page", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("referrer", StringType(), True),
            StructField("device", StructType([
                StructField("type", StringType(), True),
                StructField("browser", StringType(), True),
                StructField("os", StringType(), True)
            ]), True),
            StructField("ip_address", StringType(), True),
            StructField("user_agent", StringType(), True),
            # Optional fields
            StructField("search_query", StringType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("product_price", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("order_id", StringType(), True),
            StructField("total_amount", DoubleType(), True)
        ])
    
    def read_kafka_stream(self):
        """Create DataFrame representing the stream of input from Kafka."""
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", 'localhost:9092') \
            .option("subscribe", 'clickstream') \
            .option("startingOffsets", "latest")\
            .load()
        print("test",df.printSchema())
        
        # Parse the JSON value
        schema = self.define_schema()
        parsed_df = df.select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("kafka_key", "data.*", "kafka_timestamp")
        
        return parsed_df
    
    def process_page_views(self, stream_df):
        """Process page view events for page analytics with error handling."""
        # Wrap processing in try-except for resilience
        try:
            # Filter for page_view events and partition for better parallelism
            page_views = stream_df.filter(col("event_type") == "page_view").repartition(10)
            
            # Aggregate page views by page in 1-minute windows
            page_counts = page_views \
                .withWatermark("timestamp", "1 minute") \
                .groupBy(
                    window(col("timestamp"), "1 minute"),
                    col("page")
                ) \
                .count() \
                .select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("page"),
                    col("count").alias("view_count")
                )
            
            # Write to PostgreSQL with retry mechanism
            query = page_counts \
                .writeStream \
                .outputMode("update") \
                .option("checkpointLocation", f"{self.checkpoint_dir}/page_views") \
                .foreachBatch(self.save_page_views_to_postgres) \
                .trigger(processingTime=f"{self.spark_config['batch_duration']} seconds") \
                .start()
            
            # Also update real-time counts in Redis
            redis_query = page_views \
                .writeStream \
                .outputMode("update") \
                .option("checkpointLocation", f"{self.checkpoint_dir}/redis_page_views") \
                .foreachBatch(self.update_redis_page_counts) \
                .trigger(processingTime=f"{self.spark_config['batch_duration']} seconds") \
                .start()
            
            return [query, redis_query]
        except Exception as e:
            logger.error(f"Error processing page views: {e}")
            # Return empty list if processing fails
            return []
    
    def process_user_sessions(self, stream_df):
        """Process session data for user journey analytics."""
        try:
            # Group events by session - use repartitioning for better distribution
            session_events = stream_df \
                .repartition(col("session_id")) \
                .withWatermark("timestamp", "10 minutes") \
                .groupBy(
                    col("session_id"),
                    col("user_id")
                ) \
                .agg(
                    min("timestamp").alias("session_start"),
                    max("timestamp").alias("session_end"),
                    count("*").alias("event_count"),
                    collect_list("page").alias("pages_visited"),
                    collect_list("event_type").alias("event_types")
                )
            
            # Write session data to PostgreSQL
            query = session_events \
                .writeStream \
                .outputMode("update") \
                .option("checkpointLocation", f"{self.checkpoint_dir}/user_sessions") \
                .foreachBatch(self.save_sessions_to_postgres) \
                .trigger(processingTime=f"{self.spark_config['batch_duration']} seconds") \
                .start()
            
            return [query]
        except Exception as e:
            logger.error(f"Error processing user sessions: {e}")
            return []
    
    def process_conversions(self, stream_df):
        """Process purchase events for conversion analytics."""
        try:
            # Filter for purchase events
            purchases = stream_df.filter(col("event_type") == "purchase")
            
            # Aggregate purchase data
            purchase_stats = purchases \
                .withWatermark("timestamp", "5 minutes") \
                .groupBy(
                    window(col("timestamp"), "1 hour")
                ) \
                .agg(
                    count("*").alias("purchase_count"),
                    sum("total_amount").alias("total_revenue"),
                    approx_count_distinct("user_id").alias("unique_buyers")
                ) \
                .select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("purchase_count"),
                    col("total_revenue"),
                    col("unique_buyers")
                )
            
            # Write to PostgreSQL
            query = purchase_stats \
                .writeStream \
                .outputMode("update") \
                .option("checkpointLocation", f"{self.checkpoint_dir}/conversions") \
                .foreachBatch(self.save_purchase_stats_to_postgres) \
                .trigger(processingTime=f"{self.spark_config['batch_duration']} seconds") \
                .start()
            
            # Update real-time metrics in Redis
            redis_query = purchases \
                .writeStream \
                .outputMode("update") \
                .option("checkpointLocation", f"{self.checkpoint_dir}/redis_purchases") \
                .foreachBatch(self.update_redis_purchase_metrics) \
                .trigger(processingTime=f"{self.spark_config['batch_duration']} seconds") \
                .start()
            
            return [query, redis_query]
        except Exception as e:
            logger.error(f"Error processing conversions: {e}")
            return []
    
    def process_device_stats(self, stream_df):
        """Process device-related statistics from clickstream data."""
        try:
            # Extract and analyze device information
            device_stats = stream_df \
                .withWatermark("timestamp", "5 minutes") \
                .groupBy(
                    window(col("timestamp"), "1 hour"),
                    col("device.type").alias("device_type"),
                    col("device.browser").alias("browser"),
                    col("device.os").alias("operating_system")
                ) \
                .agg(
                    count("*").alias("visit_count"),
                    approx_count_distinct("user_id").alias("unique_users"),
                    approx_count_distinct("session_id").alias("unique_sessions")
                ) \
                .select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("device_type"),
                    col("browser"),
                    col("operating_system"),
                    col("visit_count"),
                    col("unique_users"),
                    col("unique_sessions")
                )

            # Write to PostgreSQL
            query = device_stats \
                .writeStream \
                .outputMode("update") \
                .option("checkpointLocation", f"{self.checkpoint_dir}/device_stats") \
                .foreachBatch(self.save_device_stats_to_postgres) \
                .trigger(processingTime=f"{self.spark_config['batch_duration']} seconds") \
                .start()

            return [query]
        except Exception as e:
            logger.error(f"Error processing device statistics: {e}")
            return []

    # Database write functions with retry mechanism
    def save_page_views_to_postgres(self, batch_df, batch_id):
        """Save page view statistics to PostgreSQL with retry logic."""
        if batch_df.isEmpty():
            return
        
        # Process the batch and save to PostgreSQL with retries
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.postgres.save_dataframe(
                    batch_df, 
                    "page_view_stats",
                    mode="append"
                )
                logger.info(f"Saved {batch_df.count()} page view records to PostgreSQL (batch {batch_id})")
                break
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    sleep_time = 2 ** retry_count  # Exponential backoff
                    logger.warning(f"Error saving to PostgreSQL, retrying in {sleep_time}s: {e}")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Failed to save page views to PostgreSQL after {max_retries} retries: {e}")
    
    # Similar retry pattern for other save functions
    def save_sessions_to_postgres(self, batch_df, batch_id):
        """Save session data to PostgreSQL with retry logic."""
        if batch_df.isEmpty():
            return
        
        # Convert arrays to strings for storage
        session_df = batch_df.withColumn(
            "pages_visited", 
            array_join(col("pages_visited"), ",")
        ).withColumn(
            "event_types",
            array_join(col("event_types"), ",")
        )
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.postgres.save_dataframe(
                    session_df, 
                    "user_sessions",
                    mode="append"
                )
                logger.info(f"Saved {batch_df.count()} session records to PostgreSQL (batch {batch_id})")
                break
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    sleep_time = 2 ** retry_count
                    logger.warning(f"Error saving sessions to PostgreSQL, retrying in {sleep_time}s: {e}")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Failed to save sessions to PostgreSQL after {max_retries} retries: {e}")
    
    # Redis update function with retry logic
    def update_redis_page_counts(self, batch_df, batch_id):
        """Update real-time page view counts in Redis with retry logic."""
        if batch_df.isEmpty():
            return
        
        # Count page views in this batch
        page_counts = batch_df.groupBy("page").count().collect()
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Update Redis counters in a pipeline for efficiency
                pipeline = self.redis.get_pipeline()
                timestamp = int(time.time())
                
                for row in page_counts:
                    page = row["page"]
                    count = row["count"]
                    
                    # Increment page view counter
                    pipeline.increment(f"page_views:{page}", count)
                    
                    # Add to time-series data with expiration (keep last 24 hours)
                    pipeline.add_time_series(f"page_views_ts:{page}", timestamp, count, expiry=86400)
                
                # Execute all commands in one go
                pipeline.execute()
                
                logger.info(f"Updated Redis page view counts for {len(page_counts)} pages (batch {batch_id})")
                break
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    sleep_time = 2 ** retry_count
                    logger.warning(f"Error updating Redis, retrying in {sleep_time}s: {e}")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Failed to update Redis page counts after {max_retries} retries: {e}")
    
    def save_device_stats_to_postgres(self, batch_df, batch_id):
        """Save device statistics to PostgreSQL with retry logic."""
        if batch_df.isEmpty():
            return
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.postgres.save_dataframe(
                    batch_df,
                    "device_stats",
                    mode="append"
                )
                logger.info(f"Saved {batch_df.count()} device stat records to PostgreSQL (batch {batch_id})")
                break
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    sleep_time = 2 ** retry_count
                    logger.warning(f"Error saving device stats to PostgreSQL, retrying in {sleep_time}s: {e}")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Failed to save device stats to PostgreSQL after {max_retries} retries: {e}")
    
    def start_processing(self):
        """Start all stream processing pipelines with fault tolerance."""
        queries = []
        
        try:
            # Read the stream
            stream_df = self.read_kafka_stream()
            
            # Cache the stream for reuse by different processing pipelines
            cached_stream = stream_df.persist()
            
            # Process different aspects of the data
            queries.extend(self.process_page_views(cached_stream))
            queries.extend(self.process_user_sessions(cached_stream))
            queries.extend(self.process_conversions(cached_stream))
            queries.extend(self.process_device_stats(cached_stream))
            
            # Set up monitoring
            self._setup_monitoring(queries)
            
            logger.info(f"Started {len(queries)} streaming queries")
            
            # Wait for termination or handle graceful shutdown
            self._await_termination(queries)
                
        except Exception as e:
            logger.error(f"Error in stream processing: {e}")
            self._stop_all_queries(queries)
            raise
        finally:
            logger.info("Stopping Spark session")
            self.spark.stop()
    
    def _setup_monitoring(self, queries):
        """Set up monitoring for the streaming queries."""
        for i, query in enumerate(queries):
            # Log query progress
            query.addListener(self._query_listener())
    
    def _query_listener(self):
        """Create a listener for query progress."""
        from pyspark.sql.streaming import StreamingQueryListener
        
        class QueryProgressListener(StreamingQueryListener):
            def onQueryStarted(self, event):
                logger.info(f"Query started: {event.id}")
                
            def onQueryProgress(self, event):
                progress = event.progress
                
                logger.debug(f"Query progress: {progress.name}, " 
                             f"rows/sec={progress.processedRowsPerSecond:.1f}, "
                             f"latency={progress.durationMs.triggerExecution/1000:.1f}s")
                
            def onQueryTerminated(self, event):
                if event.exception:
                    logger.error(f"Query {event.id} terminated with error: {event.exception}")
                else:
                    logger.info(f"Query {event.id} terminated successfully")
        
        return QueryProgressListener()
    
    def _await_termination(self, queries):
        """Wait for all queries to terminate with monitoring."""
        try:
            # Wait for any query to terminate
            self.spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, stopping gracefully...")
            self._stop_all_queries(queries)
    
    def _stop_all_queries(self, queries):
        """Stop all streaming queries gracefully."""
        for i, query in enumerate(queries):
            try:
                if query.isActive:
                    logger.info(f"Stopping query {i+1}/{len(queries)}...")
                    query.stop()
            except Exception as e:
                logger.error(f"Error stopping query: {e}")
                
    def start_processing(self):
        """Start all stream processing pipelines."""
        try:
            # Read the stream
            stream_df = self.read_kafka_stream()
            
            # Process different aspects of the data
            queries = []
            queries.extend(self.process_page_views(stream_df))
            queries.extend(self.process_user_sessions(stream_df))
            queries.extend(self.process_conversions(stream_df))
            queries.extend(self.process_device_stats(stream_df))
            
            logger.info(f"Started {len(queries)} streaming queries")
            
            # Wait for all queries to terminate
            for query in queries:
                query.awaitTermination()
                
        except Exception as e:
            logger.error(f"Error in stream processing: {e}")
            raise
        finally:
            logger.info("Stopping Spark session")
            self.spark.stop()

if __name__ == "__main__":
    # Create and start processor
    processor = ClickstreamProcessor()
    processor.start_processing()