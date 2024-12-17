import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# Function to create Cassandra connection
def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])  # Connect to Cassandra on localhost
        session = cluster.connect()
        logging.info("Cassandra connection created successfully!")
        return session
    except Exception as e:
        logging.error(f"Could not connect to Cassandra: {e}")
        return None

# Create keyspace in Cassandra
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")

# Create table in Cassandra
def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.orders (
        order_id UUID PRIMARY KEY,
        product_name TEXT,
        product_category TEXT,
        quantity INT,
        price FLOAT,
        total_order_amount FLOAT,
        order_timestamp TIMESTAMP,
        purchase_location TEXT
    );
    """)
    print("Table created successfully!")

# Create Spark session
def create_spark_connection():
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return s_conn
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception: {e}")
        return None

# Connect to Kafka and read data
def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'orders_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
        return spark_df
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")
        return None

# Parse JSON messages and extract fields
def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("product_category", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("price", FloatType(), False),
        StructField("total_order_amount", FloatType(), False),
        StructField("order_timestamp", TimestampType(), False),
        StructField("purchase_location", StringType(), False)
    ])

    # Parse Kafka message value
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    return sel

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Step 1: Create Spark session
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Step 2: Connect to Kafka and read messages
        spark_df = connect_to_kafka(spark_conn)

        if spark_df is not None:
            # Step 3: Parse and format data
            selection_df = create_selection_df_from_kafka(spark_df)
            print("Schema of parsed DataFrame:")
            selection_df.printSchema()

            # Step 4: Connect to Cassandra
            session = create_cassandra_connection()

            if session is not None:
                create_keyspace(session)
                create_table(session)

                # Step 5: Write data to Cassandra in real-time
                logging.info("Starting streaming to Cassandra...")
                streaming_query = (selection_df.writeStream
                                   .format("org.apache.spark.sql.cassandra")
                                   .option('checkpointLocation', '/tmp/spark-checkpoint')  
                                   .option('keyspace', 'spark_streams')
                                   .option('table', 'orders')
                                   .start())

                # Await termination
                streaming_query.awaitTermination()
