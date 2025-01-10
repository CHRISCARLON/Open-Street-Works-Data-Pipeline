from pyspark.sql import SparkSession
import requests
import psutil
import os
from stream_unzip import stream_unzip
from memory_profiler import profile

def fetch_data(dl_url):
    """tbc"""
    with requests.get(dl_url, stream=True, timeout=30) as resp:
        yield from resp.iter_content(chunk_size=6500)

def take(iterable, n: int):
    """tbc"""
    for i in range(n):
        yield next(iterable)

@profile
def stream_jsons_to_spark_df(spark, zipped_chunks, number):
    """tbc"""
    json_rdds = []

    # Process only the first n files from the zipped content
    for file, size, unzipped_chunks in take(stream_unzip(zipped_chunks), number):
        if isinstance(file, bytes):
            file = file.decode('utf-8')

        # Assuming each unzipped chunk is a complete JSON object.
        for chunk in unzipped_chunks:
            json_str = chunk.decode('utf-8')
            json_rdd = spark.sparkContext.parallelize([json_str])
            json_rdds.append(json_rdd)

    # Union all RDDs to create a single RDD
    unified_rdd = spark.sparkContext.union(json_rdds)

    # Create DataFrame from RDD
    data_frame = spark.read.json(unified_rdd)

    return data_frame

if __name__ == "__main__":

    # Get the initial memory usage
    initial_memory = psutil.Process(os.getpid()).memory_info().rss
    # Initialize Spark Session
    spark_sesh = SparkSession.builder.appName("JsonStreaming").getOrCreate()

    dl_link = "https://opendata.manage-roadworks.service.gov.uk/permit/2024/01.zip"
    chunks_to_process = fetch_data(dl_link)

    # Process and convert to Spark DataFrame, limiting to first 10 files
    df = stream_jsons_to_spark_df(spark_sesh, chunks_to_process, number=10)

    # Show the first few rows to verify
    df.show()

    # Get the final memory usage
    final_memory = psutil.Process(os.getpid()).memory_info().rss
    print(final_memory)

    # Calculate the memory usage difference
    memory_usage = final_memory - initial_memory

    # Convert memory usage to megabytes
    memory_usage_mb = memory_usage / (1024 * 1024)

    print(f"Memory usage: {memory_usage_mb:.2f} MB")
