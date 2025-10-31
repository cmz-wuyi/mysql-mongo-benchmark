# -*- coding: utf-8 -*-
import pymysql
import pymongo
import time
import random
import statistics
import pandas as pd
from tqdm import tqdm
import sys
import threading  # Import threading module

# ==============================================================================
# Step 1: Database Connection Configuration (Unchanged)
# ==============================================================================
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'test',
    'autocommit': True
}
# Note: Please ensure you have existing data in your MySQL database,
# or manually import data into this table.
MYSQL_TABLE = 'tweets'

MONGO_URI = 'mongodb://localhost:27017/'
MONGO_DATABASE = 'Test'
# Similarly, ensure you have the corresponding collection and data in your MongoDB.
MONGO_COLLECTION = 'User_comment'

# ==============================================================================
# Step 2: Test Parameter Configuration
# ==============================================================================
IDS_TO_SAMPLE_FOR_TESTS = 500000  # Number of random IDs to sample from the DB for read/update tests
QUERIES_PER_RUN = 10  # Number of operations each thread will execute
NUM_RUNS = 20  # Number of times each workload is repeated
NUM_THREADS = 16  # Number of concurrent threads (concurrency level)

# ==============================================================================
# Step 3: Database Environment Setup
# ==============================================================================

def setup_mysql(conn):
    """Prepares the MySQL environment: Creates the table if it does not exist. Does not perform any data operations."""
    print("\nVerifying MySQL environment...")
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"USE {MYSQL_CONFIG['database']}")
            # Use `IF NOT EXISTS` to safely create the table; does nothing if the table already exists.
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {MYSQL_TABLE} (
                    target INT,
                    ids BIGINT PRIMARY KEY,
                    `date` VARCHAR(255),
                    flag VARCHAR(255),
                    `user` VARCHAR(255),
                    `text` TEXT
                )
            """)
            print(f"MySQL table '{MYSQL_TABLE}' is ready.")
    except pymysql.Error as e:
        print(f"Error during MySQL setup: {e}")
        sys.exit(1)


def setup_mongo(client):
    """Prepares the MongoDB environment: Checks and fixes index conflicts to ensure the final index is correct."""
    print("\nVerifying MongoDB environment...")
    db = client[MONGO_DATABASE]
    collection = db[MONGO_COLLECTION]

    # --- Our expected index definition ---
    expected_index_name = "ids_1"
    expected_index_key = [('ids', pymongo.ASCENDING)]
    expected_index_unique = False
    # ---

    try:
        print(f"Checking indexes for collection '{MONGO_COLLECTION}'...")
        existing_indexes = collection.index_information()

        conflict_found = False
        if expected_index_name in existing_indexes:
            existing_index = existing_indexes[expected_index_name]
            normalized_existing_key = [(k, int(v)) for k, v in existing_index['key']]
            if normalized_existing_key != expected_index_key:
                conflict_found = True
                print(f"Index key conflict found for '{expected_index_name}'. Existing: {existing_index['key']}, Expected: {expected_index_key}")
            if existing_index.get('unique') != expected_index_unique:
                conflict_found = True
                print(f"Index unique constraint conflict for '{expected_index_name}'. Existing: {existing_index.get('unique')}, Expected: {expected_index_unique}")

        if conflict_found:
            print(f"Dropping conflicting index '{expected_index_name}'...")
            collection.drop_index(expected_index_name)
            existing_indexes = collection.index_information()

        if expected_index_name not in existing_indexes:
            print(f"Creating index '{expected_index_name}' on 'ids' field...")
            collection.create_index(expected_index_key, name=expected_index_name, unique=expected_index_unique)
        else:
            print(f"Index '{expected_index_name}' already exists and is correctly configured.")

        print(f"MongoDB collection '{MONGO_COLLECTION}' is ready.")

    except Exception as e:
        print(f"An error occurred during MongoDB index setup: {e}")
        sys.exit(1)


# ==============================================================================
# Step 4: Dynamically Fetch Sample IDs from Database (New Feature)
# ==============================================================================
def fetch_sample_ids(connection, db_type, count):
    """Randomly fetch a specified number of IDs from the database"""
    print(f"Fetching {count} sample IDs from {db_type}...")
    ids = []
    try:
        if db_type == "MySQL":
            with connection.cursor() as cursor:
                # Note: ORDER BY RAND() can be slow on large tables.
                # For very large datasets, consider alternative sampling methods.
                cursor.execute(f"SELECT ids FROM {MYSQL_TABLE} ORDER BY RAND() LIMIT %s", (count,))
                ids = [row[0] for row in cursor.fetchall()]

        elif db_type == "MongoDB":
            collection = connection[MONGO_DATABASE][MONGO_COLLECTION]
            pipeline = [{'$sample': {'size': count}}, {'$project': {'ids': 1, '_id': 0}}]
            ids = [doc['ids'] for doc in collection.aggregate(pipeline)]

        if len(ids) < count:
            print(f"Warning: Fetched only {len(ids)} IDs from {db_type}, which is less than the requested {count}.")
            if len(ids) == 0:
                print(f"Error: No IDs found in {db_type}. Please ensure the database is populated.")
                sys.exit(1)

        print(f"Successfully fetched {len(ids)} IDs.")
        return ids
    except Exception as e:
        print(f"Error fetching IDs from {db_type}: {e}")
        sys.exit(1)


# ==============================================================================
# Step 5: Workload Definitions (Modified)
# ==============================================================================
def run_benchmark(db_type, workload_func, connection, sample_ids, queries_per_run, num_runs):
    """Generic benchmark executor (Single-threaded, kept for reference)"""
    timings = []

    if len(sample_ids) < queries_per_run:
        return {
            "Workload": workload_func.__name__, "DB Type": db_type,
            "Avg Latency (ms)": "N/A", "Std Dev (ms)": "N/A", "Throughput (QPS)": "N/A"
        }

    for _ in range(num_runs):
        run_ids = random.sample(sample_ids, queries_per_run)
        start_time = time.perf_counter()
        workload_func(connection, run_ids)
        end_time = time.perf_counter()
        timings.append(end_time - start_time)

    total_time = sum(timings)
    avg_latency = statistics.mean(timings) * 1000
    std_dev = statistics.stdev(timings) * 1000 if len(timings) > 1 else 0
    qps = (queries_per_run * num_runs) / total_time

    return {
        "Workload": workload_func.__name__,
        "DB Type": db_type,
        "Avg Latency (ms)": f"{avg_latency:.2f}",
        "Std Dev (ms)": f"{std_dev:.2f}",
        "Throughput (QPS)": f"{qps:.2f}"
    }


# --- MySQL Workloads (Fields updated) ---
def workload_mysql_read(cursor, ids):
    for an_id in ids:
        cursor.execute(f"SELECT * FROM {MYSQL_TABLE} WHERE ids = %s", (an_id,))
        cursor.fetchone()


def workload_mysql_read_update(cursor, ids):
    for i, an_id in enumerate(ids):
        if i % 20 == 0:  # 5% Update
            cursor.execute(f"UPDATE {MYSQL_TABLE} SET user = 'benchmark_user' WHERE ids = %s", (an_id,))
        else:  # 95% Read
            cursor.execute(f"SELECT text FROM {MYSQL_TABLE} WHERE ids = %s", (an_id,))
            cursor.fetchone()


def workload_mysql_insert(cursor, records, temp_table_name):  # <--- Change 1: Added temp_table_name parameter
    """Performs insert operations on a unique temporary table"""
    try:
        # <--- Change 2: Operate on the unique temp table and define schema explicitly
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
        cursor.execute(f"""
            CREATE TABLE {temp_table_name} (
                target INT,
                ids BIGINT PRIMARY KEY,
                `date` VARCHAR(255),
                flag VARCHAR(255),
                `user` VARCHAR(255),
                `text` TEXT
            )
        """)

        sql = f"INSERT INTO {temp_table_name} (target, ids, `date`, flag, `user`, `text`) VALUES (%s, %s, %s, %s, %s, %s)"
        for row in records:
            cursor.execute(sql, (row['target'], row['ids'], row['date'], row['flag'], row['user'], row['text']))
    finally:
        # Clean up the temp table
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")


# --- MongoDB Workloads (Fields updated) ---
def workload_mongo_read(collection, ids):
    for an_id in ids:
        collection.find_one({'ids': an_id})


def workload_mongo_read_update(collection, ids):
    for i, an_id in enumerate(ids):
        if i % 20 == 0:  # 5% Update
            collection.update_one({'ids': an_id}, {'$set': {'user': 'benchmark_user'}})
        else:  # 95% Read
            collection.find_one({'ids': an_id}, {'text': 1})


def workload_mongo_insert(collection, records):
    # Insert into the temporary collection provided by worker_thread
    collection.insert_many(records)


# ==============================================================================
# NEW: Step 5.1 - Concurrent Worker Thread Function
# ==============================================================================
def worker_thread(db_type, workload_func, sample_data, queries_per_thread, mongo_client_ref=None):
    """
    The function executed by each thread.
    It creates its own database connection and executes a specified number of queries.
    """
    db_conn = None  # <--- Change: Define here to allow closing in finally
    connection = None
    try:
        # --- Key: Create an independent connection for each thread ---
        if db_type == "MySQL":
            db_conn = pymysql.connect(**MYSQL_CONFIG)
            connection = db_conn.cursor()
        elif db_type == "MongoDB":
            # --- Key Change ---
            # Create a unique temporary collection for each thread for insert tests
            if workload_func.__name__ == 'workload_mongo_insert':
                # Use thread ID to ensure collection name uniqueness
                collection_name = f"temp_insert_{threading.get_ident()}"
                connection = mongo_client_ref[MONGO_DATABASE][collection_name]
            else:
                # Use the main collection for read/update tests
                connection = mongo_client_ref[MONGO_DATABASE][MONGO_COLLECTION]

        # Select data based on workload type
        if "insert" in workload_func.__name__:
            records = []
            base_id = int(time.time() * 1000) + threading.get_ident()
            for i in range(queries_per_thread):
                records.append({
                    'target': 4, 'ids': base_id * 100 + i,  # Ensure ID uniqueness
                    'date': 'Mon Sep 29 10:41:19 UTC 2025',
                    'flag': 'NO_QUERY', 'user': f'insert_user_{threading.get_ident()}_{i}',
                    'text': 'This is an inserted tweet.'
                })

            # <--- Core Change: Pass a unique table name for MySQL insert
            if db_type == "MySQL":
                temp_table_name = f"temp_tweets_insert_{threading.get_ident()}"
                workload_func(connection, records, temp_table_name)
            else:  # MongoDB
                workload_func(connection, records)
        else:
            # For read/read-update workloads
            run_ids = random.sample(sample_data, queries_per_thread)
            workload_func(connection, run_ids)

    except Exception as e:
        print(f"Error in thread {threading.get_ident()} for {db_type}: {e}")
    finally:
        # --- Key: Close each thread's own connection ---
        if db_type == "MySQL" and connection:
            connection.close()
            if db_conn:
                db_conn.close()
        # <--- Change: Fix boolean check for MongoDB
        elif db_type == "MongoDB" and "insert" in workload_func.__name__ and connection is not None:
            # Clean up the temporary insert collection
            connection.drop()


# ==============================================================================
# NEW: Step 5.2 - Concurrent Benchmark Executor
# ==============================================================================
def run_concurrent_benchmark(db_type, workload_func, sample_ids, queries_per_thread, num_threads, num_runs,
                             mongo_client=None):
    """
    Function to execute the concurrent benchmark.
    """
    timings = []

    total_queries_per_run = queries_per_thread * num_threads
    if "insert" not in workload_func.__name__ and len(sample_ids) < total_queries_per_run:
        print(f"Warning: Not enough sample IDs for all threads ({len(sample_ids)} < {total_queries_per_run}). Skipping test.")
        return {"Workload": f"{workload_func.__name__} ({num_threads} threads)", "DB Type": db_type,
                "Avg Latency (ms)": "N/A", "Std Dev (ms)": "N/A", "Throughput (QPS)": "N/A"}

    for _ in range(num_runs):
        threads = []
        start_time = time.perf_counter()

        # Create and start all threads
        for _ in range(num_threads):
            thread = threading.Thread(target=worker_thread, args=(
                db_type, workload_func, sample_ids, queries_per_thread, mongo_client
            ))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        end_time = time.perf_counter()
        timings.append(end_time - start_time)

    total_time = sum(timings)
    # Total queries = (Queries per thread * Threads) * Number of runs
    total_queries = total_queries_per_run * num_runs

    # Average time per run (s)
    avg_run_time = statistics.mean(timings)
    # Avg Latency (ms) = (Avg time per run / Queries per run) * 1000
    # Note: This calculates latency per *query*, not per run
    avg_latency = (avg_run_time / total_queries_per_run) * 1000
    std_dev = statistics.stdev(timings) * 1000 if len(timings) > 1 else 0
    # Throughput (QPS) = Total queries / Total time
    qps = total_queries / total_time

    return {
        "Workload": f"{workload_func.__name__} ({num_threads} threads)",
        "DB Type": db_type,
        "Avg Latency (ms)": f"{avg_latency:.2f}",
        "Std Dev (ms)": f"{std_dev:.2f}",
        "Throughput (QPS)": f"{qps:.2f}"
    }


# ==============================================================================
# Step 6: Storage Measurement (Fields updated)
# ==============================================================================
def get_storage_usage(mysql_conn, mongo_client):
    storage_info = {}
    with mysql_conn.cursor() as cursor:
        cursor.execute("""
                       SELECT table_schema, SUM(data_length + index_length) / 1024 / 1024 AS 'Size (MB)'
                       FROM information_schema.TABLES
                       WHERE table_schema = %s
                         AND table_name = %s
                       GROUP BY table_schema;
                       """, (MYSQL_CONFIG['database'], MYSQL_TABLE))
        result = cursor.fetchone()
        storage_info["MySQL (MB)"] = f"{result[1]:.2f}" if result else "N/A"

    db = mongo_client[MONGO_DATABASE]
    try:
        # Use collection.stats() to get more precise info for a single collection
        stats = db.command('collStats', MONGO_COLLECTION)
        storage_info[
            "MongoDB (MB)"] = f"{(stats.get('storageSize', 0) + stats.get('totalIndexSize', 0)) / 1024 / 1024:.2f}"
    except pymongo.errors.OperationFailure:
        storage_info["MongoDB (MB)"] = "N/A (collection may be empty)"

    return storage_info


# ==============================================================================
# Step 7: Main Execution Logic (Refactored)
# ==============================================================================
if __name__ == "__main__":
    # --- 1. Connect to Databases ---
    try:
        mysql_conn = pymysql.connect(**MYSQL_CONFIG)
        print("\nSuccessfully connected to MySQL.")
    except pymysql.Error as e:
        print(f"Failed to connect to MySQL: {e}")
        sys.exit(1)

    try:
        mongo_client = pymongo.MongoClient(MONGO_URI)
        mongo_client.admin.command('ping')
        print("Successfully connected to MongoDB.")
    except pymongo.errors.ConnectionFailure as e:
        print(f"Failed to connect to MongoDB: {e}")
        sys.exit(1)

    # --- 2. Verify Environment ---
    setup_mysql(mysql_conn)
    setup_mongo(mongo_client)

    # --- 3. Fetch Sample IDs ---
    # We fetch IDs only once. Note: This assumes both DBs have the same 'ids'.
    # If they don't, you should fetch samples separately.
    # For this script, we'll use MySQL as the source of truth for IDs.
    sample_ids_for_tests = fetch_sample_ids(mysql_conn, "MySQL", IDS_TO_SAMPLE_FOR_TESTS)

    # --- 4. Define Concurrent Test Tasks ---
    concurrent_tests_to_run = [
        {"db_type": "MySQL", "workload_func": workload_mysql_read},
        {"db_type": "MySQL", "workload_func": workload_mysql_read_update},
        {"db_type": "MySQL", "workload_func": workload_mysql_insert},
        {"db_type": "MongoDB", "workload_func": workload_mongo_read},
        {"db_type": "MongoDB", "workload_func": workload_mongo_read_update},
        {"db_type": "MongoDB", "workload_func": workload_mongo_insert}
    ]

    results = []
    print("\nStarting all CONCURRENT benchmarks...")
    # 1. Create a progress bar with a total count of tasks
    with tqdm(total=len(concurrent_tests_to_run), desc="Overall Benchmark Progress") as pbar:
        for test_params in concurrent_tests_to_run:
            # 2. (Optional) Update text to show which task is currently running
            current_test_name = f"{test_params['db_type']} - {test_params['workload_func'].__name__}"
            pbar.set_description(f"Running: {current_test_name}")

            # 3. Execute the complete test task
            result = run_concurrent_benchmark(
                db_type=test_params["db_type"],
                workload_func=test_params["workload_func"],
                sample_ids=sample_ids_for_tests,
                queries_per_thread=QUERIES_PER_RUN,
                num_threads=NUM_THREADS,
                num_runs=NUM_RUNS,
                mongo_client=mongo_client
            )
            results.append(result)

            # 4. When one task is complete, advance the progress bar by one
            pbar.update(1)

    # --- 5. Format and Print Results ---
    results_df = pd.DataFrame(results)
    print("\n\n" + "=" * 80)
    print(f"{'Concurrent Benchmark Results (' + str(NUM_THREADS) + ' Threads)':^80}")
    print("=" * 80)
    print(results_df.to_string(index=False))
    print("=" * 80)

    # --- 6. Measure and Print Storage ---
    storage_results = get_storage_usage(mysql_conn, mongo_client)
    print("\n" + "=" * 80)
    print(f"{'Storage Usage':^80}")
    print("=" * 80)
    for db, size in storage_results.items():
        print(f"{db}: {size}")
    print("=" * 80)

    # --- 7. Clean Up Resources ---
    print
