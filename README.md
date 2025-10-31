# mysql-mongo-benchmark
# MySQL vs. MongoDB Performance Benchmark

A Python-based benchmarking tool to compare the performance of MySQL (SQL) and MongoDB (NoSQL) under concurrent, mixed workloads. This script measures latency and throughput (QPS) for Read, Read/Update, and Insert operations using multithreading.

## 🚀 Features

* **Concurrent Testing:** Uses Python's `threading` module to simulate multiple concurrent users.
* **Multiple Workloads:** Tests three distinct scenarios:
    * **Read-Only:** 100% read operations.
    * **Read-Update (Mixed):** 95% read operations and 5% update operations.
    * **Insert-Only:** 100% insert operations (uses temporary tables/collections for clean testing).
* **Dynamic Sampling:** Fetches a large, random sample of real IDs from the database to use in read/update tests, ensuring realistic query patterns.
* **Storage Measurement:** Reports the total disk space (data + indexes) used by both databases.
* **Easy Configuration:** Key parameters (threads, queries, runs) are configurable at the top of the script.

## 🛠️ Technologies Used

* Python 3
* `pymysql` (for MySQL)
* `pymongo` (for MongoDB)
* `pandas` (for results formatting)
* `tqdm` (for progress bars)

## 📋 Prerequisites

* Python 3.x
* A running MySQL Server (configured for `localhost`)
* A running MongoDB Server (configured for `localhost:27017`)

## ⚙️ Setup

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/your-username/mysql-vs-mongo-benchmark.git](https://github.com/your-username/mysql-vs-mongo-benchmark.git)
    cd mysql-vs-mongo-benchmark
    ```

2.  **Install dependencies:**
    It is highly recommended to use a virtual environment.
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

    *(Create a `requirements.txt` file with the following content:)*
    ```
    pymysql
    pymongo
    pandas
    tqdm
    ```

## 🗄️ Database Preparation (Crucial Step)

This script **benchmarks existing data**; it does not generate test data. You must pre-populate both databases.

1.  **MySQL:**
    * Ensure your server is running.
    * Create the database specified in `MYSQL_CONFIG` (default: `test`).
    * `CREATE DATABASE test;`
    * Load your data (e.g., from a CSV) into the `tweets` table (or the table specified in `MYSQL_TABLE`). The script will create the table structure if it doesn't exist, but it **requires data** to fetch sample IDs.
    * The schema should match: `(target INT, ids BIGINT PRIMARY KEY, date VARCHAR(255), flag VARCHAR(255), user VARCHAR(255), text TEXT)`.

2.  **MongoDB:**
    * Ensure your server is running.
    * Import your data into the database and collection specified in `MONGO_DATABASE` and `MONGO_COLLECTION` (default: `Test.User_comment`).
    * The documents **must** contain an `ids` field that corresponds to the `ids` in the MySQL table for the read/update tests to be valid.
    * The script will automatically create the necessary index on the `ids` field.

## 🔧 Configuration

Open `benchmark.py` and edit the following sections as needed.

1.  **Database Connections:**
    * Update `MYSQL_CONFIG` and `MONGO_URI` to match your database credentials and addresses.

2.  **Test Parameters:**
    * `IDS_TO_SAMPLE_FOR_TESTS`: Number of IDs to fetch for test sampling. **Must be >= (QUERIES_PER_RUN * NUM_THREADS)**.
    * `QUERIES_PER_RUN`: Operations each thread will perform in one run.
    * `NUM_RUNS`: Number of times to repeat the test for statistical accuracy.
    * `NUM_THREADS`: The concurrency level (how many threads to run simultaneously).


## ▶️ How to Run

Once your databases are populated and the script is configured, simply run:

```bash
python benchmark.py
```

**The script will:**
* Connect to both databases.
* Verify the table/collection setup.
* Fetch the sample IDs.
* Run all six benchmarks (3 workloads x 2 databases) sequentially, showing progress.
* Print the final results table.
* Print the storage usage report. 


## 📊 Example Output
```bash
Successfully connected to MySQL.
Successfully connected to MongoDB.

Verifying MySQL environment...
MySQL table 'tweets' is ready.

Verifying MongoDB environment...
Checking indexes for collection 'User_comment'...
Index 'ids_1' already exists and is correctly configured.
MongoDB collection 'User_comment' is ready.

Fetching 500000 sample IDs from MySQL...
Successfully fetched 500000 IDs.

Starting all CONCURRENT benchmarks...
Overall Benchmark Progress: 100%|██████████| 6/6 [01:30<00:00, 15.00s/it]


================================================================================
                       Concurrent Benchmark Results (16 Threads)
================================================================================
                                    Workload   DB Type  Avg Latency (ms)  Std Dev (ms)  Throughput (QPS)
              workload_mysql_read (16 threads)   MySQL             10.50          2.10           1523.80
       workload_mysql_read_update (16 threads)   MySQL             12.80          3.40           1250.10
            workload_mysql_insert (16 threads)   MySQL             25.15         10.20            636.18
               workload_mongo_read (16 threads) MongoDB              2.10          0.50           7619.05
        workload_mongo_read_update (16 threads) MongoDB              2.50          0.75           6400.00
             workload_mongo_insert (16 threads) MongoDB              5.30          1.20           3018.87
================================================================================

================================================================================
                                 Storage Usage
================================================================================
MySQL (MB): 150.45
MongoDB (MB): 120.80
================================================================================

Closing database connections.
```
