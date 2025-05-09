# Real-time Traffic Monitoring & Analysis Pipeline (End-to-End Demo)

## 1. Project Overview

This project demonstrates the construction and deployment of an end-to-end system for simulating, processing, and visualizing real-time traffic data. The primary goal is to build a complete pipeline showcasing common technologies and architectures used in Data Engineering to address traffic monitoring challenges.

**Key functionalities include:**

* **Simulation & Ingestion:** Generating mock vehicle GPS data and ingesting it into a Kafka message queue.
* **Stream Processing:** Utilizing Spark Structured Streaming to process the continuous data stream.
* **Map Matching:** Identifying the specific road segment a vehicle is traversing based on its GPS coordinates.
* **Calculation & Analysis:** Aggregating data over time windows to compute average speed, vehicle counts, and classifying traffic status (clear, slow, congested).
* **Diverse Storage:** Persisting processed results into multiple specialized storage systems:
    * **Redis:** For low-latency access to the latest segment status, feeding a real-time dashboard.
    * **ClickHouse:** An OLAP database for storing historical aggregated data suitable for trend analysis and reporting.
    * **MinIO (S3):** An object store acting as a data lake, archiving processed data in Parquet format for long-term storage and ad-hoc querying.
* **Real-time Visualization:** Displaying traffic status on a Leaflet.js map via a web interface, automatically updated using WebSockets.
* **Containerization & Orchestration:** Packaging all infrastructure components (Kafka, Spark/Airflow, Databases, etc.) using Docker and Docker Compose; orchestrating the Spark job execution with Airflow.

This project serves as a practical example of integrating Kafka, Spark, Redis, ClickHouse, MinIO, Airflow, and Docker to build a robust streaming data solution.

## 2. System Architecture

The primary data flow through the system is as follows:

1.  **Producer (`producer.py`):** Runs continuously, generating mock GPS data (vehicle ID, timestamp, lat, lon, speed). Serializes this data into Avro format using a schema retrieved from/registered with Schema Registry and produces it to the `raw_traffic_data` Kafka topic.
2.  **Kafka Cluster (KRaft):** Receives and durably stores the Avro messages, making them available for consumers. Data is replicated across brokers for fault tolerance.
3.  **Schema Registry:** Centrally stores and manages versions of the Avro schema used by the producer and consumer.
4.  **Airflow (`traffic_data_pipeline` DAG):** When triggered (manually), the `submit_traffic_processor_spark_job` task executes the `spark-submit` command within the Airflow environment.
5.  **Spark Structured Streaming (`processor.py`):**
    * The Spark application starts, running in client mode with a local master (`local[*]`) inside the Airflow container.
    * **Read:** Connects to Kafka brokers and consumes Avro messages from the `raw_traffic_data` topic in micro-batches.
    * **Deserialize:** Uses the Avro schema (read from the `schemas/` directory) to deserialize the binary messages into structured Spark DataFrame columns.
    * **Process:**
        * Cleans data (casting types, handling nulls).
        * **Map Matching:** Applies a Pandas UDF (`find_nearest_segment_pandas_udf`) which uses broadcasted segment coordinate data (`SEGMENTS_COORDS`) and a Haversine function to assign a `segment_id` to each record.
        * **Aggregate:** Groups data by `segment_id` and a tumbling time window, calculating `avg_speed` and `vehicle_count`.
        * **Classify:** Applies a standard UDF (`classify_status_udf`) to determine the traffic `status` based on `avg_speed`.
    * **Sink (`foreachBatch`):** For each processed micro-batch DataFrame (`status_df`):
        * Writes the latest segment state to **Redis**.
        * Appends the aggregated batch data to the **ClickHouse** `traffic_events` table.
        * Writes the batch data as partitioned Parquet files to **MinIO** (S3).
    * **Checkpointing:** Spark periodically saves Kafka offsets and aggregation state to the configured checkpoint location on MinIO to ensure fault tolerance.
6.  **WebSocket Server (`websocket_server/server.py`):** Runs independently, periodically querying Redis for the latest segment statuses.
7.  **Frontend (`frontend/app.js`):** Connects to the WebSocket server. Upon receiving updates, it dynamically changes the color of road segments on the Leaflet map and updates information panels.

*(Recommendation: Add an architecture diagram image here)*
`![Architecture Diagram](docs/architecture.png)`

## 3. Technology Stack

* **Orchestration:** Apache Airflow 2.8.1
* **Stream Processing:** Apache Spark 3.3.4 (Structured Streaming, PySpark)
* **Message Broker:** Apache Kafka (KRaft mode)
* **Schema Management:** Confluent Schema Registry
* **Data Serialization:** Apache Avro
* **Real-time Cache:** Redis
* **Data Warehouse/OLAP:** ClickHouse
* **Data Lake Storage:** MinIO (S3 Compatible)
* **Containerization:** Docker, Docker Compose
* **Backend (WebSocket):** Python, Flask, Flask-SocketIO, redis-py
* **Frontend:** HTML, CSS, JavaScript, Leaflet.js
* **Primary Language:** Python 3.10
* **Other:** Pandas UDFs, Git

## 4. Project Structure
Okay, here is the updated, detailed README.md in English, incorporating the instructions for using a Python virtual environment for the producer script.

Create the file README.md in your project's root directory (E:\project_data\computer_graphic_data) and paste this content. Remember to create the producer/requirements.txt file as well.

Markdown

# Real-time Traffic Monitoring & Analysis Pipeline (End-to-End Demo)

## 1. Project Overview

This project demonstrates the construction and deployment of an end-to-end system for simulating, processing, and visualizing real-time traffic data. The primary goal is to build a complete pipeline showcasing common technologies and architectures used in Data Engineering to address traffic monitoring challenges.

**Key functionalities include:**

* **Simulation & Ingestion:** Generating mock vehicle GPS data and ingesting it into a Kafka message queue.
* **Stream Processing:** Utilizing Spark Structured Streaming to process the continuous data stream.
* **Map Matching:** Identifying the specific road segment a vehicle is traversing based on its GPS coordinates.
* **Calculation & Analysis:** Aggregating data over time windows to compute average speed, vehicle counts, and classifying traffic status (clear, slow, congested).
* **Diverse Storage:** Persisting processed results into multiple specialized storage systems:
    * **Redis:** For low-latency access to the latest segment status, feeding a real-time dashboard.
    * **ClickHouse:** An OLAP database for storing historical aggregated data suitable for trend analysis and reporting.
    * **MinIO (S3):** An object store acting as a data lake, archiving processed data in Parquet format for long-term storage and ad-hoc querying.
* **Real-time Visualization:** Displaying traffic status on a Leaflet.js map via a web interface, automatically updated using WebSockets.
* **Containerization & Orchestration:** Packaging all infrastructure components (Kafka, Spark/Airflow, Databases, etc.) using Docker and Docker Compose; orchestrating the Spark job execution with Airflow.

This project serves as a practical example of integrating Kafka, Spark, Redis, ClickHouse, MinIO, Airflow, and Docker to build a robust streaming data solution.

## 2. System Architecture

The primary data flow through the system is as follows:

1.  **Producer (`producer.py`):** Runs continuously, generating mock GPS data (vehicle ID, timestamp, lat, lon, speed). Serializes this data into Avro format using a schema retrieved from/registered with Schema Registry and produces it to the `raw_traffic_data` Kafka topic.
2.  **Kafka Cluster (KRaft):** Receives and durably stores the Avro messages, making them available for consumers. Data is replicated across brokers for fault tolerance.
3.  **Schema Registry:** Centrally stores and manages versions of the Avro schema used by the producer and consumer.
4.  **Airflow (`traffic_data_pipeline` DAG):** When triggered (manually), the `submit_traffic_processor_spark_job` task executes the `spark-submit` command within the Airflow environment.
5.  **Spark Structured Streaming (`processor.py`):**
    * The Spark application starts, running in client mode with a local master (`local[*]`) inside the Airflow container.
    * **Read:** Connects to Kafka brokers and consumes Avro messages from the `raw_traffic_data` topic in micro-batches.
    * **Deserialize:** Uses the Avro schema (read from the `schemas/` directory) to deserialize the binary messages into structured Spark DataFrame columns.
    * **Process:**
        * Cleans data (casting types, handling nulls).
        * **Map Matching:** Applies a Pandas UDF (`find_nearest_segment_pandas_udf`) which uses broadcasted segment coordinate data (`SEGMENTS_COORDS`) and a Haversine function to assign a `segment_id` to each record.
        * **Aggregate:** Groups data by `segment_id` and a tumbling time window, calculating `avg_speed` and `vehicle_count`.
        * **Classify:** Applies a standard UDF (`classify_status_udf`) to determine the traffic `status` based on `avg_speed`.
    * **Sink (`foreachBatch`):** For each processed micro-batch DataFrame (`status_df`):
        * Writes the latest segment state to **Redis**.
        * Appends the aggregated batch data to the **ClickHouse** `traffic_events` table.
        * Writes the batch data as partitioned Parquet files to **MinIO** (S3).
    * **Checkpointing:** Spark periodically saves Kafka offsets and aggregation state to the configured checkpoint location on MinIO to ensure fault tolerance.
6.  **WebSocket Server (`websocket_server/server.py`):** Runs independently, periodically querying Redis for the latest segment statuses.
7.  **Frontend (`frontend/app.js`):** Connects to the WebSocket server. Upon receiving updates, it dynamically changes the color of road segments on the Leaflet map and updates information panels.

*(Recommendation: Add an architecture diagram image here)*
`![Architecture Diagram](docs/architecture.png)`

## 3. Technology Stack

* **Orchestration:** Apache Airflow 2.8.1
* **Stream Processing:** Apache Spark 3.3.4 (Structured Streaming, PySpark)
* **Message Broker:** Apache Kafka (KRaft mode)
* **Schema Management:** Confluent Schema Registry
* **Data Serialization:** Apache Avro
* **Real-time Cache:** Redis
* **Data Warehouse/OLAP:** ClickHouse
* **Data Lake Storage:** MinIO (S3 Compatible)
* **Containerization:** Docker, Docker Compose
* **Backend (WebSocket):** Python, Flask, Flask-SocketIO, redis-py
* **Frontend:** HTML, CSS, JavaScript, Leaflet.js
* **Primary Language:** Python 3.10
* **Other:** Pandas UDFs, Git

## 4. Project Structure

.
├── airflow/                  # Airflow config and Dockerfile
│   ├── Dockerfile
│   └── requirements.txt
├── dags/                     # Airflow DAGs
│   └── traffic_pipeline_dag.py
├── producer/                 # Data Producer script
│   ├── producer.py
│   └── requirements.txt  # <-- Producer dependencies
├── spark_processor/          # Spark Streaming application
│   └── app/
│       ├── processor.py      # Main processing script
│       └── utils.py          # Utility functions (e.g., haversine)
├── frontend/                 # Web User Interface
│   ├── index.html
│   ├── style.css
│   └── app.js
├── schemas/                  # Avro schema definitions
│   └── raw_traffic_event.avsc
├── scripts/                  # Utility scripts (e.g., Kafka setup)
│   └── setup_kafka_topics.sh
├── .env                      # (Needs to be created manually) Environment variables
├── .gitignore                # Files/directories ignored by Git
├── docker-compose.yml        # Docker Compose service definitions
└── README.md                 # This README file


## 5. Setup and Running Instructions

**Prerequisites:**

* Docker Engine (Latest stable version recommended)
* Docker Compose (Usually included with Docker Desktop)
* Git (for cloning the repository)
* Python 3.10 (or 3.9, matching Airflow base image) installed on your **host machine** (for running the producer locally).
* A Text Editor (VS Code recommended for features like line ending conversion).
* Web Browser

**Installation Steps:**

1.  **Clone the Repository:**
    * Open your terminal (Command Prompt, PowerShell, Git Bash, or Linux/Mac Terminal).
    * Navigate to the directory where you want to store the project.
    * Run:
      ```bash
      git clone <your_github_repo_url>
      cd <your_project_directory_name>
      ```

2.  **Configure Environment (`.env` file):**
    * **Purpose:** Sets local environment variables, primarily the Airflow User ID to prevent potential file permission issues inside the container.
    * **Create File:** Create a new file named `.env` in the project's root directory (same level as `docker-compose.yml`).
    * **Add Content:** Add the following line to the `.env` file:
      ```env
      AIRFLOW_UID=50000
      ```
    * **Note:**
        * On **Windows or Mac (using Docker Desktop)**, `50000` is generally a safe value.
        * On **Linux:** It's recommended to use your actual user ID. Run `id -u` in your Linux terminal and replace `50000` with the output (e.g., `AIRFLOW_UID=1000`).

3.  **Build Docker Images:**
    * **Purpose:** Builds the custom Airflow image which includes Spark, necessary JARs, and Python dependencies from `airflow/requirements.txt`.
    * **Command:** Run this in the project root directory:
      ```bash
      docker-compose build
      ```
    * **Note:** This might take several minutes on the first run. Subsequent builds will be faster due to Docker's caching. If errors occur, check your internet connection and the build output log carefully.

4.  **Start All Services:**
    * **Purpose:** Launches all containers defined in `docker-compose.yml` (Kafka, Schema Registry, Airflow components, Redis, ClickHouse, MinIO, WebSocket Server) in detached mode.
    * **Command:**
      ```bash
      docker-compose up -d
      ```
    * **Verify:** Run `docker-compose ps` to check if all containers are listed as `Up` or `Running`. Allow 1-2 minutes for services, especially Airflow, to fully initialize.

5.  **Initialize System Components (Run Once):**
    * **a) Create Kafka Topic:**
        * **Purpose:** Creates the necessary Kafka topic for the producer to send data to.
        * **Option 1 (Script):** If using a bash-compatible shell (Linux, Mac, Git Bash):
            ```bash
            bash scripts/setup_kafka_topics.sh
            ```
        * **Option 2 (Manual):** Execute the command directly:
            ```bash
            docker exec -it kafk-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic raw_traffic_data --partitions 3 --replication-factor 3
            ```
    * **b) Create ClickHouse Database & Table:**
        * **Purpose:** Sets up the database and table in ClickHouse to store historical aggregated data.
        * **Step 1:** Connect to the ClickHouse client inside the container:
            ```bash
            docker exec -it clickhouse-server clickhouse-client
            ```
        * **Step 2:** Execute the following SQL commands within the client:
            ```sql
            CREATE DATABASE IF NOT EXISTS traffic_db;

            CREATE TABLE IF NOT EXISTS traffic_db.traffic_events
            (
                `segment_id` String,
                `window_start` DateTime64(3), -- Assumes UTC timestamps from Spark
                `window_end` DateTime64(3),
                `avg_speed` Float32,
                `vehicle_count` UInt32,
                `status` LowCardinality(String),
                `processing_time` DateTime DEFAULT now()
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(window_start)
            ORDER BY (segment_id, window_start);
            ```
        * **Step 3:** Exit the client: `exit;`

    * **c) Create MinIO Bucket:**
        * **Purpose:** Creates the storage bucket for the data lake (Parquet files) and Spark checkpoints.
        * **Step 1:** Access the MinIO Console in your browser: `http://localhost:9001`.
        * **Step 2:** Log in using `minioadmin` / `minioadmin`.
        * **Step 3:** Click the "Create Bucket" button.
        * **Step 4:** Enter the bucket name: `traffic-data`.
        * **Step 5:** Click "Create Bucket".

**Running the Pipeline:**

1.  **Setup and Run the Data Producer:**
    * **a) Navigate to Producer Directory:** Open a **new, separate terminal** for the producer.
        ```bash
        cd path/to/your/project/producer
        ```
    * **b) Create Python Virtual Environment:** (Only required once)
        ```bash
        python -m venv .venv
        ```
    * **c) Activate Virtual Environment:**
        * **Windows (cmd.exe):** `.\.venv\Scripts\activate`
        * **Windows (PowerShell):** `.\.venv\Scripts\Activate.ps1` (May require adjusting execution policy: `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process`)
        * **Linux/Mac/Git Bash:** `source .venv/bin/activate`
        *(Your terminal prompt should now start with `(.venv)`)*
    * **d) Install Producer Dependencies:** (Run once, or when `requirements.txt` changes)
        ```bash
        pip install -r requirements.txt
        ```
    * **e) Run the Producer:** (Ensure virtual environment is active)
        ```bash
        python producer.py
        ```
    * **Leave this terminal running.** It simulates the continuous flow of data. Check for any errors (like Kafka timeouts).

2.  **Trigger the Airflow DAG:**
    * Go to the Airflow UI (`http://localhost:8080`).
    * Find the `traffic_data_pipeline` DAG.
    * Toggle the DAG to **ON** (the switch on the left).
    * Click the **Trigger DAG** button (looks like a play symbol ▶️) under the "Actions" column.
    * Monitor the task `submit_traffic_processor_spark_job`; it should change to `running`.

3.  **View the Frontend Dashboard:**
    * Open **another new terminal**.
    * Navigate to the frontend directory:
      ```bash
      cd path/to/your/project/frontend
      ```
    * Start the simple Python HTTP server on **port 8000**:
      ```bash
      python -m http.server 8000
      ```
    * Open your web browser and navigate to `http://localhost:8000`.
    * Observe the map and dashboard panels. Updates should start appearing after the first few Spark micro-batches containing data have been processed (this might take a minute or two depending on your trigger interval).

**System is now running!** Data flows from producer -> Kafka -> Spark (via Airflow) -> Redis/ClickHouse/MinIO -> WebSocket -> Frontend. To stop everything, run `docker-compose down` in the project root directory.

## 7. Monitoring and Verification (Detailed)

* **Airflow UI (`http://localhost:8080`):** Check DAG Run status. Click on the `submit_traffic_processor_spark_job` task instance -> "Log" to see the full `spark-submit` output, including Spark logs and any `print` statements from `processor.py`.
* **Spark UI (Usually `http://<container_ip>:4040`):** Find the link within the Airflow task log. Monitor running/completed jobs, stages, tasks, batch processing times (under the "Streaming" tab). Check for failed tasks/stages and associated error messages.
* **Kafka Topic (`raw_traffic_data`):** Use the console consumer to verify messages are arriving in real-time from the producer:
    ```bash
    docker exec -it kafk-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic raw_traffic_data
    ```
* **Redis (`redis:6379`):** Check real-time state:
    ```bash
    docker exec -it redis redis-cli
    keys segment:*
    get segment:segment_1 
    # (Type 'exit' to quit redis-cli)
    ```
* **ClickHouse (`clickhouse-server:8123`):** Query historical data:
    ```bash
    docker exec -it clickhouse-server clickhouse-client
    USE traffic_db;
    SELECT count() FROM traffic_events;
    SELECT * FROM traffic_events ORDER BY window_end DESC LIMIT 10;
    # (Type 'exit;' to quit)
    ```
* **MinIO (`http://localhost:9001`):** Browse the `traffic-data` bucket. Check the checkpoint directory (`checkpoints_airflow/...`) for `state`, `commits`, `offsets` folders/files being updated. Check the output directory (`processed_traffic_partitioned/...`) for new Parquet files appearing within partition folders.
* **WebSocket Server Logs:** Check for connection errors or issues reading from Redis:
    ```bash
    docker-compose logs websocket-server
    ```
* **Frontend Console (Browser F12):** Look for JavaScript errors or issues connecting to the WebSocket server (`ws://localhost:8888/ws/traffic`). Check the "Network" tab (filter by "WS") to see WebSocket messages.

## 8. Configuration Notes

* Key connection details (Kafka brokers, MinIO credentials, etc.) are sourced from environment variables defined in `docker-compose.yml`.
* Spark configurations (S3A settings, Avro SR URL) are passed via `--conf` flags in the `spark-submit` command within the Airflow DAG.
* Spark Streaming specific configurations (`checkpointLocation`, `trigger`) are set within the `processor.py` script.
* Avro schema is loaded from a file mounted into the container.

## 9. Common Troubleshooting Tips

* **Port Conflicts:** Check `docker ps` and host system tools to ensure ports (8080, 9001, 9092, 29092, 8888, 8123, etc.) are not already in use before running `docker-compose up`.
* **Line Endings (CRLF vs LF):** Ensure `.py` and `.sh` files use LF (Unix) line endings, especially if edited on Windows. Use VS Code or `dos2unix` to convert. **Remember to save files after converting.**
* **Dependency Issues (`ModuleNotFoundError`, `ImportError`):** Verify `airflow/requirements.txt` and `producer/requirements.txt` are correct. Ensure `docker-compose build` completes without pip errors. Check installation inside the container with `docker exec ... pip show ...`. Ensure `PYTHONPATH` and `PYSPARK_PYTHON` are correctly set.
* **Kafka/Producer Issues:** Use `kafka-console-consumer` to confirm data arrival in the topic. Check logs for both the producer script and the Kafka brokers (`docker-compose logs kafk-1` etc.) for connection errors or timeouts. Ensure broker addresses/ports are correct.
* **Checkpointing/State Store Issues (S3A/MinIO):** Verify MinIO credentials, endpoint, and bucket permissions. Check Spark logs for `S3AFileSystem` or `AmazonS3Exception` errors. Ensure necessary Hadoop/AWS JARs are present in `$SPARK_HOME/jars`. Consider testing with a local filesystem checkpoint (`/tmp/...`) to isolate S3A problems. Look for `HDFSBackedStateStoreProvider` warnings related to reading state.
* **Spark Performance/Resource Issues (`Falling behind`, OOM):** Increase `--driver-memory` in `spark-submit`. Increase the streaming trigger interval (`.trigger(...)`). Limit Kafka input rate (`maxOffsetsPerTrigger`). Monitor container resources with `docker stats`.

## 10. Contributing

*(Placeholder - Add guidelines if desired)*
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## 11. License

*(Placeholder - Example: MIT)*
[MIT](https://choosealicense.com/licenses/mit/)