# Adzuna Data Pipeline - Airflow Project Structure

## Directory Structure

```
adzuna-airflow-project/
├── dags/
│   └── adzuna_pipeline_dag.py          # Main DAG file
├── spark_jobs/
│   ├── adzuna_stage_1.py               # Stage 1: Data ingestion
│   ├── adzuna_stage_2.py               # Stage 2: Geocoding  
│   ├── adzuna_stage_3.py               # Stage 3: Transformations
│   ├── adzuna_stage_4.py               # Stage 4: Bid generation
│   ├── campaigns.py                    # Campaign generation
│   ├── jobs.py                         # Job processing
│   └── clicks.py                       # Click data generation
├── config/
│   ├── requirements.txt                # Python dependencies
│   └── airflow.cfg                     # Airflow configuration
├── docker-compose.yml                  # Docker setup
└── README.md                           # Project documentation
```

## Setup Instructions

### 1. Environment Setup

1. **Create project directory:**
   ```bash
   mkdir adzuna-airflow-project
   cd adzuna-airflow-project
   mkdir dags spark_jobs config logs plugins
   ```

2. **Install dependencies:**
   ```bash
   pip install -r config/requirements.txt
   ```

### 2. Airflow Configuration

1. **Initialize Airflow database:**
   ```bash
   export AIRFLOW_HOME=$(pwd)
   airflow db init
   ```

2. **Create admin user:**
   ```bash
   airflow users create \
       --username admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com
   ```

3. **Configure connections in Airflow UI:**
   - **Spark Connection:** `spark_default`
   - **AWS Connection:** `aws_default`
   - **Create Pool:** `extraction_pool` with 2 slots

### 3. Spark Jobs Deployment

1. **Copy Spark job files to `/opt/airflow/spark_jobs/`**
2. **Make scripts executable:**
   ```bash
   chmod +x spark_jobs/*.py
   ```

### 4. AWS S3 Setup

Ensure your S3 bucket structure matches:
```
s3://naya-project-job-ads/
├── data/
│   ├── raw/adzuna/YYYY/MM/DD/           # Raw extracted data
│   ├── stages/adzuna/stage_X/YYYY/MM/DD/ # Processing stages
│   ├── final/adzuna/YYYY/MM/DD/         # Final processed data
│   ├── jobs/YYYY/MM/                    # Monthly job data
│   ├── campaigns/YYYY/MM/               # Campaign data
│   └── clicks/YYYY/MM/                  # Click data
```

### 5. Running the Pipeline

1. **Start Airflow services:**
   ```bash
   # Terminal 1 - Scheduler
   airflow scheduler
   
   # Terminal 2 - Webserver  
   airflow webserver --port 8080
   ```

2. **Access Airflow UI:** http://localhost:8080

3. **Enable the DAG:** Toggle the `adzuna_job_data_pipeline` DAG to ON

## Pipeline Overview

### Data Flow

1. **Extraction (Parallel):**
   - `extract_adzuna_set1`: Uses Matan's API credentials
   - `extract_adzuna_set2`: Uses Avital's API credentials

2. **Processing Stages (Sequential):**
   - `adzuna_stage_1`: Schema enforcement and data type casting
   - `adzuna_stage_2`: Geocoding missing locations via Nominatim API
   - `adzuna_stage_3`: Feature engineering and transformations
   - `adzuna_stage_4`: Bid generation with normal distribution

3. **Final Processing (Parallel):**
   - `generate_campaigns`: Create advertising campaigns by company/state
   - `process_jobs`: Deduplicate and prepare monthly job dataset

4. **Analytics Data:**
   - `generate_clicks`: Create synthetic click data for analysis

### Key Features

- **Error Handling:** Comprehensive retry logic and error reporting
- **Rate Limiting:** 30-second delays between API calls to respect limits
- **Incremental Processing:** Campaigns support incremental updates
- **Data Quality:** Deduplication and validation at each stage
- **Scalability:** Configurable Spark resources for different stages
- **Monitoring:** Detailed logging and progress tracking

### Scheduling

- **Default Schedule:** Daily (`@daily`)
- **Catchup:** Disabled to prevent historical backfills
- **Max Active Runs:** 1 to prevent resource conflicts

### Resource Allocation

- **Light tasks:** 1g driver, 2g executor memory
- **Heavy tasks:** 2g driver, 4g executor memory (stage 2, clicks)
- **Concurrent extractions:** Limited by `extraction_pool` (2 slots)

## Monitoring and Troubleshooting

### Common Issues

1. **API Rate Limits:** Increase sleep time between requests
2. **S3 Permissions:** Ensure proper IAM roles for S3 access
3. **Spark Memory:** Adjust executor/driver memory based on data size
4. **Geocoding Failures:** Nominatim API may be temporarily unavailable

### Logs Location

- **Airflow Logs:** `logs/` directory
- **Spark Logs:** Check Spark UI at http://localhost:4040
- **Application Logs:** Captured in task logs within Airflow UI

### Performance Tuning

- **Partitioning:** Data is partitioned by year/month/day
- **Caching:** Critical DataFrames are cached for reuse
- **Adaptive Query Execution:** Enabled for automatic optimization
- **Coalescing:** Reduces small file proliferation