# requirements.txt
# Add this to your Airflow environment requirements
apache-airflow==2.7.0
apache-airflow-providers-apache-spark==4.1.3
apache-airflow-providers-amazon==8.4.0
pyspark==3.4.1
boto3==1.28.57
requests==2.31.0

# ================================
# docker-compose.yml additions
# ================================
# Add these volumes to your Airflow docker-compose.yml:
#
# volumes:
#   - ./spark_jobs:/opt/airflow/spark_jobs
#   - ./plugins:/opt/airflow/plugins
#
# And add these environment variables:
# environment:
#   - SPARK_HOME=/opt/spark
#   - PYTHONPATH=/opt/airflow/spark_jobs

# ================================
# airflow.cfg additions  
# ================================
# Add these configurations to your airflow.cfg:
#
# [core]
# dags_folder = /opt/airflow/dags
# plugins_folder = /opt/airflow/plugins
#
# [webserver]
# expose_config = True
#
# [scheduler] 
# catchup_by_default = False

# ================================
# Spark Connection Setup
# ================================
# In Airflow UI, create a connection with:
# Connection Id: spark_default
# Connection Type: Spark
# Host: spark://spark-master
# Port: 7077
# Extra: {"queue": "default"}

# ================================
# AWS Connection Setup  
# ================================
# In Airflow UI, create a connection with:
# Connection Id: aws_default
# Connection Type: Amazon Web Services
# AWS Access Key ID: <your-access-key>
# AWS Secret Access Key: <your-secret-key>
# Extra: {"region_name": "us-east-1"}

# ================================
# Pools Configuration
# ================================
# Create these pools in Airflow UI:
# Pool Name: extraction_pool
# Slots: 2
# Description: Pool for API extraction tasks to control concurrency