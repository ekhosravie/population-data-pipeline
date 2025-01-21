
#### 3. **Airflow DAG: population_pipeline.py**

This is the main Airflow file where the DAG is defined.

```python
import pandas as pd
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.dates import datetime
import logging

# Data extraction function
def extract_data():
    url = "https://data.un.org/_Docs/SYB/CSV/SYB67_1_202411_Population,%20Surface%20Area%20and%20Density.csv"
    df = pd.read_csv(url)
    df.to_csv('/tmp/population_data.csv', index=False)
    logging.info("Data extracted successfully")
    return '/tmp/population_data.csv'

# Data transformation function
def transform_data(file_path):
    # Load the data
    df = pd.read_csv(file_path)
    
    # Data cleaning and transformation
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    # Calculate percentage of population aged 60+
    df['population_60_plus_percent'] = df['population_60_plus'] / df['total_population'] * 100
    
    # Log the transformation details
    logging.info("Transformation completed")
    
    return df

# Data loading function
def load_data_to_postgres(df):
    # Establish database connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    
    # Create table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS population (
        year INT,
        country VARCHAR(100),
        population INT,
        population_60_plus INT,
        population_60_plus_percent DECIMAL
    );
    """)
    
    # Insert data into table
    for row in df.itertuples():
        cursor.execute("""
        INSERT INTO population (year, country, population, population_60_plus, population_60_plus_percent)
        VALUES (%s, %s, %s, %s, %s);
        """, (row.year, row.country, row.population, row.population_60_plus, row.population_60_plus_percent))
    
    connection.commit()
    logging.info("Data loaded to PostgreSQL successfully")

# DAG Definition
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'population_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline for population data',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_args=['/tmp/population_data.csv'],
    )

    load_task = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_postgres,
        op_args=[transform_task.output],
    )

    # Define task dependencies
    extract_task >> transform_task >> load_task
