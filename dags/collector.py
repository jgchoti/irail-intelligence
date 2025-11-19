from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import logging

default_args = {
    'owner': 'choti',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

STATIONS = [
    "Brussels-Central",
    "Antwerp-Central", 
    "Ghent-Sint-Pieters",
    "Liège-Guillemins"
]

def fetch_and_store_liveboard(station: str, **context):
    """Fetch liveboard data from iRail API and store in PostgreSQL"""
    url = f"https://api.irail.be/liveboard/?station={station}&format=json&fast=true"
    
    logging.info(f"Fetching data for {station}")
    
    try:
        # Fetch data from iRail API
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Parse response
        departures = data.get('departures', {}).get('departure', [])
        collected_at = datetime.fromtimestamp(int(data.get('timestamp', 0)))
        station_name = data.get('stationinfo', {}).get('standardname', station)
        
        if not departures:
            logging.warning(f"No departures found for {station}")
            return 0
        
        # Connect to PostgreSQL using Airflow's connection
        pg_hook = PostgresHook(postgres_conn_id='postgres_irail')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        inserted_count = 0
        
        for dep in departures:
            try:
                cursor.execute("""
                    INSERT INTO departures_raw 
                    (station, train_id, vehicle, platform, scheduled_time, 
                     delay_seconds, cancelled, destination, collected_at)
                    VALUES (%s, %s, %s, %s, to_timestamp(%s), %s, %s, %s, %s)
                """, (
                    station_name,
                    dep.get('vehicle', '').replace('BE.NMBS.', ''),
                    dep.get('vehicle', ''),
                    dep.get('platform', ''),
                    int(dep.get('time', 0)),
                    int(dep.get('delay', 0)),
                    int(dep.get('canceled', 0)) == 1,
                    dep.get('station', ''),
                    collected_at
                ))
                inserted_count += 1
            except Exception as e:
                # Skip duplicates silently
                logging.debug(f"Skipping record: {e}")
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"✅ {station_name}: Inserted {inserted_count}/{len(departures)} departures")
        
        # Store count in XCom for monitoring
        context['ti'].xcom_push(key='departures_count', value=inserted_count)
        
        return inserted_count
        
    except requests.RequestException as e:
        logging.error(f"❌ API request failed for {station}: {e}")
        raise
    except Exception as e:
        logging.error(f"❌ Unexpected error for {station}: {e}")
        raise

def log_collection_summary(**context):
    """Log summary of all collections"""
    ti = context['ti']
    
    total = 0
    for station in STATIONS:
        task_id = f'collect_{station.replace("-", "_").lower()}'
        count = ti.xcom_pull(task_ids=task_id, key='departures_count') or 0
        total += count
    
    logging.info(f"📊 Collection Summary: {total} total departures collected")
    return total

with DAG(
    'irail_data_collection',
    default_args=default_args,
    description='Collect train departure data from iRail API every 5 minutes',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    tags=['irail', 'data-collection', 'production'],
    max_active_runs=1, 
) as dag:

    # Create collection tasks for each station (run in parallel)
    collection_tasks = []
    
    for station in STATIONS:
        task = PythonOperator(
            task_id=f'collect_{station.replace("-", "_").lower()}',
            python_callable=fetch_and_store_liveboard,
            op_kwargs={'station': station},
            execution_timeout=timedelta(minutes=2),
        )
        collection_tasks.append(task)
    
    # Summary task (runs after all collections)
    summary_task = PythonOperator(
        task_id='log_summary',
        python_callable=log_collection_summary,
    )
    
    # Set dependencies
    collection_tasks >> summary_task