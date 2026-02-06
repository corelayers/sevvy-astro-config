"""
## Crop Supply Signal Pipeline

This DAG aggregates satellite imagery and weather data to generate crop supply signals:
1. Reads satellite imagery data from satellite_imagery table
2. Reads weather station data from weather_data table
3. Reads historical crop yields from historical_crop_yields table
4. Aggregates data (imagery + weather - baseline yields) per farmland/crop
5. Writes aggregated data to aggregated_agricultural_data table
6. Joins with commodity reference data to filter tradable commodities
7. Writes final signal to crop_supply_signal_report table
8. Sends success notification webhook

The pipeline processes data for a specific business date and calculates
the estimated crop output signal for each farmland region and crop type.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging
import json

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests


# Threshold filter removed - include all aggregated values for anomaly detection


def send_success_webhook(context, records_processed, total_value, business_date):
    """
    Send success webhook notification to the monitoring endpoint
    
    Args:
        context: Airflow context containing DAG run and task instance details
        records_processed: Number of records written to database
        total_value: Total estimated crop output
        business_date: The business date for the calculation
    """
    print("🔔 Sending DAG success webhook notification")
    logging.info("Sending DAG success webhook notification")
    
    try:
        dag_run = context.get('dag_run')
        dag = context.get('dag')
        execution_date = context.get('execution_date') or context.get('logical_date')
        
        webhook_payload = {
            "event_type": "dag_success",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "dag_id": dag.dag_id,
            "task_id": "write_supply_signal_report",
            "run_id": dag_run.run_id,
            "organization_id": "cmfhfglh40k9h01rbszv8zhxx",
            "deployment_id": "cmfhfmdq70kb601rb5wu0d2nd",
            "dag_details": {
                "dag_id": dag.dag_id,
                "execution_date": execution_date.isoformat(),
                "state": "success",
                "log_url": f"https://cmfhfglh40k9h01rbszv8zhxx.astronomer.run/dwu0d2nd/dags/{dag.dag_id}/grid",
                "records_processed": records_processed,
                "total_estimated_crop_output": float(total_value),
                "business_date": business_date
            },
            "environment": "production"
        }
        
        print(f"📦 DAG Success Webhook payload: {json.dumps(webhook_payload, indent=2)}")
        
        response = requests.post(
            "https://sevvy-web-git-demo-sevvy.vercel.app/api/webhooks/airflow",
            json=webhook_payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if response.status_code == 200:
            print(f"✅ DAG Success webhook sent successfully: {response.status_code}")
            logging.info(f"DAG Success webhook sent successfully: {response.status_code}")
        else:
            print(f"⚠️ DAG Success webhook response: {response.status_code} - {response.text}")
            logging.warning(f"DAG Success webhook non-200 response: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"❌ Error sending DAG success webhook: {str(e)}")
        logging.error(f"Error sending DAG success webhook: {str(e)}")


default_args = {
    'owner': 'sevvy-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    'crop_supply_signal_pipeline',
    default_args=default_args,
    description='ETL pipeline for aggregating satellite and weather data to generate crop supply signals for trading',
    schedule='@daily',
    catchup=False,
    tags=['etl', 'trading', 'crop-supply', 'production'],
)
def crop_supply_signal_pipeline():
    """Main DAG definition using TaskFlow API"""
    
    @task()
    def read_satellite_imagery(**context) -> List[Dict[str, Any]]:
        """
        Read satellite imagery data from satellite_imagery table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🛰️ Reading satellite imagery for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT observation_date, farmland_id, crop_type, imagery_coverage_pct
            FROM public.satellite_imagery
            WHERE observation_date = %s
            ORDER BY farmland_id, crop_type
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        imagery = []
        for row in results:
            imagery.append({
                'observation_date': str(row[0]),
                'farmland_id': row[1],
                'crop_type': row[2],
                'imagery_coverage_pct': float(row[3])
            })
        
        print(f"✅ Found {len(imagery)} satellite imagery records for {business_date}")
        logging.info(f"Found {len(imagery)} satellite imagery records for {business_date}")
        
        return imagery
    
    @task()
    def read_weather_data(**context) -> List[Dict[str, Any]]:
        """
        Read weather station data from weather_data table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🌦️ Reading weather data for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT observation_date, farmland_id, crop_type, weather_favorability_index
            FROM public.weather_data
            WHERE observation_date = %s
            ORDER BY farmland_id, crop_type
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        weather = []
        for row in results:
            weather.append({
                'observation_date': str(row[0]),
                'farmland_id': row[1],
                'crop_type': row[2],
                'weather_favorability_index': float(row[3])
            })
        
        print(f"✅ Found {len(weather)} weather data records for {business_date}")
        logging.info(f"Found {len(weather)} weather data records for {business_date}")
        
        return weather
    
    @task()
    def read_historical_yields(**context) -> List[Dict[str, Any]]:
        """
        Read historical crop yields from historical_crop_yields table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📊 Reading historical crop yields for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT observation_date, farmland_id, crop_type, yield_tons
            FROM public.historical_crop_yields
            WHERE observation_date = %s
            ORDER BY farmland_id, crop_type
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        yields = []
        for row in results:
            yields.append({
                'observation_date': str(row[0]),
                'farmland_id': row[1],
                'crop_type': row[2],
                'yield_tons': float(row[3])
            })
        
        print(f"✅ Found {len(yields)} historical yield records for {business_date}")
        logging.info(f"Found {len(yields)} historical yield records for {business_date}")
        
        return yields
    
    @task()
    def agricultural_data_aggregation(
        imagery: List[Dict[str, Any]],
        weather: List[Dict[str, Any]],
        yields: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Aggregate agricultural data by calculating imagery + weather - baseline yields for each farmland.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🧮 Aggregating agricultural data for {business_date}")
        
        imagery_lookup = {}
        for img in imagery:
            key = (img['farmland_id'], img['crop_type'])
            if key not in imagery_lookup:
                imagery_lookup[key] = img['imagery_coverage_pct']
        
        weather_lookup = {}
        for wthr in weather:
            key = (wthr['farmland_id'], wthr['crop_type'])
            if key not in weather_lookup:
                weather_lookup[key] = wthr['weather_favorability_index']
        
        yield_lookup = {}
        for yld in yields:
            key = (yld['farmland_id'], yld['crop_type'])
            if key not in yield_lookup:
                yield_lookup[key] = yld['yield_tons']
        
        # Get all unique farmland/crop combinations
        all_keys = set(imagery_lookup.keys()) | set(weather_lookup.keys()) | set(yield_lookup.keys())
        
        aggregated = []
        for farmland_id, crop_type in all_keys:
            imagery_coverage_pct = imagery_lookup.get((farmland_id, crop_type), 0.0)
            weather_favorability_index = weather_lookup.get((farmland_id, crop_type), 0.0)
            yield_tons = yield_lookup.get((farmland_id, crop_type), 0.0)
            
            estimated_crop_output_tons = imagery_coverage_pct - weather_favorability_index - yield_tons
            
            aggregated.append({
                'observation_date': business_date,
                'farmland_id': farmland_id,
                'crop_type': crop_type,
                'estimated_crop_output_tons': estimated_crop_output_tons
            })
            
            print(f"  Farmland {farmland_id} ({crop_type}): "
                  f"${imagery_coverage_pct:,.2f} - ${weather_favorability_index:,.2f} - ${yield_tons:,.2f} = "
                  f"${estimated_crop_output_tons:,.2f}")
        
        print(f"✅ Aggregated {len(aggregated)} farmland records")
        logging.info(f"Aggregated {len(aggregated)} farmland records for {business_date}")
        
        return aggregated
    
    @task()
    def write_aggregated_agricultural_data(aggregated: List[Dict[str, Any]], **context):
        """
        Write aggregated agricultural data to aggregated_agricultural_data table
        """
        print(f"💾 Writing {len(aggregated)} records to aggregated_agricultural_data table")
        
        if not aggregated:
            print("⚠️  No aggregated records to write")
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.aggregated_agricultural_data 
                (observation_date, farmland_id, crop_type, estimated_crop_output_tons)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (observation_date, farmland_id, crop_type) DO UPDATE SET
                estimated_crop_output_tons = EXCLUDED.estimated_crop_output_tons
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in aggregated:
                cursor.execute(insert_query, (
                    record['observation_date'],
                    record['farmland_id'],
                    record['crop_type'],
                    record['estimated_crop_output_tons']
                ))
            
            conn.commit()
            print(f"✅ Successfully wrote {len(aggregated)} records to aggregated_agricultural_data")
            logging.info(f"Wrote {len(aggregated)} aggregated records")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to aggregated_agricultural_data: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    @task()
    def read_commodity_ref_data(**context) -> List[Dict[str, Any]]:
        """
        Read commodity reference data from commodity_ref_data table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📋 Reading commodity reference data for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT observation_date, farmland_id, crop_type, is_tradable
            FROM public.commodity_ref_data
            WHERE observation_date = %s
            ORDER BY farmland_id, crop_type
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        ref_data = []
        for row in results:
            ref_data.append({
                'observation_date': str(row[0]),
                'farmland_id': row[1],
                'crop_type': row[2],
                'is_tradable': row[3]
            })
        
        print(f"✅ Found {len(ref_data)} commodity reference records for {business_date}")
        logging.info(f"Found {len(ref_data)} commodity reference records for {business_date}")
        
        return ref_data
    
    @task()
    def read_aggregated_data(**context) -> List[Dict[str, Any]]:
        """
        Read aggregated data from aggregated_agricultural_data table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📊 Reading aggregated data for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT observation_date, farmland_id, crop_type, estimated_crop_output_tons
            FROM public.aggregated_agricultural_data
            WHERE observation_date = %s
            ORDER BY farmland_id, crop_type
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        aggregated = []
        for row in results:
            aggregated.append({
                'observation_date': str(row[0]),
                'farmland_id': row[1],
                'crop_type': row[2],
                'estimated_crop_output_tons': float(row[3])
            })
        
        print(f"✅ Found {len(aggregated)} aggregated records for {business_date}")
        logging.info(f"Found {len(aggregated)} aggregated records for {business_date}")
        
        return aggregated
    
    @task()
    def supply_signal_calculation(
        aggregated: List[Dict[str, Any]],
        ref_data: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Calculate crop supply signal by joining aggregated data with reference data
        and filtering for tradable commodities only.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🔒 Calculating crop supply signal for {business_date}")
        
        # Build lookup for tradable commodities
        tradable_lookup = {}
        for ref in ref_data:
            if ref['is_tradable']:
                key = (ref['farmland_id'], ref['crop_type'])
                tradable_lookup[key] = True
        
        # Filter aggregated data to only tradable commodities
        signal_records = []
        total_value = 0
        
        for record in aggregated:
            key = (record['farmland_id'], record['crop_type'])
            
            if key in tradable_lookup:
                signal_records.append({
                    'observation_date': record['observation_date'],
                    'farmland_id': record['farmland_id'],
                    'crop_type': record['crop_type'],
                    'estimated_crop_output_tons': record['estimated_crop_output_tons']
                })
                total_value += record['estimated_crop_output_tons']
                
                print(f"  Tradable: Farmland {record['farmland_id']} ({record['crop_type']}): "
                      f"${record['estimated_crop_output_tons']:,.2f}")
        
        print(f"✅ Found {len(signal_records)} tradable commodity records")
        print(f"   Total supply signal value: ${total_value:,.2f}")
        logging.info(f"Found {len(signal_records)} tradable records with total value: {total_value}")
        
        return signal_records
    
    @task()
    def write_supply_signal_report(signal_records: List[Dict[str, Any]], **context):
        """
        Write final supply signal report to crop_supply_signal_report table
        """
        print(f"💾 Writing {len(signal_records)} records to crop_supply_signal_report table")
        
        records_written = 0
        total_value = 0
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        if not signal_records:
            print("⚠️  No signal records to write")
            send_success_webhook(context, records_written, total_value, business_date)
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.crop_supply_signal_report 
                (observation_date, farmland_id, crop_type, estimated_crop_output_tons)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (observation_date, farmland_id, crop_type) DO UPDATE SET
                estimated_crop_output_tons = EXCLUDED.estimated_crop_output_tons
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in signal_records:
                cursor.execute(insert_query, (
                    record['observation_date'],
                    record['farmland_id'],
                    record['crop_type'],
                    record['estimated_crop_output_tons']
                ))
                
                records_written += 1
                total_value += record['estimated_crop_output_tons']
            
            conn.commit()
            
            print(f"✅ Successfully wrote {records_written} records to crop_supply_signal_report")
            logging.info(f"Wrote {records_written} supply signal records with total value: {total_value}")
            
            context['ti'].xcom_push(key='records_processed', value=records_written)
            context['ti'].xcom_push(key='total_value', value=total_value)
            context['ti'].xcom_push(key='business_date', value=business_date)
            
            send_success_webhook(context, records_written, total_value, business_date)
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to crop_supply_signal_report: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    # Phase 1: Read upstream data in parallel
    imagery = read_satellite_imagery()
    weather = read_weather_data()
    yields = read_historical_yields()
    
    # Phase 2: Aggregate agricultural data
    aggregated = agricultural_data_aggregation(imagery, weather, yields)
    
    # Phase 3: Write aggregated data
    write_aggregated = write_aggregated_agricultural_data(aggregated)
    
    # Phase 4: Read data for supply signal calculation (after write completes)
    aggregated_data = read_aggregated_data()
    ref_data = read_commodity_ref_data()
    
    # Set dependencies - aggregated read must wait for write
    write_aggregated >> aggregated_data
    
    # Phase 5: Calculate supply signal (join with ref data, filter tradable)
    supply_signal = supply_signal_calculation(aggregated_data, ref_data)
    
    # Phase 6: Write final report
    write_supply_signal_report(supply_signal)


dag = crop_supply_signal_pipeline()