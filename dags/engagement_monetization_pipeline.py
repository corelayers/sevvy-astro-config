"""
## Engagement & Monetization Consolidation Pipeline

This DAG consolidates playback and ad-impression events and generates reportable engagement and monetization dashboards:
1. Reads playback events from playback_events table
2. Reads ad impression events from ad_impression_events table
3. Reads platform dimension mappings from platform_dimension_mappings table
4. Consolidates event streams (playback + impressions + platform dimensions) per content/reporting bucket
5. Writes consolidated data to consolidated_engagement_events table
6. Joins with content metadata to filter reportable content
7. Writes final dashboard data to engagement_monetization_dashboard table
8. Sends success notification webhook

The pipeline processes data for a specific business date and calculates
engagement hours and monetization metrics for each content item and reporting bucket.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging
import json

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests


# Threshold filter removed - include all consolidated values for anomaly detection


def send_success_webhook(context, records_processed, total_value, business_date):
    """
    Send success webhook notification to the monitoring endpoint
    
    Args:
        context: Airflow context containing DAG run and task instance details
        records_processed: Number of records written to database
        total_value: Total consolidated value
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
            "task_id": "write_engagement_monetization_dashboard",
            "run_id": dag_run.run_id,
            "organization_id": "cmfhfglh40k9h01rbszv8zhxx",
            "deployment_id": "cmfhfmdq70kb601rb5wu0d2nd",
            "dag_details": {
                "dag_id": dag.dag_id,
                "execution_date": execution_date.isoformat(),
                "state": "success",
                "log_url": f"https://cmfhfglh40k9h01rbszv8zhxx.astronomer.run/dwu0d2nd/dags/{dag.dag_id}/grid",
                "records_processed": records_processed,
                "total_consolidated_value": float(total_value),
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
    'engagement_monetization_pipeline',
    default_args=default_args,
    description='ETL pipeline for consolidating playback and ad-impression events into engagement and monetization dashboards',
    schedule='@daily',
    catchup=False,
    tags=['etl', 'analytics', 'engagement', 'production'],
)
def engagement_monetization_pipeline():
    """Main DAG definition using TaskFlow API"""
    
    @task()
    def read_playback_events(**context) -> List[Dict[str, Any]]:
        """
        Read playback events from playback_events table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"💰 Reading playback events for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, content_id, reporting_bucket, total_engagement_hours
            FROM public.playback_events
            WHERE business_date = %s
            ORDER BY content_id, reporting_bucket
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        playback_data = []
        for row in results:
            playback_data.append({
                'business_date': str(row[0]),
                'content_id': row[1],
                'reporting_bucket': row[2],
                'total_engagement_hours': float(row[3])
            })
        
        print(f"✅ Found {len(playback_data)} playback event records for {business_date}")
        logging.info(f"Found {len(playback_data)} playback event records for {business_date}")
        
        return playback_data
    
    @task()
    def read_ad_impression_events(**context) -> List[Dict[str, Any]]:
        """
        Read ad impression events from ad_impression_events table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"💸 Reading ad impression events for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, content_id, reporting_bucket, total_engagement_hours
            FROM public.ad_impression_events
            WHERE business_date = %s
            ORDER BY content_id, reporting_bucket
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        ad_impression_data = []
        for row in results:
            ad_impression_data.append({
                'business_date': str(row[0]),
                'content_id': row[1],
                'reporting_bucket': row[2],
                'total_engagement_hours': float(row[3])
            })
        
        print(f"✅ Found {len(ad_impression_data)} ad impression event records for {business_date}")
        logging.info(f"Found {len(ad_impression_data)} ad impression event records for {business_date}")
        
        return ad_impression_data
    
    @task()
    def read_platform_dimension_mappings(**context) -> List[Dict[str, Any]]:
        """
        Read platform dimension mappings from platform_dimension_mappings table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🏦 Reading platform dimension mappings for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, content_id, reporting_bucket, total_engagement_hours
            FROM public.platform_dimension_mappings
            WHERE business_date = %s
            ORDER BY content_id, reporting_bucket
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        platform_dimensions = []
        for row in results:
            platform_dimensions.append({
                'business_date': str(row[0]),
                'content_id': row[1],
                'reporting_bucket': row[2],
                'total_engagement_hours': float(row[3])
            })
        
        print(f"✅ Found {len(platform_dimensions)} platform dimension mapping records for {business_date}")
        logging.info(f"Found {len(platform_dimensions)} platform dimension mapping records for {business_date}")
        
        return platform_dimensions
    
    @task()
    def event_stream_consolidation(
        playback_data: List[Dict[str, Any]],
        ad_impression_data: List[Dict[str, Any]],
        platform_dimensions: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Consolidate event stream data by calculating playback + impressions + platform dimensions for each content item.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🧮 Consolidating event stream data for {business_date}")
        
        playback_lookup = {}
        for playback in playback_data:
            key = (playback['content_id'], playback['reporting_bucket'])
            amount = playback['total_engagement_hours']
            playback_lookup[key] = playback_lookup.get(key, 0) + amount
        
        ad_impression_lookup = {}
        for ad_impression in ad_impression_data:
            key = (ad_impression['content_id'], ad_impression['reporting_bucket'])
            amount = ad_impression['total_engagement_hours']
            ad_impression_lookup[key] = ad_impression_lookup.get(key, 0) + amount
        
        platform_lookup = {}
        for platform in platform_dimensions:
            key = (platform['content_id'], platform['reporting_bucket'])
            amount = platform['total_engagement_hours']
            platform_lookup[key] = platform_lookup.get(key, 0) + amount
        
        # Get all unique content/reporting bucket combinations
        all_keys = set(playback_lookup.keys()) | set(ad_impression_lookup.keys()) | set(platform_lookup.keys())
        
        consolidated = []
        for content_id, reporting_bucket in all_keys:
            playback_count = playback_lookup.get((content_id, reporting_bucket), 0.0)
            impression_count = ad_impression_lookup.get((content_id, reporting_bucket), 0.0)
            row_count = platform_lookup.get((content_id, reporting_bucket), 0.0)
            
            consolidated_total_engagement_hours = playback_count - impression_count - row_count
            
            consolidated.append({
                'business_date': business_date,
                'content_id': content_id,
                'reporting_bucket': reporting_bucket,
                'total_engagement_hours': consolidated_total_engagement_hours
            })
            
            print(f"  Content {content_id} ({reporting_bucket}): "
                  f"${playback_count:,.2f} - ${impression_count:,.2f} - ${row_count:,.2f} = "
                  f"${consolidated_total_engagement_hours:,.2f}")
        
        print(f"✅ Consolidated {len(consolidated)} content records")
        logging.info(f"Consolidated {len(consolidated)} content records for {business_date}")
        
        return consolidated
    
    @task()
    def write_consolidated_engagement_events(consolidated: List[Dict[str, Any]], **context):
        """
        Write consolidated engagement events data to consolidated_engagement_events table
        """
        print(f"💾 Writing {len(consolidated)} records to consolidated_engagement_events table")
        
        if not consolidated:
            print("⚠️  No consolidated records to write")
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.consolidated_engagement_events 
                (business_date, content_id, reporting_bucket, total_engagement_hours)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (business_date, content_id, reporting_bucket) DO UPDATE SET
                total_engagement_hours = EXCLUDED.total_engagement_hours
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in consolidated:
                cursor.execute(insert_query, (
                    record['business_date'],
                    record['content_id'],
                    record['reporting_bucket'],
                    record['total_engagement_hours']
                ))
            
            conn.commit()
            print(f"✅ Successfully wrote {len(consolidated)} records to consolidated_engagement_events")
            logging.info(f"Wrote {len(consolidated)} consolidated records")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to consolidated_engagement_events: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    @task()
    def read_content_metadata(**context) -> List[Dict[str, Any]]:
        """
        Read content metadata from content_metadata table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📋 Reading content metadata for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, content_id, reporting_bucket, is_reportable
            FROM public.content_metadata
            WHERE business_date = %s
            ORDER BY content_id, reporting_bucket
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        ref_data = []
        for row in results:
            ref_data.append({
                'business_date': str(row[0]),
                'content_id': row[1],
                'reporting_bucket': row[2],
                'is_reportable': row[3]
            })
        
        print(f"✅ Found {len(ref_data)} content metadata records for {business_date}")
        logging.info(f"Found {len(ref_data)} content metadata records for {business_date}")
        
        return ref_data
    
    @task()
    def read_consolidated_engagement_data(**context) -> List[Dict[str, Any]]:
        """
        Read consolidated data from consolidated_engagement_events table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📊 Reading consolidated data for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, content_id, reporting_bucket, total_engagement_hours
            FROM public.consolidated_engagement_events
            WHERE business_date = %s
            ORDER BY content_id, reporting_bucket
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        consolidated = []
        for row in results:
            consolidated.append({
                'business_date': str(row[0]),
                'content_id': row[1],
                'reporting_bucket': row[2],
                'total_engagement_hours': float(row[3])
            })
        
        print(f"✅ Found {len(consolidated)} consolidated records for {business_date}")
        logging.info(f"Found {len(consolidated)} consolidated records for {business_date}")
        
        return consolidated
    
    @task()
    def dashboard_kpi_calculation(
        consolidated: List[Dict[str, Any]],
        ref_data: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Calculate dashboard KPIs by joining consolidated data with content metadata
        and filtering for reportable content only.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🔒 Calculating dashboard KPIs for {business_date}")
        
        # Build lookup for reportable content
        reportable_lookup = {}
        for ref in ref_data:
            if ref['is_reportable']:
                key = (ref['content_id'], ref['reporting_bucket'])
                reportable_lookup[key] = True
        
        # Filter consolidated data to only reportable content
        dashboard_records = []
        total_value = 0
        
        for record in consolidated:
            key = (record['content_id'], record['reporting_bucket'])
            
            if key in reportable_lookup:
                dashboard_records.append({
                    'business_date': record['business_date'],
                    'content_id': record['content_id'],
                    'reporting_bucket': record['reporting_bucket'],
                    'total_engagement_hours': record['total_engagement_hours']
                })
                total_value += record['total_engagement_hours']
                
                print(f"  Reportable: Content {record['content_id']} ({record['reporting_bucket']}): "
                      f"${record['total_engagement_hours']:,.2f}")
        
        print(f"✅ Found {len(dashboard_records)} reportable content records")
        print(f"   Total dashboard value: ${total_value:,.2f}")
        logging.info(f"Found {len(dashboard_records)} reportable records with total value: {total_value}")
        
        return dashboard_records
    
    @task()
    def write_engagement_monetization_dashboard(dashboard_records: List[Dict[str, Any]], **context):
        """
        Write final dashboard data to engagement_monetization_dashboard table
        """
        print(f"💾 Writing {len(dashboard_records)} records to engagement_monetization_dashboard table")
        
        records_written = 0
        total_value = 0
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        if not dashboard_records:
            print("⚠️  No dashboard records to write")
            send_success_webhook(context, records_written, total_value, business_date)
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.engagement_monetization_dashboard 
                (business_date, content_id, reporting_bucket, total_engagement_hours)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (business_date, content_id, reporting_bucket) DO UPDATE SET
                total_engagement_hours = EXCLUDED.total_engagement_hours
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in dashboard_records:
                cursor.execute(insert_query, (
                    record['business_date'],
                    record['content_id'],
                    record['reporting_bucket'],
                    record['total_engagement_hours']
                ))
                
                records_written += 1
                total_value += record['total_engagement_hours']
            
            conn.commit()
            
            print(f"✅ Successfully wrote {records_written} records to engagement_monetization_dashboard")
            logging.info(f"Wrote {records_written} dashboard records with total value: {total_value}")
            
            context['ti'].xcom_push(key='records_processed', value=records_written)
            context['ti'].xcom_push(key='total_value', value=total_value)
            context['ti'].xcom_push(key='business_date', value=business_date)
            
            send_success_webhook(context, records_written, total_value, business_date)
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to engagement_monetization_dashboard: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    # Phase 1: Read upstream data in parallel
    playback_data = read_playback_events()
    ad_impression_data = read_ad_impression_events()
    platform_dimensions = read_platform_dimension_mappings()
    
    # Phase 2: Consolidate event stream data
    consolidated = event_stream_consolidation(playback_data, ad_impression_data, platform_dimensions)
    
    # Phase 3: Write consolidated data
    write_consolidated = write_consolidated_engagement_events(consolidated)
    
    # Phase 4: Read data for dashboard calculation (after write completes)
    consolidated_data = read_consolidated_engagement_data()
    ref_data = read_content_metadata()
    
    # Set dependencies - consolidated read must wait for write
    write_consolidated >> consolidated_data
    
    # Phase 5: Calculate dashboard KPIs (join with metadata, filter reportable)
    dashboard_data = dashboard_kpi_calculation(consolidated_data, ref_data)
    
    # Phase 6: Write final dashboard
    write_engagement_monetization_dashboard(dashboard_data)


dag = engagement_monetization_pipeline()