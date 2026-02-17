"""
## Nurse Satisfaction Analytics Pipeline

This DAG consolidates nurse satisfaction survey data with staffing information and generates unit-level risk flags:
1. Reads satisfaction survey responses from satisfaction_survey_feed table
2. Reads unit roster assignments from unit_rosters table
3. Reads shift schedule data from shift_schedules table
4. Calculates unit satisfaction scores by combining survey responses with roster and schedule context
5. Writes aggregated scores to unit_satisfaction_scores table
6. Joins with unit risk threshold data to identify at-risk units
7. Writes final risk flags to unit_risk_report table
8. Sends success notification webhook

The pipeline processes data for a specific business date and calculates
the unit-level satisfaction score to identify units requiring management attention.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging
import json

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests


# Threshold filter removed - include all aggregated scores for anomaly detection


def send_success_webhook(context, records_processed, total_value, business_date):
    """
    Send success webhook notification to the monitoring endpoint
    
    Args:
        context: Airflow context containing DAG run and task instance details
        records_processed: Number of records written to database
        total_value: Total unit satisfaction score
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
            "task_id": "write_unit_risk_report",
            "run_id": dag_run.run_id,
            "organization_id": "cmfhfglh40k9h01rbszv8zhxx",
            "deployment_id": "cmfhfmdq70kb601rb5wu0d2nd",
            "dag_details": {
                "dag_id": dag.dag_id,
                "execution_date": execution_date.isoformat(),
                "state": "success",
                "log_url": f"https://cmfhfglh40k9h01rbszv8zhxx.astronomer.run/dwu0d2nd/dags/{dag.dag_id}/grid",
                "records_processed": records_processed,
                "total_unit_satisfaction_score": float(total_value),
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
    'nurse_satisfaction_pipeline',
    default_args=default_args,
    description='ETL pipeline for consolidating nurse satisfaction survey data and generating unit risk reports',
    schedule='@daily',
    catchup=False,
    tags=['etl', 'healthcare', 'nurse-satisfaction', 'production'],
)
def nurse_satisfaction_pipeline():
    """Main DAG definition using TaskFlow API"""
    
    @task()
    def read_satisfaction_surveys(**context) -> List[Dict[str, Any]]:
        """
        Read satisfaction survey responses from satisfaction_survey_feed table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"💰 Reading satisfaction survey responses for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, unit_id, hospital_region, survey_score
            FROM public.satisfaction_survey_feed
            WHERE business_date = %s
            ORDER BY unit_id, hospital_region
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        surveys = []
        for row in results:
            surveys.append({
                'business_date': str(row[0]),
                'unit_id': row[1],
                'hospital_region': row[2],
                'survey_score': float(row[3])
            })
        
        print(f"✅ Found {len(surveys)} satisfaction survey response records for {business_date}")
        logging.info(f"Found {len(surveys)} satisfaction survey response records for {business_date}")
        
        return surveys
    
    @task()
    def read_unit_rosters(**context) -> List[Dict[str, Any]]:
        """
        Read unit roster assignments from unit_rosters table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"💸 Reading unit roster assignments for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, unit_id, hospital_region, roster_weight
            FROM public.unit_rosters
            WHERE business_date = %s
            ORDER BY unit_id, hospital_region
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        rosters = []
        for row in results:
            rosters.append({
                'business_date': str(row[0]),
                'unit_id': row[1],
                'hospital_region': row[2],
                'roster_weight': float(row[3])
            })
        
        print(f"✅ Found {len(rosters)} unit roster assignment records for {business_date}")
        logging.info(f"Found {len(rosters)} unit roster assignment records for {business_date}")
        
        return rosters
    
    @task()
    def read_shift_schedules(**context) -> List[Dict[str, Any]]:
        """
        Read shift schedule data from shift_schedules table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🏦 Reading shift schedule data for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, unit_id, hospital_region, schedule_hours
            FROM public.shift_schedules
            WHERE business_date = %s
            ORDER BY unit_id, hospital_region
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        schedules = []
        for row in results:
            schedules.append({
                'business_date': str(row[0]),
                'unit_id': row[1],
                'hospital_region': row[2],
                'schedule_hours': float(row[3])
            })
        
        print(f"✅ Found {len(schedules)} shift schedule data records for {business_date}")
        logging.info(f"Found {len(schedules)} shift schedule data records for {business_date}")
        
        return schedules
    
    @task()
    def satisfaction_score_calc(
        surveys: List[Dict[str, Any]],
        rosters: List[Dict[str, Any]],
        schedules: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Calculate unit satisfaction scores by combining survey responses with roster and schedule data for each unit.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🧮 Calculating unit satisfaction scores for {business_date}")
        
        survey_lookup = {}
        for survey in surveys:
            key = (survey['unit_id'], survey['hospital_region'])
            if key not in survey_lookup:
                survey_lookup[key] = survey['survey_score']
        
        roster_lookup = {}
        for roster in rosters:
            key = (roster['unit_id'], roster['hospital_region'])
            if key not in roster_lookup:
                roster_lookup[key] = roster['roster_weight']
        
        schedule_lookup = {}
        for schedule in schedules:
            key = (schedule['unit_id'], schedule['hospital_region'])
            if key not in schedule_lookup:
                schedule_lookup[key] = schedule['schedule_hours']
        
        # Get all unique unit/region combinations
        all_keys = set(survey_lookup.keys()) | set(roster_lookup.keys()) | set(schedule_lookup.keys())
        
        consolidated = []
        for unit_id, hospital_region in all_keys:
            survey_score = survey_lookup.get((unit_id, hospital_region), 0.0)
            roster_weight = roster_lookup.get((unit_id, hospital_region), 0.0)
            schedule_hours = schedule_lookup.get((unit_id, hospital_region), 0.0)
            
            unit_satisfaction_score = survey_score - roster_weight - schedule_hours
            
            consolidated.append({
                'business_date': business_date,
                'unit_id': unit_id,
                'hospital_region': hospital_region,
                'unit_satisfaction_score': unit_satisfaction_score
            })
            
            print(f"  Unit {unit_id} ({hospital_region}): "
                  f"${survey_score:,.2f} - ${roster_weight:,.2f} - ${schedule_hours:,.2f} = "
                  f"${unit_satisfaction_score:,.2f}")
        
        print(f"✅ Calculated {len(consolidated)} unit satisfaction score records")
        logging.info(f"Calculated {len(consolidated)} unit satisfaction score records for {business_date}")
        
        return consolidated
    
    @task()
    def write_unit_satisfaction_scores(consolidated: List[Dict[str, Any]], **context):
        """
        Write aggregated unit satisfaction score data to unit_satisfaction_scores table
        """
        print(f"💾 Writing {len(consolidated)} records to unit_satisfaction_scores table")
        
        if not consolidated:
            print("⚠️  No aggregated score records to write")
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.unit_satisfaction_scores 
                (business_date, unit_id, hospital_region, unit_satisfaction_score)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (business_date, unit_id, hospital_region) DO UPDATE SET
                unit_satisfaction_score = EXCLUDED.unit_satisfaction_score
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in consolidated:
                cursor.execute(insert_query, (
                    record['business_date'],
                    record['unit_id'],
                    record['hospital_region'],
                    record['unit_satisfaction_score']
                ))
            
            conn.commit()
            print(f"✅ Successfully wrote {len(consolidated)} records to unit_satisfaction_scores")
            logging.info(f"Wrote {len(consolidated)} aggregated score records")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to unit_satisfaction_scores: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    @task()
    def read_unit_thresholds(**context) -> List[Dict[str, Any]]:
        """
        Read unit risk threshold data from unit_risk_thresholds table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📋 Reading unit risk threshold data for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, unit_id, hospital_region, requires_intervention
            FROM public.unit_risk_thresholds
            WHERE business_date = %s
            ORDER BY unit_id, hospital_region
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        ref_data = []
        for row in results:
            ref_data.append({
                'business_date': str(row[0]),
                'unit_id': row[1],
                'hospital_region': row[2],
                'requires_intervention': row[3]
            })
        
        print(f"✅ Found {len(ref_data)} unit risk threshold records for {business_date}")
        logging.info(f"Found {len(ref_data)} unit risk threshold records for {business_date}")
        
        return ref_data
    
    @task()
    def read_unit_scores(**context) -> List[Dict[str, Any]]:
        """
        Read aggregated score data from unit_satisfaction_scores table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📊 Reading aggregated score data for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, unit_id, hospital_region, unit_satisfaction_score
            FROM public.unit_satisfaction_scores
            WHERE business_date = %s
            ORDER BY unit_id, hospital_region
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        consolidated = []
        for row in results:
            consolidated.append({
                'business_date': str(row[0]),
                'unit_id': row[1],
                'hospital_region': row[2],
                'unit_satisfaction_score': float(row[3])
            })
        
        print(f"✅ Found {len(consolidated)} aggregated score records for {business_date}")
        logging.info(f"Found {len(consolidated)} aggregated score records for {business_date}")
        
        return consolidated
    
    @task()
    def risk_flag_calculation(
        consolidated: List[Dict[str, Any]],
        ref_data: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Calculate unit risk flags by joining aggregated score data with threshold data
        and filtering for units requiring intervention only.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🔒 Calculating unit risk flags for {business_date}")
        
        # Build lookup for units requiring intervention
        reportable_lookup = {}
        for ref in ref_data:
            if ref['requires_intervention']:
                key = (ref['unit_id'], ref['hospital_region'])
                reportable_lookup[key] = True
        
        # Filter aggregated score data to only units requiring intervention
        risk_flag_records = []
        total_value = 0
        
        for record in consolidated:
            key = (record['unit_id'], record['hospital_region'])
            
            if key in reportable_lookup:
                risk_flag_records.append({
                    'business_date': record['business_date'],
                    'unit_id': record['unit_id'],
                    'hospital_region': record['hospital_region'],
                    'unit_satisfaction_score': record['unit_satisfaction_score']
                })
                total_value += record['unit_satisfaction_score']
                
                print(f"  Requires intervention: Unit {record['unit_id']} ({record['hospital_region']}): "
                      f"${record['unit_satisfaction_score']:,.2f}")
        
        print(f"✅ Found {len(risk_flag_records)} units requiring intervention records")
        print(f"   Total risk flag value: ${total_value:,.2f}")
        logging.info(f"Found {len(risk_flag_records)} units requiring intervention records with total value: {total_value}")
        
        return risk_flag_records
    
    @task()
    def write_unit_risk_report(risk_flag_records: List[Dict[str, Any]], **context):
        """
        Write final risk flag report to unit_risk_report table
        """
        print(f"💾 Writing {len(risk_flag_records)} records to unit_risk_report table")
        
        records_written = 0
        total_value = 0
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        if not risk_flag_records:
            print("⚠️  No risk flag records to write")
            send_success_webhook(context, records_written, total_value, business_date)
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.unit_risk_report 
                (business_date, unit_id, hospital_region, unit_satisfaction_score)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (business_date, unit_id, hospital_region) DO UPDATE SET
                unit_satisfaction_score = EXCLUDED.unit_satisfaction_score
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in risk_flag_records:
                cursor.execute(insert_query, (
                    record['business_date'],
                    record['unit_id'],
                    record['hospital_region'],
                    record['unit_satisfaction_score']
                ))
                
                records_written += 1
                total_value += record['unit_satisfaction_score']
            
            conn.commit()
            
            print(f"✅ Successfully wrote {records_written} records to unit_risk_report")
            logging.info(f"Wrote {records_written} risk flag records with total value: {total_value}")
            
            context['ti'].xcom_push(key='records_processed', value=records_written)
            context['ti'].xcom_push(key='total_value', value=total_value)
            context['ti'].xcom_push(key='business_date', value=business_date)
            
            send_success_webhook(context, records_written, total_value, business_date)
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to unit_risk_report: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    # Phase 1: Read upstream data in parallel
    surveys = read_satisfaction_surveys()
    rosters = read_unit_rosters()
    schedules = read_shift_schedules()
    
    # Phase 2: Calculate unit satisfaction scores
    consolidated = satisfaction_score_calc(surveys, rosters, schedules)
    
    # Phase 3: Write aggregated score data
    write_consolidated = write_unit_satisfaction_scores(consolidated)
    
    # Phase 4: Read data for risk flag calculation (after write completes)
    consolidated_data = read_unit_scores()
    ref_data = read_unit_thresholds()
    
    # Set dependencies - aggregated score read must wait for write
    write_consolidated >> consolidated_data
    
    # Phase 5: Calculate risk flags (join with threshold data, filter units requiring intervention)
    risk_flags = risk_flag_calculation(consolidated_data, ref_data)
    
    # Phase 6: Write final report
    write_unit_risk_report(risk_flags)


dag = nurse_satisfaction_pipeline()