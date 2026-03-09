"""
## Reserve Requirement Calculation Pipeline

This DAG calculates daily reserve requirements and generates compliance reports:
1. Reads custody balances from custody_balances table
2. Reads obligation balances from obligation_balances table
3. Reads segregated account balances from segregated_account_balances table
4. Aggregates amounts (custody - obligations - segregated) per account/region
5. Writes aggregated data to reserve_requirement_consolidated table
6. Joins with account classification data to filter reportable accounts
7. Writes final report to reserve_requirement_report table
8. Sends success notification webhook

The pipeline processes data for a specific business date and calculates
the net reserve requirement for each account subject to regulatory reporting.
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
        total_value: Total net reserve requirement value
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
            "task_id": "write_reserve_requirement_report",
            "run_id": dag_run.run_id,
            "organization_id": "cmfhfglh40k9h01rbszv8zhxx",
            "deployment_id": "cmfhfmdq70kb601rb5wu0d2nd",
            "dag_details": {
                "dag_id": dag.dag_id,
                "execution_date": execution_date.isoformat(),
                "state": "success",
                "log_url": f"https://cmfhfglh40k9h01rbszv8zhxx.astronomer.run/dwu0d2nd/dags/{dag.dag_id}/grid",
                "records_processed": records_processed,
                "total_net_reserve_requirement_value": float(total_value),
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
    'reserve_requirement_pipeline',
    default_args=default_args,
    description='ETL pipeline for calculating daily reserve requirements and generating compliance reports',
    schedule='@daily',
    catchup=False,
    tags=['etl', 'compliance', 'reserve-requirement', 'production'],
)
def reserve_requirement_pipeline():
    """Main DAG definition using TaskFlow API"""
    
    @task()
    def read_custody_balances(**context) -> List[Dict[str, Any]]:
        """
        Read custody balances from custody_balances table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"💰 Reading custody balances for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, account_id, jurisdiction, balance_usd
            FROM public.custody_balances
            WHERE business_date = %s
            ORDER BY account_id, jurisdiction
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        custody = []
        for row in results:
            custody.append({
                'business_date': str(row[0]),
                'account_id': row[1],
                'jurisdiction': row[2],
                'balance_usd': float(row[3])
            })
        
        print(f"✅ Found {len(custody)} custody balance records for {business_date}")
        logging.info(f"Found {len(custody)} custody balance records for {business_date}")
        
        return custody
    
    @task()
    def read_obligation_balances(**context) -> List[Dict[str, Any]]:
        """
        Read obligation balances from obligation_balances table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"💸 Reading obligation balances for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, account_id, jurisdiction, balance_usd
            FROM public.obligation_balances
            WHERE business_date = %s
            ORDER BY account_id, jurisdiction
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        obligations = []
        for row in results:
            obligations.append({
                'business_date': str(row[0]),
                'account_id': row[1],
                'jurisdiction': row[2],
                'balance_usd': float(row[3])
            })
        
        print(f"✅ Found {len(obligations)} obligation balance records for {business_date}")
        logging.info(f"Found {len(obligations)} obligation balance records for {business_date}")
        
        return obligations
    
    @task()
    def read_segregated_account_balances(**context) -> List[Dict[str, Any]]:
        """
        Read segregated account balances from segregated_account_balances table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🏦 Reading segregated account balances for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, account_id, jurisdiction, balance_usd
            FROM public.segregated_account_balances
            WHERE business_date = %s
            ORDER BY account_id, jurisdiction
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        segregated = []
        for row in results:
            segregated.append({
                'business_date': str(row[0]),
                'account_id': row[1],
                'jurisdiction': row[2],
                'balance_usd': float(row[3])
            })
        
        print(f"✅ Found {len(segregated)} segregated account balance records for {business_date}")
        logging.info(f"Found {len(segregated)} segregated account balance records for {business_date}")
        
        return segregated
    
    @task()
    def balance_aggregation(
        custody: List[Dict[str, Any]],
        obligations: List[Dict[str, Any]],
        segregated: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Aggregate balance data by calculating custody - obligations - segregated for each account.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🧮 Aggregating balance data for {business_date}")
        
        custody_lookup = {}
        for cust in custody:
            key = (cust['account_id'], cust['jurisdiction'])
            if key not in custody_lookup:
                custody_lookup[key] = cust['balance_usd']
        
        obligation_lookup = {}
        for oblig in obligations:
            key = (oblig['account_id'], oblig['jurisdiction'])
            if key not in obligation_lookup:
                obligation_lookup[key] = oblig['balance_usd']
        
        segregated_lookup = {}
        for seg in segregated:
            key = (seg['account_id'], seg['jurisdiction'])
            if key not in segregated_lookup:
                segregated_lookup[key] = seg['balance_usd']
        
        # Get all unique account/jurisdiction combinations
        all_keys = set(custody_lookup.keys()) | set(obligation_lookup.keys()) | set(segregated_lookup.keys())
        
        consolidated = []
        for account_id, jurisdiction in all_keys:
            custody_amount = custody_lookup.get((account_id, jurisdiction), 0.0)
            obligation_amount = obligation_lookup.get((account_id, jurisdiction), 0.0)
            segregated_amount = segregated_lookup.get((account_id, jurisdiction), 0.0)
            
            net_reserve_requirement = custody_amount - obligation_amount - segregated_amount
            
            consolidated.append({
                'business_date': business_date,
                'account_id': account_id,
                'jurisdiction': jurisdiction,
                'net_reserve_requirement_usd': net_reserve_requirement
            })
            
            print(f"  Account {account_id} ({jurisdiction}): "
                  f"${custody_amount:,.2f} - ${obligation_amount:,.2f} - ${segregated_amount:,.2f} = "
                  f"${net_reserve_requirement:,.2f}")
        
        print(f"✅ Aggregated {len(consolidated)} account records")
        logging.info(f"Aggregated {len(consolidated)} account records for {business_date}")
        
        return consolidated
    
    @task()
    def write_reserve_requirement_consolidated(consolidated: List[Dict[str, Any]], **context):
        """
        Write aggregated reserve requirement data to reserve_requirement_consolidated table
        """
        print(f"💾 Writing {len(consolidated)} records to reserve_requirement_consolidated table")
        
        if not consolidated:
            print("⚠️  No aggregated records to write")
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.reserve_requirement_consolidated 
                (business_date, account_id, jurisdiction, net_reserve_requirement_usd)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (business_date, account_id, jurisdiction) DO UPDATE SET
                net_reserve_requirement_usd = EXCLUDED.net_reserve_requirement_usd
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in consolidated:
                cursor.execute(insert_query, (
                    record['business_date'],
                    record['account_id'],
                    record['jurisdiction'],
                    record['net_reserve_requirement_usd']
                ))
            
            conn.commit()
            print(f"✅ Successfully wrote {len(consolidated)} records to reserve_requirement_consolidated")
            logging.info(f"Wrote {len(consolidated)} aggregated records")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to reserve_requirement_consolidated: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    @task()
    def read_account_classification_data(**context) -> List[Dict[str, Any]]:
        """
        Read account classification data from account_classification_data table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📋 Reading account classification data for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, account_id, jurisdiction, is_reportable
            FROM public.account_classification_data
            WHERE business_date = %s
            ORDER BY account_id, jurisdiction
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        ref_data = []
        for row in results:
            ref_data.append({
                'business_date': str(row[0]),
                'account_id': row[1],
                'jurisdiction': row[2],
                'is_reportable': row[3]
            })
        
        print(f"✅ Found {len(ref_data)} account classification records for {business_date}")
        logging.info(f"Found {len(ref_data)} account classification records for {business_date}")
        
        return ref_data
    
    @task()
    def read_consolidated_balances(**context) -> List[Dict[str, Any]]:
        """
        Read aggregated data from reserve_requirement_consolidated table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📊 Reading aggregated data for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, account_id, jurisdiction, net_reserve_requirement_usd
            FROM public.reserve_requirement_consolidated
            WHERE business_date = %s
            ORDER BY account_id, jurisdiction
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        consolidated = []
        for row in results:
            consolidated.append({
                'business_date': str(row[0]),
                'account_id': row[1],
                'jurisdiction': row[2],
                'net_reserve_requirement_usd': float(row[3])
            })
        
        print(f"✅ Found {len(consolidated)} aggregated records for {business_date}")
        logging.info(f"Found {len(consolidated)} aggregated records for {business_date}")
        
        return consolidated
    
    @task()
    def reserve_requirement_calc(
        consolidated: List[Dict[str, Any]],
        ref_data: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Calculate reserve requirement by joining aggregated data with classification data
        and filtering for reportable accounts only.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🔒 Calculating reserve requirement for {business_date}")
        
        # Build lookup for reportable accounts
        reportable_lookup = {}
        for ref in ref_data:
            if ref['is_reportable']:
                key = (ref['account_id'], ref['jurisdiction'])
                reportable_lookup[key] = True
        
        # Filter aggregated data to only reportable accounts
        requirement_records = []
        total_value = 0
        
        for record in consolidated:
            key = (record['account_id'], record['jurisdiction'])
            
            if key in reportable_lookup:
                requirement_records.append({
                    'business_date': record['business_date'],
                    'account_id': record['account_id'],
                    'jurisdiction': record['jurisdiction'],
                    'net_reserve_requirement_usd': record['net_reserve_requirement_usd']
                })
                total_value += record['net_reserve_requirement_usd']
                
                print(f"  Reportable: Account {record['account_id']} ({record['jurisdiction']}): "
                      f"${record['net_reserve_requirement_usd']:,.2f}")
        
        print(f"✅ Found {len(requirement_records)} reportable account records")
        print(f"   Total requirement value: ${total_value:,.2f}")
        logging.info(f"Found {len(requirement_records)} reportable records with total value: {total_value}")
        
        return requirement_records
    
    @task()
    def write_reserve_requirement_report(requirement_records: List[Dict[str, Any]], **context):
        """
        Write final requirement report to reserve_requirement_report table
        """
        print(f"💾 Writing {len(requirement_records)} records to reserve_requirement_report table")
        
        records_written = 0
        total_value = 0
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        if not requirement_records:
            print("⚠️  No requirement records to write")
            send_success_webhook(context, records_written, total_value, business_date)
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.reserve_requirement_report 
                (business_date, account_id, jurisdiction, net_reserve_requirement_usd)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (business_date, account_id, jurisdiction) DO UPDATE SET
                net_reserve_requirement_usd = EXCLUDED.net_reserve_requirement_usd
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in requirement_records:
                cursor.execute(insert_query, (
                    record['business_date'],
                    record['account_id'],
                    record['jurisdiction'],
                    record['net_reserve_requirement_usd']
                ))
                
                records_written += 1
                total_value += record['net_reserve_requirement_usd']
            
            conn.commit()
            
            print(f"✅ Successfully wrote {records_written} records to reserve_requirement_report")
            logging.info(f"Wrote {records_written} requirement records with total value: {total_value}")
            
            context['ti'].xcom_push(key='records_processed', value=records_written)
            context['ti'].xcom_push(key='total_value', value=total_value)
            context['ti'].xcom_push(key='business_date', value=business_date)
            
            send_success_webhook(context, records_written, total_value, business_date)
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to reserve_requirement_report: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    # Phase 1: Read upstream data in parallel
    custody = read_custody_balances()
    obligations = read_obligation_balances()
    segregated = read_segregated_account_balances()
    
    # Phase 2: Aggregate balance data
    consolidated = balance_aggregation(custody, obligations, segregated)
    
    # Phase 3: Write aggregated data
    write_consolidated = write_reserve_requirement_consolidated(consolidated)
    
    # Phase 4: Read data for requirement calculation (after write completes)
    consolidated_data = read_consolidated_balances()
    ref_data = read_account_classification_data()
    
    # Set dependencies - aggregated read must wait for write
    write_consolidated >> consolidated_data
    
    # Phase 5: Calculate requirement (join with classification data, filter reportable)
    requirement = reserve_requirement_calc(consolidated_data, ref_data)
    
    # Phase 6: Write final report
    write_reserve_requirement_report(requirement)


dag = reserve_requirement_pipeline()