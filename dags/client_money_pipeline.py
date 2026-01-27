"""
## Client Money Consolidation Pipeline

This DAG consolidates client money positions and generates reportable lockup calculations:
1. Reads customer credits from customer_credits table
2. Reads customer debits from customer_debits table
3. Reads reserve balances from reserve_balance table
4. Consolidates amounts (credits - debits - reserve) per customer/region
5. Writes consolidated data to client_money_consolidated table
6. Joins with customer reference data to filter reportable customers
7. Writes final report to client_money_report table
8. Sends success notification webhook

The pipeline processes data for a specific business date and calculates
the consolidated client money position for each customer.
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
            "task_id": "write_client_money_report",
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
    'client_money_pipeline',
    default_args=default_args,
    description='ETL pipeline for consolidating client money positions and generating lockup reports',
    schedule='@daily',
    catchup=False,
    tags=['etl', 'finance', 'client-money', 'production'],
)
def client_money_pipeline():
    """Main DAG definition using TaskFlow API"""
    
    @task()
    def read_customer_credits(**context) -> List[Dict[str, Any]]:
        """
        Read customer credits from customer_credits table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"💰 Reading customer credits for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, cust_id, reporting_region, amount_usd
            FROM public.customer_credits
            WHERE business_date = %s
            ORDER BY cust_id, reporting_region
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        credits = []
        for row in results:
            credits.append({
                'business_date': str(row[0]),
                'cust_id': row[1],
                'reporting_region': row[2],
                'amount_usd': float(row[3])
            })
        
        print(f"✅ Found {len(credits)} customer credit records for {business_date}")
        logging.info(f"Found {len(credits)} customer credit records for {business_date}")
        
        return credits
    
    @task()
    def read_customer_debits(**context) -> List[Dict[str, Any]]:
        """
        Read customer debits from customer_debits table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"💸 Reading customer debits for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, cust_id, reporting_region, amount_usd
            FROM public.customer_debits
            WHERE business_date = %s
            ORDER BY cust_id, reporting_region
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        debits = []
        for row in results:
            debits.append({
                'business_date': str(row[0]),
                'cust_id': row[1],
                'reporting_region': row[2],
                'amount_usd': float(row[3])
            })
        
        print(f"✅ Found {len(debits)} customer debit records for {business_date}")
        logging.info(f"Found {len(debits)} customer debit records for {business_date}")
        
        return debits
    
    @task()
    def read_reserve_balance(**context) -> List[Dict[str, Any]]:
        """
        Read reserve balances from reserve_balance table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🏦 Reading reserve balances for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, cust_id, reporting_region, amount_usd
            FROM public.reserve_balance
            WHERE business_date = %s
            ORDER BY cust_id, reporting_region
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        reserves = []
        for row in results:
            reserves.append({
                'business_date': str(row[0]),
                'cust_id': row[1],
                'reporting_region': row[2],
                'amount_usd': float(row[3])
            })
        
        print(f"✅ Found {len(reserves)} reserve balance records for {business_date}")
        logging.info(f"Found {len(reserves)} reserve balance records for {business_date}")
        
        return reserves
    
    @task()
    def subledger_consolidation(
        credits: List[Dict[str, Any]],
        debits: List[Dict[str, Any]],
        reserves: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Consolidate subledger data by calculating credits - debits - reserve for each customer.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🧮 Consolidating subledger data for {business_date}")
        
        credit_lookup = {}
        for credit in credits:
            key = (credit['cust_id'], credit['reporting_region'])
            credit_lookup[key] = credit_lookup.get(key, 0) + credit['amount_usd']
        
        debit_lookup = {}
        for debit in debits:
            key = (debit['cust_id'], debit['reporting_region'])
            debit_lookup[key] = debit_lookup.get(key, 0) + debit['amount_usd']
        
        reserve_lookup = {}
        for reserve in reserves:
            key = (reserve['cust_id'], reserve['reporting_region'])
            reserve_lookup[key] = reserve_lookup.get(key, 0) + reserve['amount_usd']
        
        # Get all unique customer/region combinations
        all_keys = set(credit_lookup.keys()) | set(debit_lookup.keys()) | set(reserve_lookup.keys())
        
        consolidated = []
        for cust_id, reporting_region in all_keys:
            credit_amount = credit_lookup.get((cust_id, reporting_region), 0.0)
            debit_amount = debit_lookup.get((cust_id, reporting_region), 0.0)
            reserve_amount = reserve_lookup.get((cust_id, reporting_region), 0.0)
            
            consolidated_amount = credit_amount - debit_amount - reserve_amount
            
            consolidated.append({
                'business_date': business_date,
                'cust_id': cust_id,
                'reporting_region': reporting_region,
                'consolidated_amount_usd': consolidated_amount
            })
            
            print(f"  Customer {cust_id} ({reporting_region}): "
                  f"${credit_amount:,.2f} - ${debit_amount:,.2f} - ${reserve_amount:,.2f} = "
                  f"${consolidated_amount:,.2f}")
        
        print(f"✅ Consolidated {len(consolidated)} customer records")
        logging.info(f"Consolidated {len(consolidated)} customer records for {business_date}")
        
        return consolidated
    
    @task()
    def write_client_money_consolidated(consolidated: List[Dict[str, Any]], **context):
        """
        Write consolidated client money data to client_money_consolidated table
        """
        print(f"💾 Writing {len(consolidated)} records to client_money_consolidated table")
        
        if not consolidated:
            print("⚠️  No consolidated records to write")
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.client_money_consolidated 
                (business_date, cust_id, reporting_region, consolidated_amount_usd)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (business_date, cust_id, reporting_region) DO UPDATE SET
                consolidated_amount_usd = EXCLUDED.consolidated_amount_usd
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in consolidated:
                cursor.execute(insert_query, (
                    record['business_date'],
                    record['cust_id'],
                    record['reporting_region'],
                    record['consolidated_amount_usd']
                ))
            
            conn.commit()
            print(f"✅ Successfully wrote {len(consolidated)} records to client_money_consolidated")
            logging.info(f"Wrote {len(consolidated)} consolidated records")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to client_money_consolidated: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    @task()
    def read_customer_ref_data(**context) -> List[Dict[str, Any]]:
        """
        Read customer reference data from customer_ref_data table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📋 Reading customer reference data for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, cust_id, reporting_region, is_reportable
            FROM public.customer_ref_data
            WHERE business_date = %s
            ORDER BY cust_id, reporting_region
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        ref_data = []
        for row in results:
            ref_data.append({
                'business_date': str(row[0]),
                'cust_id': row[1],
                'reporting_region': row[2],
                'is_reportable': row[3]
            })
        
        print(f"✅ Found {len(ref_data)} customer reference records for {business_date}")
        logging.info(f"Found {len(ref_data)} customer reference records for {business_date}")
        
        return ref_data
    
    @task()
    def read_consolidated_data(**context) -> List[Dict[str, Any]]:
        """
        Read consolidated data from client_money_consolidated table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📊 Reading consolidated data for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, cust_id, reporting_region, consolidated_amount_usd
            FROM public.client_money_consolidated
            WHERE business_date = %s
            ORDER BY cust_id, reporting_region
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        consolidated = []
        for row in results:
            consolidated.append({
                'business_date': str(row[0]),
                'cust_id': row[1],
                'reporting_region': row[2],
                'consolidated_amount_usd': float(row[3])
            })
        
        print(f"✅ Found {len(consolidated)} consolidated records for {business_date}")
        logging.info(f"Found {len(consolidated)} consolidated records for {business_date}")
        
        return consolidated
    
    @task()
    def client_money_lockup_calc(
        consolidated: List[Dict[str, Any]],
        ref_data: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Calculate client money lockup by joining consolidated data with reference data
        and filtering for reportable customers only.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🔒 Calculating client money lockup for {business_date}")
        
        # Build lookup for reportable customers
        reportable_lookup = {}
        for ref in ref_data:
            if ref['is_reportable']:
                key = (ref['cust_id'], ref['reporting_region'])
                reportable_lookup[key] = True
        
        # Filter consolidated data to only reportable customers
        lockup_records = []
        total_value = 0
        
        for record in consolidated:
            key = (record['cust_id'], record['reporting_region'])
            
            if key in reportable_lookup:
                lockup_records.append({
                    'business_date': record['business_date'],
                    'cust_id': record['cust_id'],
                    'reporting_region': record['reporting_region'],
                    'consolidated_amount_usd': record['consolidated_amount_usd']
                })
                total_value += record['consolidated_amount_usd']
                
                print(f"  Reportable: Customer {record['cust_id']} ({record['reporting_region']}): "
                      f"${record['consolidated_amount_usd']:,.2f}")
        
        print(f"✅ Found {len(lockup_records)} reportable customer records")
        print(f"   Total lockup value: ${total_value:,.2f}")
        logging.info(f"Found {len(lockup_records)} reportable records with total value: {total_value}")
        
        return lockup_records
    
    @task()
    def write_client_money_report(lockup_records: List[Dict[str, Any]], **context):
        """
        Write final lockup report to client_money_report table
        """
        print(f"💾 Writing {len(lockup_records)} records to client_money_report table")
        
        records_written = 0
        total_value = 0
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        if not lockup_records:
            print("⚠️  No lockup records to write")
            send_success_webhook(context, records_written, total_value, business_date)
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.client_money_report 
                (business_date, cust_id, reporting_region, consolidated_amount_usd)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (business_date, cust_id, reporting_region) DO UPDATE SET
                consolidated_amount_usd = EXCLUDED.consolidated_amount_usd
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in lockup_records:
                cursor.execute(insert_query, (
                    record['business_date'],
                    record['cust_id'],
                    record['reporting_region'],
                    record['consolidated_amount_usd']
                ))
                
                records_written += 1
                total_value += record['consolidated_amount_usd']
            
            conn.commit()
            
            print(f"✅ Successfully wrote {records_written} records to client_money_report")
            logging.info(f"Wrote {records_written} lockup records with total value: {total_value}")
            
            context['ti'].xcom_push(key='records_processed', value=records_written)
            context['ti'].xcom_push(key='total_value', value=total_value)
            context['ti'].xcom_push(key='business_date', value=business_date)
            
            send_success_webhook(context, records_written, total_value, business_date)
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to client_money_report: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    # Phase 1: Read upstream data in parallel
    credits = read_customer_credits()
    debits = read_customer_debits()
    reserves = read_reserve_balance()
    
    # Phase 2: Consolidate subledger data
    consolidated = subledger_consolidation(credits, debits, reserves)
    
    # Phase 3: Write consolidated data
    write_consolidated = write_client_money_consolidated(consolidated)
    
    # Phase 4: Read data for lockup calculation (after write completes)
    consolidated_data = read_consolidated_data()
    ref_data = read_customer_ref_data()
    
    # Set dependencies - consolidated read must wait for write
    write_consolidated >> consolidated_data
    
    # Phase 5: Calculate lockup (join with ref data, filter reportable)
    lockup = client_money_lockup_calc(consolidated_data, ref_data)
    
    # Phase 6: Write final report
    write_client_money_report(lockup)


dag = client_money_pipeline()