"""
## Cloud Spend Projection Pipeline

This DAG joins cloud billing data with account metadata and Savings Plan coverage to estimate monthly spend and savings:
1. Reads billing line items from cloud_billing_items table
2. Reads account attributes from account_metadata table
3. Reads Savings Plan coverage from savings_plan_coverage table
4. Joins billing with account and coverage data per account/cost center
5. Writes projected spend to monthly_spend_estimates table
6. Joins with cost optimization rules to filter reportable accounts
7. Writes final projections to spend_savings_report table
8. Sends success notification webhook

The pipeline processes data for a specific business date and calculates
the projected monthly cloud spend and potential savings for each account.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging
import json

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests


# Threshold filter removed - include all projected values for anomaly detection


def send_success_webhook(context, records_processed, total_value, business_date):
    """
    Send success webhook notification to the monitoring endpoint
    
    Args:
        context: Airflow context containing DAG run and task instance details
        records_processed: Number of records written to database
        total_value: Total projected value
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
            "task_id": "write_spend_savings_report",
            "run_id": dag_run.run_id,
            "organization_id": "cmfhfglh40k9h01rbszv8zhxx",
            "deployment_id": "cmfhfmdq70kb601rb5wu0d2nd",
            "dag_details": {
                "dag_id": dag.dag_id,
                "execution_date": execution_date.isoformat(),
                "state": "success",
                "log_url": f"https://cmfhfglh40k9h01rbszv8zhxx.astronomer.run/dwu0d2nd/dags/{dag.dag_id}/grid",
                "records_processed": records_processed,
                "total_projected_value": float(total_value),
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
    'cloud_spend_projection_pipeline',
    default_args=default_args,
    description='ETL pipeline for projecting cloud spend and calculating Savings Plan savings',
    schedule='@daily',
    catchup=False,
    tags=['etl', 'finops', 'cloud-cost', 'production'],
)
def cloud_spend_projection_pipeline():
    """Main DAG definition using TaskFlow API"""
    
    @task()
    def read_cloud_billing_items(**context) -> List[Dict[str, Any]]:
        """
        Read cloud billing items from cloud_billing_items table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"💰 Reading cloud billing items for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, account_id, cost_center, charge_usd
            FROM public.cloud_billing_items
            WHERE business_date = %s
            ORDER BY account_id, cost_center
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        billing_items = []
        for row in results:
            billing_items.append({
                'business_date': str(row[0]),
                'account_id': row[1],
                'cost_center': row[2],
                'charge_usd': float(row[3])
            })
        
        print(f"✅ Found {len(billing_items)} cloud billing item records for {business_date}")
        logging.info(f"Found {len(billing_items)} cloud billing item records for {business_date}")
        
        return billing_items
    
    @task()
    def read_account_metadata(**context) -> List[Dict[str, Any]]:
        """
        Read account metadata from account_metadata table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"💸 Reading account metadata for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, account_id, cost_center, adjustment_usd
            FROM public.account_metadata
            WHERE business_date = %s
            ORDER BY account_id, cost_center
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        account_meta = []
        for row in results:
            account_meta.append({
                'business_date': str(row[0]),
                'account_id': row[1],
                'cost_center': row[2],
                'adjustment_usd': float(row[3])
            })
        
        print(f"✅ Found {len(account_meta)} account metadata records for {business_date}")
        logging.info(f"Found {len(account_meta)} account metadata records for {business_date}")
        
        return account_meta
    
    @task()
    def read_savings_plan_coverage(**context) -> List[Dict[str, Any]]:
        """
        Read savings plan coverage from savings_plan_coverage table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🏦 Reading savings plan coverage for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, account_id, cost_center, coverage_usd
            FROM public.savings_plan_coverage
            WHERE business_date = %s
            ORDER BY account_id, cost_center
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        coverage = []
        for row in results:
            coverage.append({
                'business_date': str(row[0]),
                'account_id': row[1],
                'cost_center': row[2],
                'coverage_usd': float(row[3])
            })
        
        print(f"✅ Found {len(coverage)} savings plan coverage records for {business_date}")
        logging.info(f"Found {len(coverage)} savings plan coverage records for {business_date}")
        
        return coverage
    
    @task()
    def spend_projection_join(
        billing_items: List[Dict[str, Any]],
        account_meta: List[Dict[str, Any]],
        coverage: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Join billing data by calculating billing - account adjustments - coverage for each account.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🧮 Joining spend projection data for {business_date}")
        
        billing_lookup = {}
        for billing_item in billing_items:
            key = (billing_item['account_id'], billing_item['cost_center'])
            if key not in billing_lookup:
                billing_lookup[key] = billing_item['charge_usd']
        
        account_lookup = {}
        for account in account_meta:
            key = (account['account_id'], account['cost_center'])
            if key not in account_lookup:
                account_lookup[key] = account['adjustment_usd']
        
        coverage_lookup = {}
        for cov in coverage:
            key = (cov['account_id'], cov['cost_center'])
            if key not in coverage_lookup:
                coverage_lookup[key] = cov['coverage_usd']
        
        # Get all unique account/cost center combinations
        all_keys = set(billing_lookup.keys()) | set(account_lookup.keys()) | set(coverage_lookup.keys())
        
        consolidated = []
        for account_id, cost_center in all_keys:
            billing_amount = billing_lookup.get((account_id, cost_center), 0.0)
            account_adjustment = account_lookup.get((account_id, cost_center), 0.0)
            coverage_amount = coverage_lookup.get((account_id, cost_center), 0.0)
            
            projected_spend = billing_amount - account_adjustment - coverage_amount
            
            consolidated.append({
                'business_date': business_date,
                'account_id': account_id,
                'cost_center': cost_center,
                'projected_spend_usd': projected_spend
            })
            
            print(f"  Account {account_id} ({cost_center}): "
                  f"${billing_amount:,.2f} - ${account_adjustment:,.2f} - ${coverage_amount:,.2f} = "
                  f"${projected_spend:,.2f}")
        
        print(f"✅ Consolidated {len(consolidated)} account records")
        logging.info(f"Consolidated {len(consolidated)} account records for {business_date}")
        
        return consolidated
    
    @task()
    def write_monthly_spend_estimates(consolidated: List[Dict[str, Any]], **context):
        """
        Write projected spend data to monthly_spend_estimates table
        """
        print(f"💾 Writing {len(consolidated)} records to monthly_spend_estimates table")
        
        if not consolidated:
            print("⚠️  No projected records to write")
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.monthly_spend_estimates 
                (business_date, account_id, cost_center, projected_spend_usd)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (business_date, account_id, cost_center) DO UPDATE SET
                projected_spend_usd = EXCLUDED.projected_spend_usd
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in consolidated:
                cursor.execute(insert_query, (
                    record['business_date'],
                    record['account_id'],
                    record['cost_center'],
                    record['projected_spend_usd']
                ))
            
            conn.commit()
            print(f"✅ Successfully wrote {len(consolidated)} records to monthly_spend_estimates")
            logging.info(f"Wrote {len(consolidated)} projected records")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to monthly_spend_estimates: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    @task()
    def read_cost_optimization_rules(**context) -> List[Dict[str, Any]]:
        """
        Read cost optimization rules from cost_optimization_rules table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📋 Reading cost optimization rules for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, account_id, cost_center, is_reportable
            FROM public.cost_optimization_rules
            WHERE business_date = %s
            ORDER BY account_id, cost_center
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        ref_data = []
        for row in results:
            ref_data.append({
                'business_date': str(row[0]),
                'account_id': row[1],
                'cost_center': row[2],
                'is_reportable': row[3]
            })
        
        print(f"✅ Found {len(ref_data)} cost optimization rule records for {business_date}")
        logging.info(f"Found {len(ref_data)} cost optimization rule records for {business_date}")
        
        return ref_data
    
    @task()
    def read_monthly_estimates(**context) -> List[Dict[str, Any]]:
        """
        Read projected data from monthly_spend_estimates table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📊 Reading projected data for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, account_id, cost_center, projected_spend_usd
            FROM public.monthly_spend_estimates
            WHERE business_date = %s
            ORDER BY account_id, cost_center
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        consolidated = []
        for row in results:
            consolidated.append({
                'business_date': str(row[0]),
                'account_id': row[1],
                'cost_center': row[2],
                'projected_spend_usd': float(row[3])
            })
        
        print(f"✅ Found {len(consolidated)} projected records for {business_date}")
        logging.info(f"Found {len(consolidated)} projected records for {business_date}")
        
        return consolidated
    
    @task()
    def savings_projection_calc(
        consolidated: List[Dict[str, Any]],
        ref_data: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Calculate savings projection by joining projected data with optimization rules
        and filtering for reportable accounts only.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🔒 Calculating savings projection for {business_date}")
        
        # Build lookup for reportable accounts
        reportable_lookup = {}
        for ref in ref_data:
            if ref['is_reportable']:
                key = (ref['account_id'], ref['cost_center'])
                reportable_lookup[key] = True
        
        # Filter projected data to only reportable accounts
        projection_records = []
        total_value = 0
        
        for record in consolidated:
            key = (record['account_id'], record['cost_center'])
            
            if key in reportable_lookup:
                projection_records.append({
                    'business_date': record['business_date'],
                    'account_id': record['account_id'],
                    'cost_center': record['cost_center'],
                    'projected_spend_usd': record['projected_spend_usd']
                })
                total_value += record['projected_spend_usd']
                
                print(f"  Reportable: Account {record['account_id']} ({record['cost_center']}): "
                      f"${record['projected_spend_usd']:,.2f}")
        
        print(f"✅ Found {len(projection_records)} reportable account records")
        print(f"   Total projected value: ${total_value:,.2f}")
        logging.info(f"Found {len(projection_records)} reportable records with total value: {total_value}")
        
        return projection_records
    
    @task()
    def write_spend_savings_report(projection_records: List[Dict[str, Any]], **context):
        """
        Write final projection report to spend_savings_report table
        """
        print(f"💾 Writing {len(projection_records)} records to spend_savings_report table")
        
        records_written = 0
        total_value = 0
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        if not projection_records:
            print("⚠️  No projection records to write")
            send_success_webhook(context, records_written, total_value, business_date)
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.spend_savings_report 
                (business_date, account_id, cost_center, projected_spend_usd)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (business_date, account_id, cost_center) DO UPDATE SET
                projected_spend_usd = EXCLUDED.projected_spend_usd
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in projection_records:
                cursor.execute(insert_query, (
                    record['business_date'],
                    record['account_id'],
                    record['cost_center'],
                    record['projected_spend_usd']
                ))
                
                records_written += 1
                total_value += record['projected_spend_usd']
            
            conn.commit()
            
            print(f"✅ Successfully wrote {records_written} records to spend_savings_report")
            logging.info(f"Wrote {records_written} projection records with total value: {total_value}")
            
            context['ti'].xcom_push(key='records_processed', value=records_written)
            context['ti'].xcom_push(key='total_value', value=total_value)
            context['ti'].xcom_push(key='business_date', value=business_date)
            
            send_success_webhook(context, records_written, total_value, business_date)
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to spend_savings_report: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    # Phase 1: Read upstream data in parallel
    billing_items = read_cloud_billing_items()
    account_meta = read_account_metadata()
    coverage = read_savings_plan_coverage()
    
    # Phase 2: Join spend projection data
    consolidated = spend_projection_join(billing_items, account_meta, coverage)
    
    # Phase 3: Write projected data
    write_consolidated = write_monthly_spend_estimates(consolidated)
    
    # Phase 4: Read data for projection calculation (after write completes)
    consolidated_data = read_monthly_estimates()
    ref_data = read_cost_optimization_rules()
    
    # Set dependencies - projected read must wait for write
    write_consolidated >> consolidated_data
    
    # Phase 5: Calculate projection (join with ref data, filter reportable)
    projections = savings_projection_calc(consolidated_data, ref_data)
    
    # Phase 6: Write final report
    write_spend_savings_report(projections)


dag = cloud_spend_projection_pipeline()