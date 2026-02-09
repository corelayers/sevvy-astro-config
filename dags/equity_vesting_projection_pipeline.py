"""
## Equity Vesting Projection Pipeline

This DAG projects equity vesting values and generates tax impact advisory insights:
1. Reads grant agreements from grant_agreements table
2. Reads employee profiles from employee_profiles table
3. Reads stock price data from stock_price_data table
4. Calculates vesting values (shares × price) per employee/grant
5. Writes consolidated projections to vesting_projections_consolidated table
6. Joins with client account data to filter active advisory clients
7. Computes AMT exposure and tax liabilities for final report
8. Writes advisory insights to advisory_insights_report table
9. Sends success notification webhook

The pipeline processes data for a specific business date and calculates
projected vesting values and tax implications for wealth advisors.
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
        total_value: Total projected vesting value
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
            "task_id": "write_advisory_insights_report",
            "run_id": dag_run.run_id,
            "organization_id": "cmfhfglh40k9h01rbszv8zhxx",
            "deployment_id": "cmfhfmdq70kb601rb5wu0d2nd",
            "dag_details": {
                "dag_id": dag.dag_id,
                "execution_date": execution_date.isoformat(),
                "state": "success",
                "log_url": f"https://cmfhfglh40k9h01rbszv8zhxx.astronomer.run/dwu0d2nd/dags/{dag.dag_id}/grid",
                "records_processed": records_processed,
                "total_projected_vesting_value": float(total_value),
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
    'equity_vesting_projection_pipeline',
    default_args=default_args,
    description='ETL pipeline for projecting equity vesting values and generating tax impact advisory insights',
    schedule='@daily',
    catchup=False,
    tags=['etl', 'wealth-advisory', 'equity-comp', 'production'],
)
def equity_vesting_projection_pipeline():
    """Main DAG definition using TaskFlow API"""
    
    @task()
    def read_grant_agreements(**context) -> List[Dict[str, Any]]:
        """
        Read grant agreements from grant_agreements table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"💰 Reading grant agreements for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, grant_id, employee_id, shares_granted
            FROM public.grant_agreements
            WHERE business_date = %s
            ORDER BY grant_id, employee_id
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        grants = []
        for row in results:
            grants.append({
                'business_date': str(row[0]),
                'grant_id': row[1],
                'employee_id': row[2],
                'shares_granted': float(row[3])
            })
        
        print(f"✅ Found {len(grants)} grant agreement records for {business_date}")
        logging.info(f"Found {len(grants)} grant agreement records for {business_date}")
        
        return grants
    
    @task()
    def read_employee_profiles(**context) -> List[Dict[str, Any]]:
        """
        Read employee profiles from employee_profiles table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"💸 Reading employee profiles for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, grant_id, employee_id, shares_vested
            FROM public.employee_profiles
            WHERE business_date = %s
            ORDER BY grant_id, employee_id
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        employees = []
        for row in results:
            employees.append({
                'business_date': str(row[0]),
                'grant_id': row[1],
                'employee_id': row[2],
                'shares_vested': float(row[3])
            })
        
        print(f"✅ Found {len(employees)} employee profile records for {business_date}")
        logging.info(f"Found {len(employees)} employee profile records for {business_date}")
        
        return employees
    
    @task()
    def read_stock_price_data(**context) -> List[Dict[str, Any]]:
        """
        Read stock price data from stock_price_data table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🏦 Reading stock price data for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, grant_id, employee_id, price_per_share
            FROM public.stock_price_data
            WHERE business_date = %s
            ORDER BY grant_id, employee_id
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        prices = []
        for row in results:
            prices.append({
                'business_date': str(row[0]),
                'grant_id': row[1],
                'employee_id': row[2],
                'price_per_share': float(row[3])
            })
        
        print(f"✅ Found {len(prices)} stock price data records for {business_date}")
        logging.info(f"Found {len(prices)} stock price data records for {business_date}")
        
        return prices
    
    @task()
    def vesting_value_calculation(
        grants: List[Dict[str, Any]],
        employees: List[Dict[str, Any]],
        prices: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Calculate vesting values by calculating shares granted - shares vested - price per share for each employee.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🧮 Calculating vesting values for {business_date}")
        
        grant_lookup = {}
        for grant in grants:
            key = (grant['grant_id'], grant['employee_id'])
            if key not in grant_lookup:
                grant_lookup[key] = grant['shares_granted']
        
        employee_lookup = {}
        for employee in employees:
            key = (employee['grant_id'], employee['employee_id'])
            if key not in employee_lookup:
                employee_lookup[key] = employee['shares_vested']
        
        price_lookup = {}
        for price in prices:
            key = (price['grant_id'], price['employee_id'])
            if key not in price_lookup:
                price_lookup[key] = price['price_per_share']
        
        # Get all unique employee/grant combinations
        all_keys = set(grant_lookup.keys()) | set(employee_lookup.keys()) | set(price_lookup.keys())
        
        consolidated = []
        for grant_id, employee_id in all_keys:
            shares_granted = grant_lookup.get((grant_id, employee_id), 0.0)
            shares_vested = employee_lookup.get((grant_id, employee_id), 0.0)
            price_per_share = price_lookup.get((grant_id, employee_id), 0.0)
            
            projected_vesting_value_usd = shares_granted - shares_vested - price_per_share
            
            consolidated.append({
                'business_date': business_date,
                'grant_id': grant_id,
                'employee_id': employee_id,
                'projected_vesting_value_usd': projected_vesting_value_usd
            })
            
            print(f"  Employee {employee_id} Grant {grant_id}: "
                  f"${shares_granted:,.2f} - ${shares_vested:,.2f} - ${price_per_share:,.2f} = "
                  f"${projected_vesting_value_usd:,.2f}")
        
        print(f"✅ Consolidated {len(consolidated)} employee grant records")
        logging.info(f"Consolidated {len(consolidated)} employee grant records for {business_date}")
        
        return consolidated
    
    @task()
    def write_vesting_projections_consolidated(consolidated: List[Dict[str, Any]], **context):
        """
        Write consolidated vesting projections to vesting_projections_consolidated table
        """
        print(f"💾 Writing {len(consolidated)} records to vesting_projections_consolidated table")
        
        if not consolidated:
            print("⚠️  No consolidated records to write")
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.vesting_projections_consolidated 
                (business_date, grant_id, employee_id, projected_vesting_value_usd)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (business_date, grant_id, employee_id) DO UPDATE SET
                projected_vesting_value_usd = EXCLUDED.projected_vesting_value_usd
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in consolidated:
                cursor.execute(insert_query, (
                    record['business_date'],
                    record['grant_id'],
                    record['employee_id'],
                    record['projected_vesting_value_usd']
                ))
            
            conn.commit()
            print(f"✅ Successfully wrote {len(consolidated)} records to vesting_projections_consolidated")
            logging.info(f"Wrote {len(consolidated)} consolidated records")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to vesting_projections_consolidated: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    @task()
    def read_client_account_data(**context) -> List[Dict[str, Any]]:
        """
        Read client account data from client_account_data table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📋 Reading client account data for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, grant_id, employee_id, is_advisory_client
            FROM public.client_account_data
            WHERE business_date = %s
            ORDER BY grant_id, employee_id
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        ref_data = []
        for row in results:
            ref_data.append({
                'business_date': str(row[0]),
                'grant_id': row[1],
                'employee_id': row[2],
                'is_advisory_client': row[3]
            })
        
        print(f"✅ Found {len(ref_data)} client account records for {business_date}")
        logging.info(f"Found {len(ref_data)} client account records for {business_date}")
        
        return ref_data
    
    @task()
    def read_consolidated_vesting_data(**context) -> List[Dict[str, Any]]:
        """
        Read consolidated vesting data from vesting_projections_consolidated table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📊 Reading consolidated vesting data for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, grant_id, employee_id, projected_vesting_value_usd
            FROM public.vesting_projections_consolidated
            WHERE business_date = %s
            ORDER BY grant_id, employee_id
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        consolidated = []
        for row in results:
            consolidated.append({
                'business_date': str(row[0]),
                'grant_id': row[1],
                'employee_id': row[2],
                'projected_vesting_value_usd': float(row[3])
            })
        
        print(f"✅ Found {len(consolidated)} consolidated vesting records for {business_date}")
        logging.info(f"Found {len(consolidated)} consolidated vesting records for {business_date}")
        
        return consolidated
    
    @task()
    def tax_impact_calculation(
        consolidated: List[Dict[str, Any]],
        ref_data: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Calculate tax impact by joining consolidated vesting data with client account data
        and filtering for advisory clients only.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🔒 Calculating tax impact for {business_date}")
        
        # Build lookup for advisory clients
        advisory_client_lookup = {}
        for ref in ref_data:
            if ref['is_advisory_client']:
                key = (ref['grant_id'], ref['employee_id'])
                advisory_client_lookup[key] = True
        
        # Filter consolidated vesting data to only advisory clients
        advisory_records = []
        total_value = 0
        
        for record in consolidated:
            key = (record['grant_id'], record['employee_id'])
            
            if key in advisory_client_lookup:
                advisory_records.append({
                    'business_date': record['business_date'],
                    'grant_id': record['grant_id'],
                    'employee_id': record['employee_id'],
                    'projected_vesting_value_usd': record['projected_vesting_value_usd']
                })
                total_value += record['projected_vesting_value_usd']
                
                print(f"  Advisory Client: Employee {record['employee_id']} Grant {record['grant_id']}: "
                      f"${record['projected_vesting_value_usd']:,.2f}")
        
        print(f"✅ Found {len(advisory_records)} advisory client records")
        print(f"   Total tax impact value: ${total_value:,.2f}")
        logging.info(f"Found {len(advisory_records)} advisory records with total value: {total_value}")
        
        return advisory_records
    
    @task()
    def write_advisory_insights_report(advisory_records: List[Dict[str, Any]], **context):
        """
        Write final advisory insights to advisory_insights_report table
        """
        print(f"💾 Writing {len(advisory_records)} records to advisory_insights_report table")
        
        records_written = 0
        total_value = 0
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        if not advisory_records:
            print("⚠️  No advisory records to write")
            send_success_webhook(context, records_written, total_value, business_date)
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.advisory_insights_report 
                (business_date, grant_id, employee_id, projected_vesting_value_usd)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (business_date, grant_id, employee_id) DO UPDATE SET
                projected_vesting_value_usd = EXCLUDED.projected_vesting_value_usd
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in advisory_records:
                cursor.execute(insert_query, (
                    record['business_date'],
                    record['grant_id'],
                    record['employee_id'],
                    record['projected_vesting_value_usd']
                ))
                
                records_written += 1
                total_value += record['projected_vesting_value_usd']
            
            conn.commit()
            
            print(f"✅ Successfully wrote {records_written} records to advisory_insights_report")
            logging.info(f"Wrote {records_written} advisory records with total value: {total_value}")
            
            context['ti'].xcom_push(key='records_processed', value=records_written)
            context['ti'].xcom_push(key='total_value', value=total_value)
            context['ti'].xcom_push(key='business_date', value=business_date)
            
            send_success_webhook(context, records_written, total_value, business_date)
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to advisory_insights_report: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    # Phase 1: Read upstream data in parallel
    grants = read_grant_agreements()
    employees = read_employee_profiles()
    prices = read_stock_price_data()
    
    # Phase 2: Calculate vesting values
    consolidated = vesting_value_calculation(grants, employees, prices)
    
    # Phase 3: Write consolidated vesting projections
    write_consolidated = write_vesting_projections_consolidated(consolidated)
    
    # Phase 4: Read data for tax impact calculation (after write completes)
    consolidated_data = read_consolidated_vesting_data()
    ref_data = read_client_account_data()
    
    # Set dependencies - consolidated read must wait for write
    write_consolidated >> consolidated_data
    
    # Phase 5: Calculate tax impact (join with client account data, filter advisory clients)
    advisory_insights = tax_impact_calculation(consolidated_data, ref_data)
    
    # Phase 6: Write final advisory insights report
    write_advisory_insights_report(advisory_insights)


dag = equity_vesting_projection_pipeline()