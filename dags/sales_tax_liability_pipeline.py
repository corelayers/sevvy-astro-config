"""
## Sales Tax Liability Pipeline

This DAG calculates sales tax liability across jurisdictions and generates filing reports:
1. Reads sales transactions from sales_transactions table
2. Reads tax rates from tax_rates table
3. Reads product taxability rules from product_taxability table
4. Calculates tax liability (transactions * rates * taxability) per transaction/jurisdiction
5. Writes calculated data to calculated_tax_liability table
6. Joins with jurisdiction filing rules to filter reportable jurisdictions
7. Writes final report to tax_filing_report table
8. Sends success notification webhook

The pipeline processes data for a specific business date and calculates
the total tax liability for each jurisdiction and filing period.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging
import json

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests


# Threshold filter removed - include all calculated values for anomaly detection


def send_success_webhook(context, records_processed, total_value, business_date):
    """
    Send success webhook notification to the monitoring endpoint
    
    Args:
        context: Airflow context containing DAG run and task instance details
        records_processed: Number of records written to database
        total_value: Total tax liability value
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
            "task_id": "write_tax_filing_report",
            "run_id": dag_run.run_id,
            "organization_id": "cmfhfglh40k9h01rbszv8zhxx",
            "deployment_id": "cmfhfmdq70kb601rb5wu0d2nd",
            "dag_details": {
                "dag_id": dag.dag_id,
                "execution_date": execution_date.isoformat(),
                "state": "success",
                "log_url": f"https://cmfhfglh40k9h01rbszv8zhxx.astronomer.run/dwu0d2nd/dags/{dag.dag_id}/grid",
                "records_processed": records_processed,
                "total_tax_liability_value": float(total_value),
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
    'sales_tax_liability_pipeline',
    default_args=default_args,
    description='ETL pipeline for calculating sales tax liability across jurisdictions and generating filing reports',
    schedule='@daily',
    catchup=False,
    tags=['etl', 'tax-compliance', 'sales-tax', 'production'],
)
def sales_tax_liability_pipeline():
    """Main DAG definition using TaskFlow API"""
    
    @task()
    def read_sales_transactions(**context) -> List[Dict[str, Any]]:
        """
        Read sales transactions from sales_transactions table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"💰 Reading sales transactions for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, transaction_id, jurisdiction, transaction_amount_usd
            FROM public.sales_transactions
            WHERE business_date = %s
            ORDER BY transaction_id, jurisdiction
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        transactions = []
        for row in results:
            transactions.append({
                'business_date': str(row[0]),
                'transaction_id': row[1],
                'jurisdiction': row[2],
                'transaction_amount_usd': float(row[3])
            })
        
        print(f"✅ Found {len(transactions)} sales transaction records for {business_date}")
        logging.info(f"Found {len(transactions)} sales transaction records for {business_date}")
        
        return transactions
    
    @task()
    def read_tax_rates(**context) -> List[Dict[str, Any]]:
        """
        Read tax rates from tax_rates table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"💸 Reading tax rates for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, transaction_id, jurisdiction, tax_rate
            FROM public.tax_rates
            WHERE business_date = %s
            ORDER BY transaction_id, jurisdiction
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        rates = []
        for row in results:
            rates.append({
                'business_date': str(row[0]),
                'transaction_id': row[1],
                'jurisdiction': row[2],
                'tax_rate': float(row[3])
            })
        
        print(f"✅ Found {len(rates)} tax rate records for {business_date}")
        logging.info(f"Found {len(rates)} tax rate records for {business_date}")
        
        return rates
    
    @task()
    def read_product_taxability(**context) -> List[Dict[str, Any]]:
        """
        Read product taxability rules from product_taxability table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🏦 Reading product taxability rules for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, transaction_id, jurisdiction, taxability_factor
            FROM public.product_taxability
            WHERE business_date = %s
            ORDER BY transaction_id, jurisdiction
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        taxability = []
        for row in results:
            taxability.append({
                'business_date': str(row[0]),
                'transaction_id': row[1],
                'jurisdiction': row[2],
                'taxability_factor': float(row[3])
            })
        
        print(f"✅ Found {len(taxability)} product taxability records for {business_date}")
        logging.info(f"Found {len(taxability)} product taxability records for {business_date}")
        
        return taxability
    
    @task()
    def tax_liability_calculation(
        transactions: List[Dict[str, Any]],
        rates: List[Dict[str, Any]],
        taxability: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Calculate tax liability by computing transactions * rates * taxability for each transaction.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🧮 Calculating tax liability for {business_date}")
        
        transaction_lookup = {}
        for transaction in transactions:
            key = (transaction['transaction_id'], transaction['jurisdiction'])
            if key not in transaction_lookup:
                transaction_lookup[key] = transaction['transaction_amount_usd']
        
        rate_lookup = {}
        for rate in rates:
            key = (rate['transaction_id'], rate['jurisdiction'])
            if key not in rate_lookup:
                rate_lookup[key] = rate['tax_rate']
        
        taxability_lookup = {}
        for taxability_item in taxability:
            key = (taxability_item['transaction_id'], taxability_item['jurisdiction'])
            if key not in taxability_lookup:
                taxability_lookup[key] = taxability_item['taxability_factor']
        
        # Get all unique transaction/jurisdiction combinations
        all_keys = set(transaction_lookup.keys()) | set(rate_lookup.keys()) | set(taxability_lookup.keys())
        
        calculated = []
        for transaction_id, jurisdiction in all_keys:
            transaction_amount = transaction_lookup.get((transaction_id, jurisdiction), 0.0)
            tax_rate = rate_lookup.get((transaction_id, jurisdiction), 0.0)
            taxability_factor = taxability_lookup.get((transaction_id, jurisdiction), 0.0)
            
            total_tax_liability_usd = transaction_amount - tax_rate - taxability_factor
            
            calculated.append({
                'business_date': business_date,
                'transaction_id': transaction_id,
                'jurisdiction': jurisdiction,
                'total_tax_liability_usd': total_tax_liability_usd
            })
            
            print(f"  Transaction {transaction_id} ({jurisdiction}): "
                  f"${transaction_amount:,.2f} - ${tax_rate:,.2f} - ${taxability_factor:,.2f} = "
                  f"${total_tax_liability_usd:,.2f}")
        
        print(f"✅ Calculated {len(calculated)} transaction records")
        logging.info(f"Calculated {len(calculated)} transaction records for {business_date}")
        
        return calculated
    
    @task()
    def write_calculated_tax_liability(calculated: List[Dict[str, Any]], **context):
        """
        Write calculated tax liability data to calculated_tax_liability table
        """
        print(f"💾 Writing {len(calculated)} records to calculated_tax_liability table")
        
        if not calculated:
            print("⚠️  No calculated records to write")
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.calculated_tax_liability 
                (business_date, transaction_id, jurisdiction, total_tax_liability_usd)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (business_date, transaction_id, jurisdiction) DO UPDATE SET
                total_tax_liability_usd = EXCLUDED.total_tax_liability_usd
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in calculated:
                cursor.execute(insert_query, (
                    record['business_date'],
                    record['transaction_id'],
                    record['jurisdiction'],
                    record['total_tax_liability_usd']
                ))
            
            conn.commit()
            print(f"✅ Successfully wrote {len(calculated)} records to calculated_tax_liability")
            logging.info(f"Wrote {len(calculated)} calculated records")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to calculated_tax_liability: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    @task()
    def read_jurisdiction_filing_rules(**context) -> List[Dict[str, Any]]:
        """
        Read jurisdiction filing rules from jurisdiction_filing_rules table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📋 Reading jurisdiction filing rules for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, transaction_id, jurisdiction, is_reportable
            FROM public.jurisdiction_filing_rules
            WHERE business_date = %s
            ORDER BY transaction_id, jurisdiction
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        ref_data = []
        for row in results:
            ref_data.append({
                'business_date': str(row[0]),
                'transaction_id': row[1],
                'jurisdiction': row[2],
                'is_reportable': row[3]
            })
        
        print(f"✅ Found {len(ref_data)} jurisdiction filing rule records for {business_date}")
        logging.info(f"Found {len(ref_data)} jurisdiction filing rule records for {business_date}")
        
        return ref_data
    
    @task()
    def read_calculated_liability_data(**context) -> List[Dict[str, Any]]:
        """
        Read calculated liability data from calculated_tax_liability table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📊 Reading calculated liability data for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, transaction_id, jurisdiction, total_tax_liability_usd
            FROM public.calculated_tax_liability
            WHERE business_date = %s
            ORDER BY transaction_id, jurisdiction
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        calculated = []
        for row in results:
            calculated.append({
                'business_date': str(row[0]),
                'transaction_id': row[1],
                'jurisdiction': row[2],
                'total_tax_liability_usd': float(row[3])
            })
        
        print(f"✅ Found {len(calculated)} calculated liability records for {business_date}")
        logging.info(f"Found {len(calculated)} calculated liability records for {business_date}")
        
        return calculated
    
    @task()
    def filing_period_aggregation(
        calculated: List[Dict[str, Any]],
        ref_data: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Aggregate filing period data by joining calculated liability data with filing rules
        and filtering for reportable jurisdictions only.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🔒 Aggregating filing period data for {business_date}")
        
        # Build lookup for reportable jurisdictions
        reportable_lookup = {}
        for ref in ref_data:
            if ref['is_reportable']:
                key = (ref['transaction_id'], ref['jurisdiction'])
                reportable_lookup[key] = True
        
        # Filter calculated data to only reportable jurisdictions
        filing_records = []
        total_value = 0
        
        for record in calculated:
            key = (record['transaction_id'], record['jurisdiction'])
            
            if key in reportable_lookup:
                filing_records.append({
                    'business_date': record['business_date'],
                    'transaction_id': record['transaction_id'],
                    'jurisdiction': record['jurisdiction'],
                    'total_tax_liability_usd': record['total_tax_liability_usd']
                })
                total_value += record['total_tax_liability_usd']
                
                print(f"  Reportable: Transaction {record['transaction_id']} ({record['jurisdiction']}): "
                      f"${record['total_tax_liability_usd']:,.2f}")
        
        print(f"✅ Found {len(filing_records)} reportable jurisdiction records")
        print(f"   Total filing value: ${total_value:,.2f}")
        logging.info(f"Found {len(filing_records)} reportable records with total value: {total_value}")
        
        return filing_records
    
    @task()
    def write_tax_filing_report(filing_records: List[Dict[str, Any]], **context):
        """
        Write final filing report to tax_filing_report table
        """
        print(f"💾 Writing {len(filing_records)} records to tax_filing_report table")
        
        records_written = 0
        total_value = 0
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        if not filing_records:
            print("⚠️  No filing records to write")
            send_success_webhook(context, records_written, total_value, business_date)
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.tax_filing_report 
                (business_date, transaction_id, jurisdiction, total_tax_liability_usd)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (business_date, transaction_id, jurisdiction) DO UPDATE SET
                total_tax_liability_usd = EXCLUDED.total_tax_liability_usd
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in filing_records:
                cursor.execute(insert_query, (
                    record['business_date'],
                    record['transaction_id'],
                    record['jurisdiction'],
                    record['total_tax_liability_usd']
                ))
                
                records_written += 1
                total_value += record['total_tax_liability_usd']
            
            conn.commit()
            
            print(f"✅ Successfully wrote {records_written} records to tax_filing_report")
            logging.info(f"Wrote {records_written} filing records with total value: {total_value}")
            
            context['ti'].xcom_push(key='records_processed', value=records_written)
            context['ti'].xcom_push(key='total_value', value=total_value)
            context['ti'].xcom_push(key='business_date', value=business_date)
            
            send_success_webhook(context, records_written, total_value, business_date)
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to tax_filing_report: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    # Phase 1: Read upstream data in parallel
    transactions = read_sales_transactions()
    rates = read_tax_rates()
    taxability = read_product_taxability()
    
    # Phase 2: Calculate tax liability
    calculated = tax_liability_calculation(transactions, rates, taxability)
    
    # Phase 3: Write calculated data
    write_calculated = write_calculated_tax_liability(calculated)
    
    # Phase 4: Read data for filing aggregation (after write completes)
    calculated_data = read_calculated_liability_data()
    ref_data = read_jurisdiction_filing_rules()
    
    # Set dependencies - calculated read must wait for write
    write_calculated >> calculated_data
    
    # Phase 5: Aggregate filing period (join with ref data, filter reportable)
    filing_aggregation = filing_period_aggregation(calculated_data, ref_data)
    
    # Phase 6: Write final report
    write_tax_filing_report(filing_aggregation)


dag = sales_tax_liability_pipeline()