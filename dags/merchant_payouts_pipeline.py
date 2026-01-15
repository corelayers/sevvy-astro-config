"""
## Merchant Payouts Pipeline

This DAG calculates daily merchant payout amounts in USD:
1. Reads payment transfers from payment_transfers table
2. Reads FX market data (exchange rates) from fx_market_data table
3. Converts transfer amounts from local currency to USD using FX rates
4. Aggregates payout totals by merchant
5. Writes results to merchant_payouts_daily table
6. Sends success notification webhook

The pipeline processes data for a specific business date and calculates
the total payout value for each merchant in USD.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging
import json

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests


def send_success_webhook(context, payouts_processed, total_payout_value, business_date):
    """
    Send success webhook notification to the monitoring endpoint
    
    Args:
        context: Airflow context containing DAG run and task instance details
        payouts_processed: Number of merchant payouts written to database
        total_payout_value: Total value of all payouts in USD
        business_date: The business date for the payouts
    """
    print("🔔 Sending DAG success webhook notification")
    logging.info("Sending DAG success webhook notification")
    
    try:
        # Extract context information
        dag_run = context.get('dag_run')
        dag = context.get('dag')
        execution_date = context.get('execution_date') or context.get('logical_date')
        
        # Prepare webhook payload
        webhook_payload = {
            "event_type": "dag_success",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "dag_id": dag.dag_id,
            "task_id": "write_merchant_payouts_daily",
            "run_id": dag_run.run_id,
            "organization_id": "cmfhfglh40k9h01rbszv8zhxx",
            "deployment_id": "cmfhfmdq70kb601rb5wu0d2nd",
            "dag_details": {
                "dag_id": dag.dag_id,
                "execution_date": execution_date.isoformat(),
                "state": "success",
                "log_url": f"https://cmfhfglh40k9h01rbszv8zhxx.astronomer.run/dwu0d2nd/dags/{dag.dag_id}/grid",
                "payouts_processed": payouts_processed,
                "total_payout_value": float(total_payout_value),
                "business_date": business_date
            },
            "environment": "production"
        }
        
        print(f"📦 DAG Success Webhook payload: {json.dumps(webhook_payload, indent=2)}")
        
        # Send webhook request
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


# Default arguments for the DAG
default_args = {
    'owner': 'sevvy-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG using TaskFlow API
@dag(
    'merchant_payouts_pipeline',
    default_args=default_args,
    description='ETL pipeline for calculating merchant payout values in USD',
    schedule='@daily',
    catchup=False,
    tags=['etl', 'finance', 'payments', 'production'],
)
def merchant_payouts_pipeline():
    """Main DAG definition using TaskFlow API"""
    
    @task()
    def read_payment_transfers(**context) -> List[Dict[str, Any]]:
        """
        Read payment transfers from payment_transfers table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"💳 Reading payment transfers for business date: {business_date}")
        
        # Get database connection
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        # Query to fetch payment transfers
        query = """
            SELECT transfer_id, merchant_id, amount_local, currency_code, fee_local, created_at
            FROM public.payment_transfers
            WHERE DATE(created_at) = %s
            ORDER BY merchant_id, transfer_id
        """
        
        # Execute query
        results = pg_hook.get_records(query, parameters=[business_date])
        
        # Convert to list of dictionaries
        transfers = []
        for row in results:
            transfers.append({
                'transfer_id': row[0],
                'merchant_id': row[1],
                'amount_local': float(row[2]),
                'currency_code': row[3],
                'fee_local': float(row[4]) if row[4] else 0.0,
                'created_at': str(row[5])
            })
        
        print(f"✅ Found {len(transfers)} payment transfers for {business_date}")
        logging.info(f"Found {len(transfers)} payment transfers for {business_date}")
        
        return transfers
    
    @task()
    def read_fx_market_data(**context) -> List[Dict[str, Any]]:
        """
        Read FX market data (exchange rates) from fx_market_data table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"💱 Reading FX market data for business date: {business_date}")
        
        # Get database connection
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        # Query to fetch FX rates
        query = """
            SELECT rate_date, from_currency, to_currency, mid_rate, quote_source_id, source_weight, ingested_at
            FROM public.fx_market_data
            WHERE rate_date = %s
            ORDER BY from_currency, to_currency
        """
        
        # Execute query
        results = pg_hook.get_records(query, parameters=[business_date])
        
        # Convert to list of dictionaries
        fx_rates = []
        for row in results:
            fx_rates.append({
                'rate_date': str(row[0]),
                'from_currency': row[1],
                'to_currency': row[2],
                'mid_rate': float(row[3]),
                'quote_source_id': row[4],
                'source_weight': float(row[5]) if row[5] else None,
                'ingested_at': str(row[6]) if row[6] else None
            })
        
        print(f"✅ Found {len(fx_rates)} FX rate records for {business_date}")
        logging.info(f"Found {len(fx_rates)} FX rate records for {business_date}")
        
        return fx_rates
    
    @task()
    def calculate_payouts(transfers: List[Dict[str, Any]], 
                         fx_rates: List[Dict[str, Any]],
                         **context) -> List[Dict[str, Any]]:
        """
        Calculate USD payout amounts by matching transfers with FX rates
        and aggregating by merchant
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🧮 Calculating payouts for {len(transfers)} transfers")
        
        # Track merchant totals
        merchant_totals = {}
        
        for transfer in transfers:
            merchant_id = transfer['merchant_id']
            currency_code = transfer['currency_code']
            amount_local = transfer['amount_local']
            fee_local = transfer['fee_local']
            
            # Initialize merchant totals if not exists
            if merchant_id not in merchant_totals:
                merchant_totals[merchant_id] = {
                    'amount_usd': 0.0,
                    'fees_usd': 0.0,
                    'transfer_count': 0
                }
            
            # Handle USD transfers directly
            if currency_code == 'USD':
                merchant_totals[merchant_id]['amount_usd'] += amount_local
                merchant_totals[merchant_id]['fees_usd'] += fee_local
                merchant_totals[merchant_id]['transfer_count'] += 1
                print(f"  Transfer {transfer['transfer_id']}: ${amount_local:,.2f} USD (direct)")
                continue
            
            # Find matching FX rate and convert to USD
            matched = False
            for fx_rate in fx_rates:
                if (fx_rate['from_currency'] == currency_code and 
                    fx_rate['to_currency'] == 'USD'):
                    
                    rate = fx_rate['mid_rate']
                    amount_usd = amount_local * rate
                    fee_usd = fee_local * rate
                    
                    merchant_totals[merchant_id]['amount_usd'] += amount_usd
                    merchant_totals[merchant_id]['fees_usd'] += fee_usd
                    merchant_totals[merchant_id]['transfer_count'] += 1
                    
                    matched = True
                    print(f"  Transfer {transfer['transfer_id']}: "
                          f"{amount_local:,.2f} {currency_code} × {rate} = ${amount_usd:,.2f} USD")
            
            if not matched:
                logging.warning(f"No FX rate found for {currency_code} -> USD, "
                               f"skipping transfer {transfer['transfer_id']}")
        
        # Convert to list of payout records
        payouts = []
        total_amount = 0
        
        for merchant_id, totals in merchant_totals.items():
            payouts.append({
                'business_date': business_date,
                'merchant_id': merchant_id,
                'amount_usd': totals['amount_usd'],
                'transfer_count': totals['transfer_count'],
                'total_fees_usd': totals['fees_usd']
            })
            total_amount += totals['amount_usd']
        
        print(f"✅ Calculated payouts for {len(payouts)} merchants")
        print(f"   Total payout value: ${total_amount:,.2f}")
        logging.info(f"Calculated {len(payouts)} merchant payouts with total value: {total_amount}")
        
        return payouts
    
    @task()
    def write_merchant_payouts_daily(payouts: List[Dict[str, Any]], **context):
        """
        Write calculated payouts to merchant_payouts_daily table
        """
        print(f"💾 Writing {len(payouts)} merchant payouts to merchant_payouts_daily table")
        
        # Initialize variables for webhook
        payouts_written = 0
        total_payout_value = 0
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        if not payouts:
            print("⚠️  No payouts to write")
            send_success_webhook(context, payouts_written, total_payout_value, business_date)
            return
        
        # Get database connection
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        # Insert query with upsert
        insert_query = """
            INSERT INTO public.merchant_payouts_daily 
                (business_date, merchant_id, amount_usd, transfer_count, total_fees_usd)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (business_date, merchant_id) DO UPDATE SET
                amount_usd = EXCLUDED.amount_usd,
                transfer_count = EXCLUDED.transfer_count,
                total_fees_usd = EXCLUDED.total_fees_usd
        """
        
        # Execute inserts in a transaction
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for payout in payouts:
                cursor.execute(insert_query, (
                    payout['business_date'],
                    payout['merchant_id'],
                    payout['amount_usd'],
                    payout['transfer_count'],
                    payout['total_fees_usd']
                ))
                
                payouts_written += 1
                total_payout_value += payout['amount_usd']
            
            # Commit the transaction
            conn.commit()
            
            print(f"✅ Successfully wrote {payouts_written} merchant payouts to merchant_payouts_daily table")
            logging.info(f"Wrote {payouts_written} payouts with total value: {total_payout_value}")
            
            # Push statistics to XCom for success callback
            context['ti'].xcom_push(key='payouts_processed', value=payouts_written)
            context['ti'].xcom_push(key='total_payout_value', value=total_payout_value)
            context['ti'].xcom_push(key='business_date', value=business_date)
            
            # Send success webhook after successful write
            send_success_webhook(context, payouts_written, total_payout_value, business_date)
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to merchant_payouts_daily: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    # Define task dependencies with parallel execution for reads
    transfers = read_payment_transfers()
    fx_rates = read_fx_market_data()
    
    # Calculate payouts after both transfers and FX rates are ready
    payouts = calculate_payouts(transfers, fx_rates)
    
    # Write payouts to database
    write_merchant_payouts_daily(payouts)

# Instantiate the DAG
dag = merchant_payouts_pipeline()
