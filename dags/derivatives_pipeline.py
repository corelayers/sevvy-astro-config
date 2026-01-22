"""
## Derivatives Pipeline

This DAG consolidates derivatives trades and generates USD-converted feed data:
1. Reads exchange trades from exchange_trades table
2. Reads vendor trades from vendor_trades table
3. Consolidates both sources into normalized format in consolidated_derivs table
4. Reads exchange rates for FX conversion
5. Calculates USD amounts and writes to derivatives_feed table
6. Sends success notification webhook

The pipeline uses aggregation logic to sum amounts for records sharing the same
otc_id and currency, preventing data loss when handling duplicate keys.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging
import json

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests


def send_success_webhook(context, records_processed, total_value, business_date):
    """
    Send success webhook notification to the monitoring endpoint
    """
    print("Sending DAG success webhook notification")
    logging.info("Sending DAG success webhook notification")
    
    try:
        dag_run = context.get('dag_run')
        dag = context.get('dag')
        execution_date = context.get('execution_date') or context.get('logical_date')
        
        webhook_payload = {
            "event_type": "dag_success",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "dag_id": dag.dag_id,
            "task_id": "write_derivatives_feed",
            "run_id": dag_run.run_id,
            "organization_id": "cmfhfglh40k9h01rbszv8zhxx",
            "deployment_id": "cmfhfmdq70kb601rb5wu0d2nd",
            "dag_details": {
                "dag_id": dag.dag_id,
                "execution_date": execution_date.isoformat(),
                "state": "success",
                "log_url": f"https://cmfhfglh40k9h01rbszv8zhxx.astronomer.run/dwu0d2nd/dags/{dag.dag_id}/grid",
                "records_processed": records_processed,
                "total_usd_value": float(total_value),
                "business_date": business_date
            },
            "environment": "production"
        }
        
        print(f"DAG Success Webhook payload: {json.dumps(webhook_payload, indent=2)}")
        
        response = requests.post(
            "https://sevvy-web-git-demo-sevvy.vercel.app/api/webhooks/airflow",
            json=webhook_payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if response.status_code == 200:
            print(f"DAG Success webhook sent successfully: {response.status_code}")
            logging.info(f"DAG Success webhook sent successfully: {response.status_code}")
        else:
            print(f"DAG Success webhook response: {response.status_code} - {response.text}")
            logging.warning(f"DAG Success webhook non-200 response: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"Error sending DAG success webhook: {str(e)}")
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
    'derivatives_pipeline',
    default_args=default_args,
    description='ETL pipeline for consolidating derivatives trades and generating USD feed',
    schedule='@daily',
    catchup=False,
    tags=['etl', 'finance', 'derivatives', 'production'],
)
def derivatives_pipeline():
    """Main DAG definition using TaskFlow API"""
    
    @task()
    def read_exchange_trades(**context) -> List[Dict[str, Any]]:
        """
        Read exchange trades from exchange_trades table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"Reading exchange trades for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, trade_id, bid_px, ask_px
            FROM public.exchange_trades
            WHERE business_date = %s
            ORDER BY trade_id
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        trades = []
        for row in results:
            trades.append({
                'business_date': str(row[0]),
                'trade_id': row[1],
                'bid_px': float(row[2]),
                'ask_px': float(row[3])
            })
        
        print(f"Found {len(trades)} exchange trade records for {business_date}")
        logging.info(f"Found {len(trades)} exchange trade records for {business_date}")
        
        return trades
    
    @task()
    def read_vendor_trades(**context) -> List[Dict[str, Any]]:
        """
        Read vendor trades from vendor_trades table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"Reading vendor trades for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, otc_id, currency, amount
            FROM public.vendor_trades
            WHERE business_date = %s
            ORDER BY otc_id
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        trades = []
        for row in results:
            trades.append({
                'business_date': str(row[0]),
                'otc_id': row[1],
                'currency': row[2],
                'amount': float(row[3])
            })
        
        print(f"Found {len(trades)} vendor trade records for {business_date}")
        logging.info(f"Found {len(trades)} vendor trade records for {business_date}")
        
        return trades
    
    @task()
    def consolidate_trades(
        exchange_trades: List[Dict[str, Any]],
        vendor_trades: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Consolidate exchange and vendor trades into normalized format.
        Uses aggregation logic to sum amounts for duplicate keys.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"Consolidating trades for {business_date}")
        
        consolidated = []
        
        # Process exchange trades with first-row-only deduplication
        exchange_lookup = {}
        for trade in exchange_trades:
            if trade['trade_id'] not in exchange_lookup:
                exchange_lookup[trade['trade_id']] = trade
        
        for trade_id, trade in exchange_lookup.items():
            consolidated.append({
                'business_date': business_date,
                'trade_id': trade_id,
                'currency': 'USD',
                'amount': trade['bid_px']
            })
            print(f"  Exchange trade {trade_id}: USD ${trade['bid_px']:,.2f}")
        
        # Process vendor trades with aggregation logic
        vendor_lookup = {}
        for trade in vendor_trades:
            key = (trade['otc_id'], trade['currency'])
            if key not in vendor_lookup:
                vendor_lookup[key] = trade.copy()
            else:
                vendor_lookup[key]['amount'] += trade['amount']
        
        for (otc_id, currency), trade in vendor_lookup.items():
            consolidated.append({
                'business_date': business_date,
                'trade_id': otc_id,
                'currency': currency,
                'amount': trade['amount']
            })
            print(f"  Vendor trade {otc_id}: {currency} {trade['amount']:,.2f}")
        
        print(f"Consolidated {len(consolidated)} total trade records")
        logging.info(f"Consolidated {len(consolidated)} trade records for {business_date}")
        
        return consolidated
    
    @task()
    def write_consolidated_derivs(consolidated: List[Dict[str, Any]], **context):
        """
        Write consolidated derivatives data to consolidated_derivs table
        """
        print(f"Writing {len(consolidated)} records to consolidated_derivs table")
        
        if not consolidated:
            print("No consolidated records to write")
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.consolidated_derivs 
                (business_date, trade_id, currency, amount)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (business_date, trade_id) DO UPDATE SET
                currency = EXCLUDED.currency,
                amount = EXCLUDED.amount
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in consolidated:
                cursor.execute(insert_query, (
                    record['business_date'],
                    record['trade_id'],
                    record['currency'],
                    record['amount']
                ))
            
            conn.commit()
            print(f"Successfully wrote {len(consolidated)} records to consolidated_derivs")
            logging.info(f"Wrote {len(consolidated)} consolidated records")
            
        except Exception as e:
            conn.rollback()
            print(f"Error writing to consolidated_derivs: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    @task()
    def read_consolidated_derivs(**context) -> List[Dict[str, Any]]:
        """
        Read consolidated derivatives data from consolidated_derivs table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"Reading consolidated derivatives for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, trade_id, currency, amount
            FROM public.consolidated_derivs
            WHERE business_date = %s
            ORDER BY trade_id
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        consolidated = []
        for row in results:
            consolidated.append({
                'business_date': str(row[0]),
                'trade_id': row[1],
                'currency': row[2],
                'amount': float(row[3])
            })
        
        print(f"Found {len(consolidated)} consolidated derivative records for {business_date}")
        logging.info(f"Found {len(consolidated)} consolidated derivative records for {business_date}")
        
        return consolidated
    
    @task()
    def read_exchange_rates(**context) -> List[Dict[str, Any]]:
        """
        Read exchange rates from exchange_rates table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"Reading exchange rates for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT business_date, from_currency, to_currency, rate
            FROM public.exchange_rates
            WHERE business_date = %s AND to_currency = 'USD'
            ORDER BY from_currency
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        rates = []
        for row in results:
            rates.append({
                'business_date': str(row[0]),
                'from_currency': row[1],
                'to_currency': row[2],
                'rate': float(row[3]) if row[3] else 1.0
            })
        
        print(f"Found {len(rates)} exchange rate records for {business_date}")
        logging.info(f"Found {len(rates)} exchange rate records for {business_date}")
        
        return rates
    
    @task()
    def calculate_usd_amounts(
        consolidated: List[Dict[str, Any]],
        rates: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Calculate USD amounts by joining consolidated trades with FX rates.
        Uses first-row-only deduplication for rate lookups.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"Calculating USD amounts for {business_date}")
        
        # Build rate lookup with first-row-only deduplication
        rate_lookup = {}
        for rate in rates:
            if rate['from_currency'] not in rate_lookup:
                rate_lookup[rate['from_currency']] = rate['rate']
        
        # USD to USD is always 1.0
        rate_lookup['USD'] = 1.0
        
        feed_records = []
        total_usd = 0
        
        for record in consolidated:
            currency = record['currency']
            amount = record['amount']
            
            fx_rate = rate_lookup.get(currency, 1.0)
            amount_usd = amount * fx_rate
            
            feed_records.append({
                'business_date': business_date,
                'trade_id': record['trade_id'],
                'amount_usd': amount_usd
            })
            
            total_usd += amount_usd
            print(f"  Trade {record['trade_id']}: {currency} {amount:,.2f} * {fx_rate} = USD ${amount_usd:,.2f}")
        
        print(f"Calculated USD amounts for {len(feed_records)} records, total: ${total_usd:,.2f}")
        logging.info(f"Calculated USD amounts for {len(feed_records)} records")
        
        return feed_records
    
    @task()
    def write_derivatives_feed(feed_records: List[Dict[str, Any]], **context):
        """
        Write final derivatives feed to derivatives_feed table
        """
        print(f"Writing {len(feed_records)} records to derivatives_feed table")
        
        records_written = 0
        total_value = 0
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        if not feed_records:
            print("No feed records to write")
            send_success_webhook(context, records_written, total_value, business_date)
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.derivatives_feed 
                (business_date, trade_id, amount_usd)
            VALUES (%s, %s, %s)
            ON CONFLICT (business_date, trade_id) DO UPDATE SET
                amount_usd = EXCLUDED.amount_usd
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in feed_records:
                cursor.execute(insert_query, (
                    record['business_date'],
                    record['trade_id'],
                    record['amount_usd']
                ))
                
                records_written += 1
                total_value += record['amount_usd']
            
            conn.commit()
            
            print(f"Successfully wrote {records_written} records to derivatives_feed")
            logging.info(f"Wrote {records_written} feed records with total value: {total_value}")
            
            context['ti'].xcom_push(key='records_processed', value=records_written)
            context['ti'].xcom_push(key='total_value', value=total_value)
            context['ti'].xcom_push(key='business_date', value=business_date)
            
            send_success_webhook(context, records_written, total_value, business_date)
            
        except Exception as e:
            conn.rollback()
            print(f"Error writing to derivatives_feed: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    # Phase 1: Read upstream trade data in parallel
    exchange_trades = read_exchange_trades()
    vendor_trades = read_vendor_trades()
    
    # Phase 2: Consolidate trades into normalized format
    consolidated = consolidate_trades(exchange_trades, vendor_trades)
    
    # Phase 3: Write consolidated data
    write_consolidated = write_consolidated_derivs(consolidated)
    
    # Phase 4: Read data for USD conversion (after write completes)
    consolidated_data = read_consolidated_derivs()
    fx_rates = read_exchange_rates()
    
    # Set dependencies - consolidated read must wait for write
    write_consolidated >> consolidated_data
    
    # Phase 5: Calculate USD amounts (join with FX rates)
    feed_data = calculate_usd_amounts(consolidated_data, fx_rates)
    
    # Phase 6: Write final feed
    write_derivatives_feed(feed_data)


dag = derivatives_pipeline()