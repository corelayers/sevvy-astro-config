"""
## Card Balance Consolidation Pipeline

This DAG consolidates card transactions and generates daily balance reports:
1. Reads card transactions from card_transactions table
2. Reads user profiles from user_profiles table
3. Reads card metadata from card_metadata table
4. Aggregates transactions per user and card issuer
5. Writes aggregated data to aggregated_transactions table
6. Joins with active cards data to filter displayable cards
7. Writes final balance to daily_balance_report table
8. Sends success notification webhook

The pipeline processes data for a specific business date and calculates
the daily balance shown to users for each of their cards.
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
        total_value: Total aggregated value
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
            "task_id": "write_daily_balance_report",
            "run_id": dag_run.run_id,
            "organization_id": "cmfhfglh40k9h01rbszv8zhxx",
            "deployment_id": "cmfhfmdq70kb601rb5wu0d2nd",
            "dag_details": {
                "dag_id": dag.dag_id,
                "execution_date": execution_date.isoformat(),
                "state": "success",
                "log_url": f"https://cmfhfglh40k9h01rbszv8zhxx.astronomer.run/dwu0d2nd/dags/{dag.dag_id}/grid",
                "records_processed": records_processed,
                "total_aggregated_value": float(total_value),
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
    'card_balance_pipeline',
    default_args=default_args,
    description='ETL pipeline for aggregating card transactions and computing daily user balances',
    schedule='@daily',
    catchup=False,
    tags=['etl', 'consumer-finance', 'card-balance', 'production'],
)
def card_balance_pipeline():
    """Main DAG definition using TaskFlow API"""
    
    @task()
    def read_card_transactions(**context) -> List[Dict[str, Any]]:
        """
        Read card transactions from card_transactions table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"💰 Reading card transactions for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT transaction_date, user_id, card_issuer, transaction_amount_usd
            FROM public.card_transactions
            WHERE transaction_date = %s
            ORDER BY user_id, card_issuer
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        transactions = []
        for row in results:
            transactions.append({
                'transaction_date': str(row[0]),
                'user_id': row[1],
                'card_issuer': row[2],
                'transaction_amount_usd': float(row[3])
            })
        
        print(f"✅ Found {len(transactions)} card transaction records for {business_date}")
        logging.info(f"Found {len(transactions)} card transaction records for {business_date}")
        
        return transactions
    
    @task()
    def read_user_profiles(**context) -> List[Dict[str, Any]]:
        """
        Read user profiles from user_profiles table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"💸 Reading user profiles for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT profile_date, user_id, card_issuer, profile_adjustment_usd
            FROM public.user_profiles
            WHERE profile_date = %s
            ORDER BY user_id, card_issuer
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        profiles = []
        for row in results:
            profiles.append({
                'profile_date': str(row[0]),
                'user_id': row[1],
                'card_issuer': row[2],
                'profile_adjustment_usd': float(row[3])
            })
        
        print(f"✅ Found {len(profiles)} user profile records for {business_date}")
        logging.info(f"Found {len(profiles)} user profile records for {business_date}")
        
        return profiles
    
    @task()
    def read_card_metadata(**context) -> List[Dict[str, Any]]:
        """
        Read card metadata from card_metadata table for the execution date
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🏦 Reading card metadata for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT metadata_date, user_id, card_issuer, metadata_value_usd
            FROM public.card_metadata
            WHERE metadata_date = %s
            ORDER BY user_id, card_issuer
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        metadata = []
        for row in results:
            metadata.append({
                'metadata_date': str(row[0]),
                'user_id': row[1],
                'card_issuer': row[2],
                'metadata_value_usd': float(row[3])
            })
        
        print(f"✅ Found {len(metadata)} card metadata records for {business_date}")
        logging.info(f"Found {len(metadata)} card metadata records for {business_date}")
        
        return metadata
    
    @task()
    def transaction_aggregation(
        transactions: List[Dict[str, Any]],
        profiles: List[Dict[str, Any]],
        metadata: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Aggregate transaction data by calculating transactions - profiles - metadata for each user.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🧮 Aggregating transaction data for {business_date}")
        
        transaction_lookup = {}
        for transaction in transactions:
            key = (transaction['user_id'], transaction['card_issuer'])
            if key not in transaction_lookup:
                transaction_lookup[key] = transaction['transaction_amount_usd']
        
        profile_lookup = {}
        for profile in profiles:
            key = (profile['user_id'], profile['card_issuer'])
            if key not in profile_lookup:
                profile_lookup[key] = profile['profile_adjustment_usd']
        
        card_lookup = {}
        for card in metadata:
            key = (card['user_id'], card['card_issuer'])
            if key not in card_lookup:
                card_lookup[key] = card['metadata_value_usd']
        
        # Get all unique user/card combinations
        all_keys = set(transaction_lookup.keys()) | set(profile_lookup.keys()) | set(card_lookup.keys())
        
        aggregated = []
        for user_id, card_issuer in all_keys:
            transaction_amount = transaction_lookup.get((user_id, card_issuer), 0.0)
            profile_amount = profile_lookup.get((user_id, card_issuer), 0.0)
            card_amount = card_lookup.get((user_id, card_issuer), 0.0)
            
            daily_balance = transaction_amount - profile_amount - card_amount
            
            aggregated.append({
                'transaction_date': business_date,
                'user_id': user_id,
                'card_issuer': card_issuer,
                'daily_balance_usd': daily_balance
            })
            
            print(f"  User {user_id} ({card_issuer}): "
                  f"${transaction_amount:,.2f} - ${profile_amount:,.2f} - ${card_amount:,.2f} = "
                  f"${daily_balance:,.2f}")
        
        print(f"✅ Aggregated {len(aggregated)} user records")
        logging.info(f"Aggregated {len(aggregated)} user records for {business_date}")
        
        return aggregated
    
    @task()
    def write_aggregated_transactions(aggregated: List[Dict[str, Any]], **context):
        """
        Write aggregated card transaction data to aggregated_transactions table
        """
        print(f"💾 Writing {len(aggregated)} records to aggregated_transactions table")
        
        if not aggregated:
            print("⚠️  No aggregated records to write")
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.aggregated_transactions 
                (transaction_date, user_id, card_issuer, daily_balance_usd)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (transaction_date, user_id, card_issuer) DO UPDATE SET
                daily_balance_usd = EXCLUDED.daily_balance_usd
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in aggregated:
                cursor.execute(insert_query, (
                    record['transaction_date'],
                    record['user_id'],
                    record['card_issuer'],
                    record['daily_balance_usd']
                ))
            
            conn.commit()
            print(f"✅ Successfully wrote {len(aggregated)} records to aggregated_transactions")
            logging.info(f"Wrote {len(aggregated)} aggregated records")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to aggregated_transactions: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    @task()
    def read_active_cards(**context) -> List[Dict[str, Any]]:
        """
        Read active cards data from active_cards table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📋 Reading active cards data for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT card_date, user_id, card_issuer, is_displayable
            FROM public.active_cards
            WHERE card_date = %s
            ORDER BY user_id, card_issuer
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        ref_data = []
        for row in results:
            ref_data.append({
                'card_date': str(row[0]),
                'user_id': row[1],
                'card_issuer': row[2],
                'is_displayable': row[3]
            })
        
        print(f"✅ Found {len(ref_data)} active card records for {business_date}")
        logging.info(f"Found {len(ref_data)} active card records for {business_date}")
        
        return ref_data
    
    @task()
    def read_aggregated_data(**context) -> List[Dict[str, Any]]:
        """
        Read aggregated data from aggregated_transactions table
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"📊 Reading aggregated data for business date: {business_date}")
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        query = """
            SELECT transaction_date, user_id, card_issuer, daily_balance_usd
            FROM public.aggregated_transactions
            WHERE transaction_date = %s
            ORDER BY user_id, card_issuer
        """
        
        results = pg_hook.get_records(query, parameters=[business_date])
        
        aggregated = []
        for row in results:
            aggregated.append({
                'transaction_date': str(row[0]),
                'user_id': row[1],
                'card_issuer': row[2],
                'daily_balance_usd': float(row[3])
            })
        
        print(f"✅ Found {len(aggregated)} aggregated records for {business_date}")
        logging.info(f"Found {len(aggregated)} aggregated records for {business_date}")
        
        return aggregated
    
    @task()
    def balance_calculation(
        aggregated: List[Dict[str, Any]],
        ref_data: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Calculate card balance by joining aggregated data with active cards data
        and filtering for displayable cards only.
        """
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        print(f"🔒 Calculating card balance for {business_date}")
        
        # Build lookup for displayable cards
        displayable_lookup = {}
        for ref in ref_data:
            if ref['is_displayable']:
                key = (ref['user_id'], ref['card_issuer'])
                displayable_lookup[key] = True
        
        # Filter aggregated data to only displayable cards
        balance_records = []
        total_value = 0
        
        for record in aggregated:
            key = (record['user_id'], record['card_issuer'])
            
            if key in displayable_lookup:
                balance_records.append({
                    'transaction_date': record['transaction_date'],
                    'user_id': record['user_id'],
                    'card_issuer': record['card_issuer'],
                    'daily_balance_usd': record['daily_balance_usd']
                })
                total_value += record['daily_balance_usd']
                
                print(f"  Displayable: User {record['user_id']} ({record['card_issuer']}): "
                      f"${record['daily_balance_usd']:,.2f}")
        
        print(f"✅ Found {len(balance_records)} displayable card records")
        print(f"   Total balance value: ${total_value:,.2f}")
        logging.info(f"Found {len(balance_records)} displayable records with total value: {total_value}")
        
        return balance_records
    
    @task()
    def write_daily_balance_report(balance_records: List[Dict[str, Any]], **context):
        """
        Write final balance report to daily_balance_report table
        """
        print(f"💾 Writing {len(balance_records)} records to daily_balance_report table")
        
        records_written = 0
        total_value = 0
        execution_date = context.get('execution_date') or context.get('logical_date')
        business_date = execution_date.strftime('%Y-%m-%d')
        
        if not balance_records:
            print("⚠️  No balance records to write")
            send_success_webhook(context, records_written, total_value, business_date)
            return
        
        pg_hook = PostgresHook(postgres_conn_id='pipeline_test_rds')
        
        insert_query = """
            INSERT INTO public.daily_balance_report 
                (transaction_date, user_id, card_issuer, daily_balance_usd)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (transaction_date, user_id, card_issuer) DO UPDATE SET
                daily_balance_usd = EXCLUDED.daily_balance_usd
        """
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for record in balance_records:
                cursor.execute(insert_query, (
                    record['transaction_date'],
                    record['user_id'],
                    record['card_issuer'],
                    record['daily_balance_usd']
                ))
                
                records_written += 1
                total_value += record['daily_balance_usd']
            
            conn.commit()
            
            print(f"✅ Successfully wrote {records_written} records to daily_balance_report")
            logging.info(f"Wrote {records_written} balance records with total value: {total_value}")
            
            context['ti'].xcom_push(key='records_processed', value=records_written)
            context['ti'].xcom_push(key='total_value', value=total_value)
            context['ti'].xcom_push(key='business_date', value=business_date)
            
            send_success_webhook(context, records_written, total_value, business_date)
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error writing to daily_balance_report: {str(e)}")
            logging.error(f"Database insert failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    # Phase 1: Read upstream data in parallel
    transactions = read_card_transactions()
    profiles = read_user_profiles()
    metadata = read_card_metadata()
    
    # Phase 2: Aggregate transaction data
    aggregated = transaction_aggregation(transactions, profiles, metadata)
    
    # Phase 3: Write aggregated data
    write_aggregated = write_aggregated_transactions(aggregated)
    
    # Phase 4: Read data for balance calculation (after write completes)
    aggregated_data = read_aggregated_data()
    ref_data = read_active_cards()
    
    # Set dependencies - aggregated read must wait for write
    write_aggregated >> aggregated_data
    
    # Phase 5: Calculate balance (join with ref data, filter displayable)
    balances = balance_calculation(aggregated_data, ref_data)
    
    # Phase 6: Write final report
    write_daily_balance_report(balances)


dag = card_balance_pipeline()