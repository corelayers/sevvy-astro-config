"""
## ClickHouse Channel Summary Pipeline

This DAG calculates daily channel transaction summaries:
1. Reads FX rates from fx_rates_daily table
2. Reads transactions from raw_daily_txn_by_country_currency table
3. Joins and converts amounts to USD
4. Aggregates by region and channel
5. Writes results to daily_channel_transaction_summary table

Parameters:
- business_date: Date to process (YYYY-MM-DD format)
- region_filter: Optional filter to limit regions (e.g., 'US'). None = all regions.

Used for anomaly detection testing:
- Phase 1: Run with region_filter=None to establish baseline
- Phase 2: Set region_filter='US' to trigger anomaly alerts
"""

from datetime import datetime, timedelta, date
from typing import List, Dict, Any
from decimal import Decimal
import logging
import os

from airflow.decorators import dag, task
from airflow.models.param import Param
from clickhouse_driver import Client


def get_clickhouse_client() -> Client:
    """Create ClickHouse client from environment variables"""
    return Client(
        host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
        port=int(os.getenv('CLICKHOUSE_PORT', '9000')),
        user=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD', ''),
        database=os.getenv('CLICKHOUSE_DATABASE', 'default'),
    )


default_args = {
    'owner': 'sevvy-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    'clickhouse_channel_summary_pipeline',
    default_args=default_args,
    description='ETL pipeline for daily channel transaction summary (ClickHouse)',
    schedule=None,
    catchup=False,
    tags=['etl', 'clickhouse', 'adhoc'],
    params={
        'business_date': Param(
            default=str(date.today()),
            type='string',
            description='Business date in YYYY-MM-DD format'
        ),
        'region_filter': Param(
            default=None,
            type=['string', 'null'],
            description='Optional region filter (e.g., US). None = all regions.'
        ),
    },
)
def clickhouse_channel_summary_pipeline():
    """Main DAG definition for ClickHouse channel summary ETL"""

    @task()
    def read_fx_rates(**context) -> List[Dict[str, Any]]:
        """Read FX rates from fx_rates_daily for the business date"""
        business_date = context['params']['business_date']
        
        print(f"💱 Reading FX rates for business date: {business_date}")
        
        client = get_clickhouse_client()
        
        query = """
            SELECT 
                report_date,
                currency,
                usd_rate
            FROM fx_rates_daily
            WHERE report_date = %(report_date)s
        """
        
        results = client.execute(
            query,
            {'report_date': business_date},
            with_column_types=True
        )
        
        rows, columns = results
        column_names = [col[0] for col in columns]
        
        fx_rates = []
        for row in rows:
            rate_dict = dict(zip(column_names, row))
            rate_dict['report_date'] = str(rate_dict['report_date'])
            rate_dict['usd_rate'] = float(rate_dict['usd_rate'])
            fx_rates.append(rate_dict)
        
        print(f"✅ Found {len(fx_rates)} FX rates")
        for rate in fx_rates:
            print(f"   {rate['currency']}: {rate['usd_rate']}")
        
        return fx_rates

    @task()
    def read_transactions(**context) -> List[Dict[str, Any]]:
        """Read transactions from raw_daily_txn_by_country_currency for the business date"""
        business_date = context['params']['business_date']
        
        print(f"📊 Reading transactions for business date: {business_date}")
        
        client = get_clickhouse_client()
        
        query = """
            SELECT 
                report_date,
                country,
                channel,
                currency,
                txn_count,
                total_amount_local
            FROM raw_daily_txn_by_country_currency
            WHERE report_date = %(report_date)s
        """
        
        results = client.execute(
            query,
            {'report_date': business_date},
            with_column_types=True
        )
        
        rows, columns = results
        column_names = [col[0] for col in columns]
        
        transactions = []
        for row in rows:
            txn_dict = dict(zip(column_names, row))
            txn_dict['report_date'] = str(txn_dict['report_date'])
            txn_dict['txn_count'] = int(txn_dict['txn_count'])
            txn_dict['total_amount_local'] = float(txn_dict['total_amount_local'])
            transactions.append(txn_dict)
        
        print(f"✅ Found {len(transactions)} transaction records")
        logging.info(f"Found {len(transactions)} transactions for {business_date}")
        
        return transactions

    @task()
    def transform_and_aggregate(
        transactions: List[Dict[str, Any]],
        fx_rates: List[Dict[str, Any]],
        **context
    ) -> List[Dict[str, Any]]:
        """
        Transform transactions:
        - Join with FX rates
        - Convert amounts to USD
        - Aggregate by region and channel
        - Apply region_filter if set
        """
        business_date = context['params']['business_date']
        region_filter = context['params'].get('region_filter')
        
        print(f"🧮 Transforming data for {business_date}")
        if region_filter:
            print(f"   ⚠️ Region filter active: {region_filter}")
        
        # Build FX rate lookup
        rate_lookup = {rate['currency']: rate['usd_rate'] for rate in fx_rates}
        
        # Aggregate by (region, channel)
        aggregated = {}
        
        for txn in transactions:
            region = txn['country']
            
            # Apply region filter if set
            if region_filter and region != region_filter:
                continue
            
            channel = txn['channel']
            currency = txn['currency']
            
            # Get USD rate
            usd_rate = rate_lookup.get(currency)
            if usd_rate is None:
                logging.warning(f"No FX rate for {currency}, skipping transaction")
                continue
            
            # Convert to USD
            amount_usd = txn['total_amount_local'] * usd_rate
            
            # Aggregate
            key = (region, channel)
            if key not in aggregated:
                aggregated[key] = {
                    'report_date': business_date,
                    'region': region,
                    'channel': channel,
                    'txn_count': 0,
                    'total_amount_usd': 0.0,
                }
            
            aggregated[key]['txn_count'] += txn['txn_count']
            aggregated[key]['total_amount_usd'] += amount_usd
        
        summary = list(aggregated.values())
        
        print(f"✅ Aggregated into {len(summary)} summary records")
        for rec in summary:
            print(f"   {rec['region']}/{rec['channel']}: "
                  f"{rec['txn_count']} txns, ${rec['total_amount_usd']:,.2f}")
        
        return summary

    @task()
    def write_summary(summary_data: List[Dict[str, Any]], **context):
        """Write summary data to daily_channel_transaction_summary table"""
        business_date = context['params']['business_date']
        
        print(f"💾 Writing {len(summary_data)} records to daily_channel_transaction_summary")
        
        if not summary_data:
            print("⚠️ No data to write")
            return
        
        client = get_clickhouse_client()
        
        # Prepare data for insert
        rows = [
            (
                rec['report_date'],
                rec['region'],
                rec['channel'],
                rec['txn_count'],
                Decimal(str(round(rec['total_amount_usd'], 2)))
            )
            for rec in summary_data
        ]
        
        # Insert data (ReplacingMergeTree will handle deduplication)
        client.execute(
            """
            INSERT INTO daily_channel_transaction_summary 
                (report_date, region, channel, txn_count, total_amount_usd)
            VALUES
            """,
            rows
        )
        
        total_usd = sum(rec['total_amount_usd'] for rec in summary_data)
        total_txns = sum(rec['txn_count'] for rec in summary_data)
        
        print(f"✅ Successfully wrote {len(summary_data)} records")
        print(f"   Total transactions: {total_txns:,}")
        print(f"   Total USD amount: ${total_usd:,.2f}")
        logging.info(f"Wrote {len(summary_data)} records for {business_date}")

    # Define task dependencies
    fx_rates = read_fx_rates()
    transactions = read_transactions()
    summary = transform_and_aggregate(transactions, fx_rates)
    write_summary(summary)


# Instantiate the DAG
dag = clickhouse_channel_summary_pipeline()
