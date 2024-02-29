#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = os.environ.get('USER')
    password = os.environ.get('PASSWORD')
    host = os.environ.get('HOST')
    port = os.environ.get('PORT')
    db = os.environ.get('DB')

    # List of CSV files to be ingested
    csv_files = [
        "exam/data_analyst_test.csv",
        "exam/data_analyst_test_usage.csv"
    ]

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    for csv_file in csv_files:
        csv_name = csv_file.split("/")[-1]
        os.system(f"wget https://raw.githubusercontent.com/viktorybloom/amber/main/{csv_file} -O {csv_name}")

        df = pd.read_csv(csv_name)

        # Assuming the table name is the same as the CSV file name (without extension)
        table_name = csv_name.split(".")[0]

        # Define column data types 
        if table_name == "data_analyst_test":
            column_types = {
                'CUSTOMER_ID': 'VARCHAR(255)',
                'BILLING_ORG': 'VARCHAR(255)',
                'STATE': 'VARCHAR(10)',
                'SALES_DATE': 'DATE',
                'COOL_OFF_END_DATE': 'DATE',
                'FRMP_END_DATE': 'DATE',
                'ACCOUNT_STATUS': 'VARCHAR(20)',
                'CURRENT_METER_TYPE': 'VARCHAR(20)',
            }
        elif table_name == "data_analyst_test_usage":
            column_types = {
                'CUSTOMER_ID': 'VARCHAR(255)',
                'USAGE_DATE_AEST': 'DATE',
                'DAILY_IMPORT_KWH': 'FLOAT',
                'DAILY_EXPORT_KWH': 'FLOAT',
            }

        # Convert data types before writing to SQL
        df = df.astype(column_types)

        # Write to SQL with specified data types
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False, dtype=column_types)

        print(f'Finished ingesting {csv_name} into the postgres database')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')

    args = parser.parse_args()

    main(args)

