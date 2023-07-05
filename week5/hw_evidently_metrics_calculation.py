import joblib
import datetime
import time
import random
import logging
import uuid
import pytz
import io
import pandas as pd
import psycopg

from prefect import task, flow
from evidently import ColumnMapping
from evidently.report import Report
from evidently.metrics import ColumnDriftMetric, DatasetDriftMetric, DatasetMissingValuesMetric, ColumnQuantileMetric, RegressionQualityMetric

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

SEND_TIMEOUT = 10
rand = random.Random()

create_table_query = """
drop table if exists hw_metrics;
create table hw_metrics(
    timestamp timestamp,
    quatile_metric float
); 
"""

# Load reference data and model
# reference_data = pd.read_parquet('data/reference.parquet')
# jan_data = pd.read_parquet('./data/green_tripdata_2022-01.parquet')
reference_data = pd.read_parquet('./data/green_tripdata_2022-02.parquet')
# reference_data = pd.concat([jan_data, feb_data])
# with open('models/lin_reg.bin', 'rb') as f_in:
	# model = joblib.load(f_in)

# raw_data = pd.read_parquet('data/green_tripdata_2022-02.parquet')
raw_data = pd.read_parquet('data/green_tripdata_2023-03.parquet')
begin = datetime.datetime(2023, 3, 1, 0, 0)

num_features = ['passenger_count', 'trip_distance', 'fare_amount', 'total_amount', 'fare_amount']
cat_features = ['PULocationID', 'DOLocationID']
 
column_mapping = ColumnMapping(
    # prediction='fare_amount',
    numerical_features=num_features,
    categorical_features=cat_features,
    # target=None
)

report = Report(metrics = [
    ColumnQuantileMetric(column_name="fare_amount", quantile=0.5)
])

# @task
def prep_db():
    with psycopg.connect("host=localhost port=5432 user=postgres password=postgres", autocommit=True) as conn:
        res = conn.execute("SELECT 1 FROM pg_database WHERE datname = 'test'")
        if len(res.fetchall()) == 0:
            conn.execute("CREATE DATABASE test;")
        with psycopg.connect("host=localhost port=5432 user=postgres password=postgres dbname=test") as conn:
            conn.execute(create_table_query)

# @task
def calculate_metrics_postgresql(curr, i):
    current_data = raw_data[(raw_data.lpep_pickup_datetime >= (begin + datetime.timedelta(i))) &
		(raw_data.lpep_pickup_datetime < (begin + datetime.timedelta(i + 1)))]
    
    current_data.fillna(0, inplace=True)
    # current_data['prediction'] = model.predict(current_data[num_features + cat_features].fillna(0))
    
    # report.run(reference_data = reference_data, current_data = current_data,
	# 	column_mapping=column_mapping)
    report.run(reference_data = reference_data, current_data = current_data,
		column_mapping=column_mapping)
    
    result = report.as_dict()
    
    # prediction_drift = result['metrics'][0]['result']['drift_score']
    # num_drifted_columns = result['metrics'][1]['result']['number_of_drifted_columns']
    # share_missing_values = result['metrics'][2]['result']['current']['share_of_missing_values']
    quatile_metric = result['metrics'][0]['result']['current']['value']
    print(quatile_metric)
    
    curr.execute(
		"insert into hw_metrics(timestamp, quatile_metric) values (%s, %s)",
		(begin + datetime.timedelta(i), quatile_metric)
	)

# @flow
def batch_monitoring_backfill():
	prep_db()
	last_send = datetime.datetime.now() - datetime.timedelta(seconds=10)
	with psycopg.connect("host=localhost port=5432 dbname=test user=postgres password=postgres", autocommit=True) as conn:
		for i in range(0, 27):
			with conn.cursor() as curr:
				calculate_metrics_postgresql(curr, i)

			new_send = datetime.datetime.now()
			seconds_elapsed = (new_send - last_send).total_seconds()
			if seconds_elapsed < SEND_TIMEOUT:
				time.sleep(SEND_TIMEOUT - seconds_elapsed)
			while last_send < new_send:
				last_send = last_send + datetime.timedelta(seconds=10)
			logging.info("data sent")

if __name__ == '__main__':
	batch_monitoring_backfill()