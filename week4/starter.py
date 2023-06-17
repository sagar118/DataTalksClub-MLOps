#!/usr/bin/env python
# coding: utf-8

import argparse
import pickle
import numpy as np
import pandas as pd

categorical = ['PULocationID', 'DOLocationID']

def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

def load_model(path):
    with open(path, 'rb') as f_in:
        dv, model = pickle.load(f_in)
    return dv, model

def main(year: str, month: str):
    filename = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:0>2}.parquet'

    df = read_data(filename)
    dicts = df[categorical].to_dict(orient='records')

    dv, model = load_model('model.bin')

    X_val = dv.transform(dicts)
    y_pred = model.predict(X_val)

    print(np.mean(y_pred))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--year', required=True)
    parser.add_argument('--month', required=True)

    args = parser.parse_args()

    main(args.year, args.month)

# df = read_data('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-02.parquet')




# Q2. Preparing the output
# year, month = 2022, 2
# output = pd.DataFrame()
# output['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
# output['duration'] = y_pred


# output_file = f'predictions/yellow_tripdata_{year}-{month:02d}.parquet'
# output.to_parquet(
#     output_file,
#     engine='pyarrow',
#     compression=None,
#     index=False
# )




