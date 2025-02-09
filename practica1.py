import numpy as np
import pandas as pd
from datetime import datetime


def inRecallTimeWindow(t1, t2, timeWindowDays=2.0):
    return (t2 - t1).dt.days < timeWindowDays

def is_precursor(data):

    data['call_ts_next'] = data.groupby('customer_id')['call_ts'].shift(-1)
    data['is_precursor'] = inRecallTimeWindow(data['call_ts'], data['call_ts_next']).map({True: 'Y', False: 'N'})
    data.drop(columns=['call_ts_next'], inplace=True)
    return data

def precursor_call_id(data):

    data['call_ts_previous'] = data.groupby('customer_id')['call_ts'].shift(1)
    data['call_id_previous'] = data.groupby('customer_id')['call_id'].shift(1)
    mask = inRecallTimeWindow(data['call_ts_previous'], data['call_ts'])
    data['precursor_call_id'] = data['call_id_previous'].where(mask).astype('Int64')
    data['hours_from_first_call'] = (data['call_ts']- data['call_ts_previous']).dt.total_seconds() / 3600
    data.drop(columns=['call_ts_previous','call_id_previous'],inplace=True)
    return data

def process_chunk(chunk,output_file,first_chunk):

    chunk = chunk.sort_values(by=['customer_id','call_ts'])
    chunk['call_ts'] = pd.to_datetime(chunk['call_ts'])

    chunk = is_precursor(chunk)
    chunk = precursor_call_id(chunk)

    chunk.to_csv(output_file, mode='a', index=False, header=first_chunk)



def main():

    """
    path = "./calls_without_target.csv/calls_without_target.csv"

    data = pd.read_csv(path,sep=';')
    data = data.sort_values(by=['customer_id','call_ts'])
    data['call_ts'] = pd.to_datetime(data['call_ts'])

    data = is_precursor(data)

    data = precursor_call_id(data)
    """
    # Esta parte es el codigo para el processamiento de los datos en chunks
    CHUNK_SIZE = 1000  
    INPUT_FILE = "./calls_without_target.csv/calls_without_target.csv"   
    OUTPUT_FILE = "./calls_without_target.csv/processed_calls.csv"
    with pd.read_csv(INPUT_FILE,sep=";",chunksize=CHUNK_SIZE) as reader:
        for i,chunck in enumerate(reader):
            first_chunk = (i == 0)
            process_chunk(chunck,OUTPUT_FILE,first_chunk)

if __name__ == '__main__':

    main()