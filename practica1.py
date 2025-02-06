import numpy as np
import pandas as pd
from datetime import datetime

def inRecallTimeWindow(t1,t2):
    timeWindowDays = 2.0
    dt1 = datetime.strptime(t1,"%Y/%m/%d %H:%M:%S")
    dt2 = datetime.strptime(t2,"%Y/%m/%d %H:%M:%S")
    delta = dt2 - dt1
    days = int(delta.total_seconds() /86400.0)
    return(days<timeWindowDays)
def chech_if_precursor(row):
    if (row['call_ts_next'] == 'nan') or (row['call_ts'] == 'nan'):
        return 'N'
    if inRecallTimeWindow(row['call_ts'],row['call_ts_next']):
        return 'Y'
    
    return 'N'
def is_precursor(data):

    data = data.sort_values(by=['customer_id','call_ts'])
    data['call_ts_next'] = data.groupby('customer_id')['call_ts'].shift(-1).astype(str)
    data['is_precursor'] = data.apply(chech_if_precursor, axis=1)
    data.drop(columns=['call_ts_next'], inplace=True)
    return data

def main():

    path = "./calls_without_target.csv/calls_without_target.csv"

    data = pd.read_csv(path,sep=';')

    data = is_precursor(data)

    print(data[data['is_precursor'] == 'Y'].head())

if __name__ == '__main__':

    main()