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

def is_precursor(data):

    data = data.sort_values(by=['customer_id','call_ts'])
    data['is_precursor'] = 'N'

    for customer_id,group in data.groupby('customer_id'):

        for i in range(1,len(group)):

            if inRecallTimeWindow(group.iloc[i-1]['call_ts'],group.iloc[i]['call_ts']):

                data.loc[group.index[i],'is_precursor'] = 'Y'
    return data 

def main():

    path = "./calls_without_target.csv/calls_without_target.csv"

    data = pd.read_csv(path,sep=';')

    data = is_precursor(data)

    print(data.head())

if __name__ == '__main__':

    main()