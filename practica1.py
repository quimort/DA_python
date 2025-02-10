from operator import itemgetter
import pandas as pd
import os
import csv
import heapq
import time 


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

def process_chunk(chunk,prev_last_row, next_first_row,output_file,first_chunk):

    if prev_last_row is not None:
        chunk = pd.concat([prev_last_row, chunk])
    if next_first_row is not None:
        chunk = pd.concat([chunk, next_first_row])

    chunk = chunk.sort_values(by=['customer_id','call_ts'])
    chunk['call_ts'] = pd.to_datetime(chunk['call_ts'])

    chunk = is_precursor(chunk)
    chunk = precursor_call_id(chunk)
    # Extract first and last rows for next iteration
    next_prev_last_row = chunk.iloc[[-1]]  # Last row for next chunk
    next_first_row = chunk.iloc[[0]]  # First row for next chunk

    # Remove first and last row before saving
    chunk = chunk.iloc[1:-1]  

    chunk.to_csv(output_file, mode='a', index=False, header=first_chunk,quoting=csv.QUOTE_NONNUMERIC)

    return next_prev_last_row, next_first_row

def chunking_sort(chunk_size,input_file,output_file,sort_by):

    file_names = []
    for batch_no, chunk in enumerate(pd.read_csv(input_file, sep=";",chunksize=chunk_size), 1):
        chunk.sort_values(by=sort_by, inplace=True)
        file_name = f"{input_file[:-4]}_{batch_no}.csv"
        chunk.to_csv(file_name, index=False)
        file_names.append(file_name)

    # merge the chunks
    chunks = [csv.DictReader(open(file_name)) for file_name in file_names]
    with open(output_file, "w",newline='', encoding='utf-8') as outfile:
        field_names = ["call_id", "customer_id","call_ts"]
        writer = csv.DictWriter(outfile, fieldnames=field_names,delimiter=";", quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()

        for row in heapq.merge(*chunks, key=itemgetter("customer_id","call_ts")):
            writer.writerow(row)

    return file_names

def main_by_chunk():
    # primero de todo hacemos un sort de los datos por customer_id y call_ts

    # Esta parte es el codigo para el processamiento de los datos en chunks
    CHUNK_SIZE = 1000  
    INPUT_FILE = "./calls_without_target.csv/calls_without_target_in.csv"   
    OUTPUT_FILE = "./calls_without_target.csv/processed_calls.csv"
    file_names = chunking_sort(CHUNK_SIZE,"./calls_without_target.csv/calls_without_target.csv",INPUT_FILE,['customer_id','call_ts'])
    # Remove output file if it exists
    for file in file_names:
        os.remove(file)
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    
    prev_last_row = None  
    next_first_row = None 

    with pd.read_csv(INPUT_FILE, chunksize=CHUNK_SIZE, names=['call_id','customer_id','call_ts']) as reader:
        first_chunk = True
        next_chunk = next(reader, None)  # Read first chunk to preload next_first_row

        while next_chunk is not None:
            # Read ahead to get first row of the next chunk
            future_chunk = next(reader, None)  
            if future_chunk is not None:
                next_first_row = future_chunk.iloc[[0]]  

            # Process current chunk safely
            prev_last_row, next_first_row = process_chunk(next_chunk, prev_last_row, next_first_row, OUTPUT_FILE,first_chunk)

            # Move to next chunk
            next_chunk = future_chunk
            first_chunk = False  

def main():

    path = "./calls_without_target.csv/calls_without_target.csv"

    data = pd.read_csv(path,sep=';')
    data = data.sort_values(by=['customer_id','call_ts'])
    data['call_ts'] = pd.to_datetime(data['call_ts'])

    data = is_precursor(data)

    data = precursor_call_id(data)

    print(data.head())
    

if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time()-start)
    #main_by_chunk()