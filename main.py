# npx dukascopy-node -i xauusd -from 2019-01-01 -to 2019-01-10 -t m1 -f csv -v true -vu units -fn tuhin

'''
scp main.py root@x.x.x.x:/mnt/fxdata

npx dukascopy-node -i xauusd -from 2000-01-01 -to 2024-06-01 -t m1 -f csv -v true -vu units -fn xauusd-24-06-01 -dir .

## Done
      "audcad",
      "audchf",


'''



import subprocess

file_name = 'data'
file_ext = 'csv'

file = file_name + '.' + file_ext

metadata_file = 'instrument-meta-data.json' # https://raw.githubusercontent.com/Leo4815162342/dukascopy-node/master/src/utils/instrument-meta-data/generated/instrument-meta-data.json
instruments_file = 'instrument-groups.json' # instruments https://raw.githubusercontent.com/Leo4815162342/dukascopy-node/master/src/utils/instrument-meta-data/generated/instrument-groups.json

def get_fx_and_metals():
    import json
    import os

    ## Metadata
    # Specify the path to your JSON file
    file_path = os.path.join(os.path.dirname(__file__), metadata_file)

    # Read the file
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = file.read()
    except FileNotFoundError as e:
        print(f'File not found: {file_path}')
        exit(1)
    except IOError as e:
        print(f'Error reading file: {file_path}')
        exit(1)

    try:
        # Parse JSON data
        metaData = json.loads(data)

        # Print the parsed data
        # print(metaData['xauusd'])

    except json.JSONDecodeError as e:
        print(f'Error parsing JSON: {e}')
        exit(1)

    # Specify the path to your JSON file
    file_path = os.path.join(os.path.dirname(__file__), instruments_file)

    # Read the file
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = file.read()
    except FileNotFoundError as e:
        print(f'File not found: {file_path}')
        exit(1)
    except IOError as e:
        print(f'Error reading file: {file_path}')
        exit(1)

    try:
        # Parse JSON data
        jsonData = json.loads(data)
        instruments = {}
        # Iterate over each object in the array and print the contents
        for item in jsonData:
            if item['id'] in ['fx_crosses', 'fx_majors', 'fx_metals']:
                # print('ID:', item['id'])
                # print('Instruments:')
                for instrument in item['instruments']:
                    # print(f"'{instrument}',")
                    instruments[instrument] = metaData[instrument]
                # print()  # Print an empty line for separation
        return instruments
    except json.JSONDecodeError as e:
        print(f'Error parsing JSON: {e}')
        exit(1)



instruments = get_fx_and_metals()
# print(instruments.keys())
# li = []
# for i in instruments.values():
#     li.append(i['startDayForMinuteCandles'])
#
# print(sorted(li))

## Year
#
# import h5py
# # import numpy as np
# f = h5py.File("mytestfile.hdf5", "a")

import pandas as pd
from datetime import datetime
import time

# data1 = pd.read_csv('w1.csv')
# data1 = data1.set_index('timestamp')
#
# # Write the processed data1 to HDF5 file
# data1.to_hdf('w.hdf', key='xauusd', mode='w', complevel=9, format='table')
#
# # Read and process the second CSV file
# data2 = pd.read_csv('w2.csv')
# data2 = data2.set_index('timestamp')
#
# # Append the second dataset to the same HDF5 file
# data2.to_hdf('w.hdf', key='xauusd2', mode='a', complevel=9, format='table', append=True)

# Read the HDF5 file to verify its contents
# data_hdf = pd.read_hdf('w.hdf')
# print(data_hdf)

# with pd.HDFStore('w.hdf') as f:
#     print(f.keys())
#     print(f['xauusd'])
#     print(f['xauusd2'])
# Define the command as a list of strings (each part of the command is a separate element)

# npx dukascopy-node -i xauusd -from 2000-01-01 -to 2024-06-01 -t m1 -f csv -v true -vu units -fl true -bp 3000 -r 10 -rp 20000 -fn xauusd-24-06-01 -dir .



# symbol = "xauusd"

def get_time():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def dump_stat(df):
    print(df)
    style = '''
    <style>
    /* CSS styles for the table */
    .dataframe {
        font-family: Arial, sans-serif;
        border-collapse: collapse;
        width: 100%;
    }
    .dataframe th, .dataframe td {
        border: 1px solid #dddddd;
        padding: 8px;
        text-align: left;
    }
    .dataframe th {
        background-color: #f2f2f2;
    }
    </style>
    '''
    with open('output/index.html', 'w') as f:
        f.write(style)
        f.write(df.to_html(index=False))

df = pd.DataFrame(columns=['symbol', 'status', 'time', 'duration'])

for symbol in instruments.keys():
    print('\n\n\n----------------\nfetching symbol:' + symbol)
    started = datetime.now()
    dd = {'symbol': symbol, 'status': 'downloading...', 'time': get_time(), 'duration': '...'}
    df = df.append(dd, ignore_index=True)
    dump_stat(df)
    from_date = "1995-01-01"
    # from_date = "2024-05-20"
    to_date = "2024-06-01"
    command = [
        "npx",
        "dukascopy-node",
        "-i", symbol,
        "-from", from_date,
        "-to", to_date,
        "-t", "m1",
        "-v", "true",
        "-vu", "units",
        "-fl", "true",
        "-bp", "1000", # pause between batch
        "-r", "10",
        "-rp", "20000",
        "-fn", symbol+'-'+to_date,
        "-f", file_ext,
        "-dir", "."
    ]


    # Execute the command
    while True:
        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=2 * 60 * 60)
            # 'result.stdout' contains the standard output of the command
            # print("Command output:", result.stdout)
            break
        except Exception as e:
            # If the command returns a non-zero exit code, it raises a CalledProcessError
            print("Error executing command:", e)
            print("Command output (if any):", e.output)
            print("Retrying after a while ...")
            time.sleep(60 * 5)

    ended = datetime.now()
    time_took = ended - started

    file_saved_lines = [line.strip() for line in result.stdout.splitlines() if 'File saved' in line]

    if len(file_saved_lines) > 0:
        file_saved_lines = file_saved_lines[0]
    else:
        print('-- Download Failure !!!! ---')
        file_saved_lines = str(result.stdout)

    df.loc[df.index[-1], 'status'] = file_saved_lines
    df.loc[df.index[-1], 'time'] = get_time()
    df.loc[df.index[-1], 'duration'] = time_took
    dump_stat(df)
    time.sleep(60 * 5)


    # print(result.stdout)

