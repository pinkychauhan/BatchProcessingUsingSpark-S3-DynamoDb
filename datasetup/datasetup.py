
import zipfile
import boto3
import re
import os
from io import BytesIO
import pandas as pd

pattern = re.compile('(.*)On_Time_On_Time_Performance_(.*)_(.*).csv')

column_names = [
    'Year', 'Month', 'DayofMonth', 'DayOfWeek', 'FlightDate', 'UniqueCarrier',
    'FlightNum', 'Origin', 'Dest', 'CRSDepTime', 'DepDelay', 'ArrDelay', 'Cancelled'
]
basepath = '/mnt/aviation/airline_ontime'
extracted_dir = 'extracted'

s3 = boto3.client('s3')

for subdir, dirs, files in os.walk(basepath):
    for file in files:
        try:
            filepath = os.path.join(subdir, file)
            extractedsubdir = os.path.join(subdir, extracted_dir)
            print("Processing: " + filepath)
            with zipfile.ZipFile(filepath, 'r') as zip_ref:
                zip_ref.extractall(extracted_dir)
                original_csv = []
                for f in os.listdir(extracted_dir):
                    if f.endswith('csv'):
                        original_csv.append(f)
                for csv in original_csv:
                    print("Process extracted csv: " + os.path.join(extracted_dir, csv))
                    match = re.search(pattern, csv)
                    if match:
                        year = match.group(2)
                        month = match.group(3)
                        csv_df = pd.read_csv(os.path.join(extracted_dir, csv),
                                             usecols=column_names)
                        converted_csv = os.path.join('airline_ontime', year+"_"+month+".csv")
                        print("Writing csv: " + converted_csv)
                        #csv_df.to_csv(converted_csv, index=False)
                        csv_buffer = BytesIO()
                        csv_df.to_csv(csv_buffer, index=False)
                        s3.put_object(Bucket="bucket", Key=converted_csv, Body=csv_buffer.getvalue())
                    os.remove(os.path.join(extracted_dir, csv))
        except Exception as ex:
            print("Error processing file: " + file)
            print(ex)


