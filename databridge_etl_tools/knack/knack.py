
import requests
import csv
import os,sys
import boto3

csv.field_size_limit(sys.maxsize)

class Knack():
    '''
    Extracts a CSV from Knack (https://dashboard.knack.com/apps)
    ''' 
    def __init__(self,
                 knack_objectid,
                 app_id, 
                 api_key, 
                 s3_bucket, 
                 s3_key,
                 **kwargs):
        self.knack_objectid = knack_objectid
        self.app_id = app_id
        self.api_key = api_key
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.csv_path = '/tmp/output.csv'

    def load_to_s3(self):
        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.s3_key).put(Body=open(self.csv_path, 'rb'))

    def extract(self):
        print(f'Starting extract from {self.knack_objectid}')

        # Knack API Endpoint
        endpoint = f'https://api.knack.com/v1/objects/{self.knack_objectid}/records'

        headers = {
            'X-Knack-Application-Id': self.app_id,
            'X-Knack-REST-API-Key': self.api_key,
            'Content-Type': 'application/json'
        }

        response = requests.get(endpoint, headers=headers)

        if response.status_code == 200:
            data = response.json()
            records = data['records']

            # Check if records exist and write to CSV
            if records:
                with open(self.csv_path, 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=records[0].keys())
                    writer.writeheader()
                    for record in records:
                        writer.writerow(record)
                print(f'Extraction successful? File size: {os.path.getsize(self.csv_path)}')
                self.load_to_s3()
            else:
                print("No records found.")

        else:
            print(f"Failed to fetch data. Status Code: {response.status_code}. Reason: {response.text}")

        
