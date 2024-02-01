import csv
from typing import List, Type, Optional, Dict
import sys
import os
import json

import requests
import click
import boto3
from hurry.filesize import size


class Airtable():
    def __init__(self, app_id:str, api_key:str, table_name:str, s3_bucket:str, s3_key:str):
        self.app_id = app_id
        self.api_key = api_key
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.offset = None
        self.rows_per_page = 1000
        self.csv_path = f'/tmp/{self.table_name}.csv'

    def get_fieldnames(self):
        
        response = requests.get(
            f'https://api.airtable.com/v0/{self.app_id}/{self.table_name}?maxRecords={self.rows_per_page}',
            headers={
                f'Authorization': f'Bearer {self.api_key}'
            }
        )
        
        data = response.json()

        fieldnames = []

        for record in data['records']:
            record_fieldnames = list(record['fields'].keys())

            for fieldname in record_fieldnames:
                if fieldname not in fieldnames:
                    fieldnames.append(fieldname)
                    
        return fieldnames

    def get_records(self):
        endpoint = f'https://api.airtable.com/v0/{self.app_id}/{self.table_name}?maxRecords={self.rows_per_page}'
        print(f'Starting extract from airtable endpoint: {endpoint}')

        response = requests.get(
            endpoint,
            headers={
                'Authorization': f'Bearer {self.api_key}'
            },
            params={
                'offset': self.offset
            }
        )
        
        data = response.json()
        yield data['records']
        
        if 'offset' in data: 
            yield from get_records(self.app_id, self.api_key, self.table_name, offset=data['offset'], rows_per_page=1000)

    def process_row(self, row: Dict) -> Dict:
        for key, value in row.items():
            if isinstance(value, list):
                row[key] = json.dumps(value)

        return row

    def load_to_s3(self):
        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.s3_key).put(Body=open(self.csv_path, 'rb'))

    def clean_up(self) -> None:
        if os.path.isfile(self.csv_path):
            os.remove(self.csv_path)

    def extract(self):
        
        fieldnames = self.get_fieldnames()

        if (self.s3_bucket and self.s3_key):

            with open(self.csv_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)

                writer.writeheader()

                for records_batch in self.get_records():
                    for record in records_batch:
                        row = self.process_row(record['fields'])
                        writer.writerow(row)

            num_lines = sum(1 for _ in open(self.csv_path)) - 1
            file_size = size(os.path.getsize(self.csv_path))
            print(f'Extraction successful? File size: {file_size}, total lines: {num_lines}')
            self.load_to_s3()
            self.clean_up()

        else:
            writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)

            writer.writeheader()

            for records_batch in self.get_records(self.app_id, self.api_key, self.table_name):
                for record in records_batch:
                    row = self.process_row(record['fields'])
                    writer.writerow(row)
            
            sys.stdout.flush()

    