import re
import sys
import csv
import codecs

import click
from smart_open import open
import boto3

from .ais_request import ais_request

csv.field_size_limit(sys.maxsize)


class AIS_Geocoder():
    '''
    For geocoding addresses and inserting other information from AIS results into our dataset.
    '''
    def __init__(self,
            ais_url, 
            ais_key, 
            ais_user,
            s3_bucket,
            s3_input_key,
            s3_output_key,
            query_fields,
            ais_fields,
            remove_fields,
            **kwargs):
        
        self.ais_url = ais_url
        self.ais_key = ais_key
        self.ais_user = ais_user
        self.s3_bucket = s3_bucket
        self.s3_input_key = s3_input_key
        self.s3_output_key = s3_output_key
        self.query_fields = query_fields.split(',')
        self.ais_fields = ais_fields.split(',')
        self.csv_path = '/tmp/output.csv'


    def ais_inner_geocode(self):

        out_rows = None
        session = boto3.Session()

        # Remove first slash if passed in with key.
        if self.s3_input_key[0] == '/':
            self.s3_input_key = self.s3_input_key[1:-1]
        if self.s3_output_key[0] == '/':
            self.s3_output_key = self.s3_output_key[1:-1]

        input_file = f's3://{self.s3_bucket}/{self.s3_input_key}'
        output_file = f's3://{self.s3_bucket}/{self.s3_output_key}'

        # Use smart_open imported over built-in open, which will opaquely use boto3 to stream our data out of and back into S3
        # https://github.com/piskvorky/smart_open
        with open(input_file, 'r', transport_params={'client': session.client('s3')}) as input_stream:
            with open(output_file, 'w', transport_params={'client': session.client('s3')}) as output_stream:

                #rows = csv.DictReader(codecs.iterdecode(input_stream, 'utf-8'))
                rows = csv.DictReader(input_stream)

                for row in rows:
                    query_elements = []
                    for query_field in self.query_fields:
                        query_elements.append(row[query_field])

                    result = None

                    if not result:
                        result = ais_request(self.ais_url, self.ais_key, self.ais_user, query_elements)

                    if result and 'features' in result and len(result['features']) > 0:

                        feature = result['features'][0]
                        for ais_field in self.ais_fields:
                            if ais_field == 'lon' or ais_field == 'longitude':
                                row[ais_field] = feature['geometry']['coordinates'][0] if feature['geometry']['coordinates'][0] is not None else ''
                            elif ais_field == 'lat' or ais_field == 'latitude':
                                row[ais_field] = feature['geometry']['coordinates'][1]  if feature['geometry']['coordinates'][0] is not None else ''
                            elif ais_field == 'shape':
                                coords = feature['geometry']['coordinates']
                                row[ais_field] = 'SRID=4326;POINT ({x} {y})'.format(x=coords[0], y=coords[1])
                            else:
                                row[ais_field] = feature['properties'][ais_field]
                    else:
                        print('Could not geocode "{}"'.format(query_elements))

                    if out_rows == None:
                        headers = rows._fieldnames + self.ais_fields
                        print(f'Headers!: {headers}')
                        out_rows = csv.DictWriter(output_stream, headers)
                        out_rows.writeheader()

                    out_rows.writerow(row)
