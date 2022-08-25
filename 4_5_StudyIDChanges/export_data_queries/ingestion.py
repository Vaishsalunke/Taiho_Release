#!/usr/bin/env python3

"""
Usage:
  ingestion.py  --zk_nodes=<nodes> --studyid=<studyid>
  ingestion.py (-h | --help)
  ingestion.py --version
Description:
    This script moves data from the given source folder to destination folder after transforming it into the desired format in the given S3 bucket
Options:
    -h or help                                      Show help
    version                                         Show version
    zk_nodes=<nodes>                                Comma separated string of zookeeper hosts (used for retrieval of S3 credentials)
    studyid=<studyid>                               ID of the rave study
"""

import traceback
import boto3
import pandas as pd
import re
import os
import yaml
import logging
from ast import literal_eval
from kazoo.client import KazooClient
from datetime import datetime
from docopt import docopt
import warnings
from dateutil.parser import parse
warnings.filterwarnings('ignore')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

path = '/home/comprehend/repo/din-customer/taiho/export_data_queries'

# Reading the config
with open(f'{path}/config.yaml', 'rb') as y:
    config = yaml.load(y,Loader=yaml.FullLoader)

# Reading the necessary dynamic variables from the config
secret_key_path = config['secret_key_path']
access_key_path = config['access_key_path']
ts_watermark_path = config['ts_watermark_path']
source = config['source_folder']
dest = config['destination_folder']
site = config['site_folder']
domains = config['domain_list']
rename_cols = config['rename_cols']
bucket = config['bucket']

#Function to get the secret values from zookeeper
def get_key_value(zknodes):
    zkClient = KazooClient(hosts=zknodes)
    zkClient.start()
    access_key = zkClient.get(access_key_path)[0].decode('UTF-8')
    secret_key = zkClient.get(secret_key_path)[0].decode('UTF-8')
    ts_watermark = zkClient.get(ts_watermark_path)[0].decode('UTF-8')
    return access_key, secret_key, ts_watermark

# Actual function that transforms and moves the files
def modify_and_place_data(zknodes,study):

    access_key, secret_key, ts_watermark = get_key_value(zknodes)

    study = study.lower()

    s3_client = boto3.client(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
        )

    if s3_client:
        logging.info(f'\n\n(((((((((((( INITIALIZING CONNECTION TO {bucket} BUCKET ))))))))))))\n\n\n')
    else:
        logging.error(f'Unable to connect to {bucket} bucket')

    ts_dict = literal_eval(ts_watermark)

    if study in ts_dict.keys():
        ts = ts_dict.get(study)
    else:
        ts_dict[study] = '1900-01-01 00:00:00'
        ts = ts_dict.get(study)
    ts = parse(ts)

    for domain in domains:

        try:

            logging.info(f'<====================== RUNNING DOMAIN {domain.upper()} ======================>\n')

            domain = domain.lower()
            save_domain = domain
            save_domain = domain

            if domain.upper() == 'LB':
                domain = 'lab_normlab'
                save_domain = 'lb'
            elif domain.upper() == 'EC':
                domain = 'exo'
                save_domain = 'ec'

            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=f'{source}/{study}_{domain}')
            
            counter=0
            for page in pages:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.csv') and obj['Size'] > 0 and (obj['LastModified'].replace(tzinfo=None) >= ts.replace(tzinfo=None)):
                        
                        file_name = f"{os.path.basename(obj['Key'])}".replace(study,save_domain)

                        s3_client.download_file(Bucket=bucket,Key=obj['Key'],Filename=f'{path}/df_in.csv')

                        df = pd.read_csv(f'{path}/df_in.csv')

                        if not df.empty:

                            if 'MAP' not in domain.upper():
                                df['AUDIT_ACTION'] = ['I' for _ in range(df.shape[0])]
                                df.to_csv(f'{path}/df_out.csv')
                                s3_client.upload_file(Filename=f'{path}/df_out.csv',Bucket=bucket, Key=f'{dest}/{study}/{save_domain}/{file_name}')

                            elif 'MAP' in domain.upper():
                                if domain.upper() == 'MAP_SITE':
                                    if 'sitecountry' not in df.columns.tolist():
                                        if counter==0:
                                            site_files = s3_client.list_objects(Bucket=bucket, Prefix=f'study_site_metadata/{study}/')['Contents']
                                            site_loc = [i['Key'] for i in site_files if '.csv' in i['Key']][0]
                                            s3_client.download_file(Bucket=bucket,Key=site_loc,Filename=f'{path}/sites.csv')
                                            sites = pd.read_csv(f'{path}/sites.csv')
                                            sites['site_number'] = pd.to_numeric(sites['site_number'],errors='coerce')
                                            logging.info(f'fetching country info from {site_loc}')
                                            counter+=1
                                        df['siteno'] = pd.to_numeric(df['siteno'],errors='coerce')
                                        df['sitecountry'] = [sites[sites['site_number']==i]['country'].values[0] for i in df['siteno'].values.tolist()]
                                        df = df.drop(['studysitenumber'],axis=1,errors='ignore')
                                        df = df.drop(['site_country'],axis=1,errors='ignore')
                                df = df.rename(columns=rename_cols[f'{domain.upper()}'])
                                df.to_csv(f'{path}/df_out.csv')
                                s3_client.upload_file(Filename=f'{path}/df_out.csv',Bucket=bucket, Key=f'{dest}/{study}/{save_domain}/{file_name}')

                            logging.info(f"processed file -- {os.path.basename(obj['Key'])} -- {df.shape[0]} rows -- file last modified on {obj['LastModified'].strftime(format='%Y-%b-%d %H:%M')}")

                        else:
                            continue
                    
            print('\n')
            logging.info(f'<====================== FINISHING DOMAIN {save_domain.upper()} ======================>\n\n\n')

        except Exception as exp:
            logging.error(f'An Error occured during loading domain {domain.upper()}')
            logging.exception(exp)

    logging.info(f'***** ALL DOMAINS TRANSFORMED AND INSERTED SUCCESSFULLY INTO {dest} *****')

    ts_dict[f'{study}'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
    ts_dict = bytes(str(ts_dict),'UTF-8')
    zkClient = KazooClient(hosts=zknodes)
    zkClient.start()
    zkClient.set(ts_watermark_path, ts_dict)

    if os.path.exists(f'{path}/df_in.csv'):
        os.remove(f'{path}/df_in.csv')
    if os.path.exists(f'{path}/df_out.csv'):
        os.remove(f'{path}/df_out.csv')
    if os.path.exists(f'{path}/sites.csv'):
        os.remove(f'{path}/sites.csv')

if __name__ == '__main__':
    args = docopt(__doc__, version='ingestion 1.0')
    modify_and_place_data(args['--zk_nodes'],args['--studyid'])

