import boto3
import pandas as pd
import json
import requests
import os
import logging
import tempfile

def search(bucket, dst_dir, sufix):
    ret_list = []
    for obj in bucket.objects.filter(Prefix=dst_dir):
        video_ext = os.path.splitext(obj.key)[-1]
        if(video_ext == sufix):
            ret_list.append(obj.key)
    if (len(ret_list) > 0):
        return ret_list
    else:
        return None

if __name__ == "__main__":
    try:
        logging.critical('set_environ_name')
        BUCKET_NAME = os.environ.get('BUCKET_NAME')
        SOURCE_DIR = os.environ.get('SOURCE_DIR')
        COPY_DIR = os.environ.get('COPY_DIR')
        API_ENDPOINT = os.environ.get('API_ENDPOINT')
        SOURCE_FILE = os.environ.get('SOURCE_FILE')
        COPY_FILE = os.environ.get('COPY_FILE')
        
        logging.critical('set_S3')
        s3 = boto3.client('s3')
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(BUCKET_NAME)
        
        logging.critical('search_json')
        json_path_list = search(bucket, SOURCE_DIR+'/json/'+SOURCE_FILE, '.json')
        logging.critical('search_jpg')
        jpg_path_list = search(bucket, SOURCE_DIR+'/image/'+SOURCE_FILE, '.jpg')


        tmpdir = tempfile.TemporaryDirectory()
        tmp = tmpdir.name + '/'

        tmp_json = tmp + 'tmp.json'
        logging.critical('copy_json')
        for json_path in json_path_list:
            bucket.download_file(json_path, tmp_json)
            copy_path = json_path.replace(SOURCE_DIR, COPY_DIR)
            copy_json_path = copy_path.replace(SOURCE_FILE, COPY_FILE)
            json_open = open(tmp_json, 'r')
            json_load = json.load(json_open)
            json_load['UserInfo'][0]['ID'] = COPY_FILE
            json_load['UserInfo'][0]['cheat_snap'] = COPY_DIR + '/image/' + COPY_FILE
            with open(tmp_json, 'w') as outfile:
                json.dump(json_load, outfile)
            bucket.upload_file(tmp_json, copy_json_path)
            # s3.copy_object(Bucket=BUCKET_NAME, Key=copy_json_path, CopySource={'Bucket': BUCKET_NAME, 'Key': json_path})

        logging.critical('copy_jpg')
        for jpg_path in jpg_path_list:
            copy_path = jpg_path.replace(SOURCE_DIR, COPY_DIR)
            copy_jpg_path = copy_path.replace(SOURCE_FILE, COPY_FILE)
            s3.copy_object(Bucket=BUCKET_NAME, Key=copy_jpg_path, CopySource={'Bucket': BUCKET_NAME, 'Key': jpg_path})

        exit(0)

    except Exception as e:
        logging.error('---except exception---')
        logging.error(e)
        exit(1)