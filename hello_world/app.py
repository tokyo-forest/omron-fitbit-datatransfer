import json

import csv
import os

import boto3 as boto3
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

REGION = 'ap-northeast-1'


def lambda_handler(event, context):
    param_value = get_parameters("google-drive-parameter")

    service_account_info = json.loads(param_value, strict=False)
    credentials = service_account.Credentials.from_service_account_info(service_account_info)

    service = build('drive', 'v3', credentials=credentials)

    # Call the Drive v3 API
    results = service.files().list(
        q="'10T1BEyP6jGzRKQ6f8Kgw9yufEppOWSUv' in parents and name contains 'Omron'"  # nannanyのGoogleDriveに特化しているので要修正
    ).execute()
    items = results.get('files', [])

    if not items:
        print('No files found.')
        return
    else:
        print('Files:')
        for item in items:
            print(u'{0} ({1})'.format(item['name'], item['id']))
            target_id = item['id']

    request = service.files().get_media(fileId=target_id)

    # ダウンロードする必要ない気がしてきた。そのままストリーム処理してしまえば良いか
    with open(os.path.join('.', 'tmp'), 'wb') as f1:
        filename = f1.name
        downloader = MediaIoBaseDownload(f1, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print("Download %d%%." % int(status.progress() * 100))

    print(filename)
    with open(filename, 'r') as f2:
        reader = csv.reader(f2)
        next(reader)

        for row in reader:
            register_weight(row[0], row[2])
            register_fat(row[0], row[3])

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world",
            }
        ),
    }


def get_parameters(param_key):
    ssm = boto3.client('ssm', region_name=REGION)
    response = ssm.get_parameters(
        Names=[
            param_key,
        ],
        WithDecryption=True
    )
    return response['Parameters'][0]['Value']


def register_weight(date, weight):
    print(date, weight)


def register_fat(date, fat):
    print(date, fat)
