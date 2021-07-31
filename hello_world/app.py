import datetime
import json

import csv

import boto3 as boto3
import requests as requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

REGION = 'ap-northeast-1'


def lambda_handler(event, context):
    param_value = get_parameters("google-drive-parameter")
    access_token = get_parameters("fitbit-access-token")

    service_account_info = json.loads(param_value, strict=False)
    credentials = service_account.Credentials.from_service_account_info(service_account_info)

    service = build('drive', 'v3', credentials=credentials)

    # Call the Drive v3 API
    results = service.files().list(
        q="'10T1BEyP6jGzRKQ6f8Kgw9yufEppOWSUv' in parents and name contains 'Omron'"  # nannanyのGoogleDriveに特化しているので要修正
    ).execute()
    items = results.get('files', [])

    target_id = None
    if not items:
        print('No files found.')
        return
    else:
        print('Files:')
        for item in items:
            print(u'{0} ({1})'.format(item['name'], item['id']))
            target_id = item['id']

    request = service.files().get_media(fileId=target_id)

    with open('/tmp/temp_file', 'wb') as f1:
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
            day, time = convert_date(row[0])
            register_weight(day, time, row[2], access_token)
            register_fat(day, time, row[3], access_token)

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


def register_weight(day, time, weight, access_token):
    payload = {'date': day, 'time': time, 'weight': weight}
    headers = {'authorization': f'Bearer {access_token}'}
    post_request(payload, headers, 'weight.json')


def register_fat(day, time, fat, access_token):
    payload = {'date': day, 'time': time, 'fat': fat}
    headers = {'authorization': f'Bearer {access_token}'}
    post_request(payload, headers, 'fat.json')


def post_request(payload, headers, endpoint):
    response = requests.post(f'https://api.fitbit.com/1/user/-/body/log/{endpoint}', params=payload, headers=headers)
    print(response.text)


def convert_date(org_date):
    target_date_time = datetime.datetime.strptime(org_date, "%Y/%m/%d %H:%M")

    day = target_date_time.strftime("%Y-%m-%d")
    time = target_date_time.strftime("%H:%M:%S")

    return day, time
