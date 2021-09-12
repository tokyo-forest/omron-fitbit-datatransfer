import datetime
import json

import csv
import urllib.parse

import boto3 as boto3
import requests as requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
from slack_sdk.webhook import WebhookClient

REGION = 'ap-northeast-1'
url = 'https://hooks.slack.com/services/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'
webhook = WebhookClient(url)


def lambda_handler(event, context):
    webhook_start_response = webhook.send(text="omron-fitbit-datatran started")
    assert webhook_start_response.status_code == 200
    assert webhook_start_response.body == 'ok'

    param_value = get_parameters("google-drive-parameter")
    refresh_token = get_parameters("fitbit-refresh-token")
    client_secret = get_parameters("fitbit-client-secret")

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

    access_token = refresh_access_token(client_secret, refresh_token)

    with open(filename, 'r') as f2:
        reader = csv.reader(f2)
        next(reader)

        for row in reader:
            day, time = convert_date(row[0])
            register_weight(day, time, row[2], access_token)
            register_fat(day, time, row[3], access_token)

    file = {'name': datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}
    service.files().update(fileId=target_id, body=file).execute()

    webhook_start_response = webhook.send(text="omron-fitbit-datatran finished successfully!!")
    assert webhook_start_response.status_code == 200
    assert webhook_start_response.body == 'ok'
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "success!!",
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
    post_request(payload=payload, headers=headers, endpoint='https://api.fitbit.com/1/user/-/body/log/weight.json',
                 body=None)


def register_fat(day, time, fat, access_token):
    payload = {'date': day, 'time': time, 'fat': fat}
    headers = {'authorization': f'Bearer {access_token}'}
    post_request(payload=payload, headers=headers, endpoint='https://api.fitbit.com/1/user/-/body/log/fat.json',
                 body=None)


def refresh_access_token(client_secret, refresh_token):
    headers = {'authorization': f'Basic {client_secret}', 'Content-Type': 'application/x-www-form-urlencoded'}
    body = {'grant_type': 'refresh_token', 'refresh_token': f'{refresh_token}'}
    body_data = urllib.parse.urlencode(body)
    response = post_request(payload=None, headers=headers, endpoint='https://api.fitbit.com/oauth2/token',
                            body=body_data)

    print(response)

    print(response.json()["access_token"])
    return response.json()["access_token"]


def post_request(payload, headers, endpoint, body):
    response = requests.post(endpoint, params=payload, headers=headers, data=body)
    print(response.text)
    try:
        response.raise_for_status()
    except Exception:
        webhook_start_response = webhook.send(text="omron-fitbit-datatran failed!!")
        assert webhook_start_response.status_code == 200
        assert webhook_start_response.body == 'ok'
        raise

    return response


def convert_date(org_date):
    target_date_time = datetime.datetime.strptime(org_date, "%Y/%m/%d %H:%M")

    day = target_date_time.strftime("%Y-%m-%d")
    time = target_date_time.strftime("%H:%M:%S")

    return day, time
