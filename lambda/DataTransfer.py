from __future__ import print_function

import csv
import os

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from oauth2client.service_account import ServiceAccountCredentials

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']


def main():
    key_file = 'lambda/omron-fitbit-relation.json'  # これをsecret managerとかから取るようにしたい。
    creds = ServiceAccountCredentials.from_json_keyfile_name(key_file)

    service = build('drive', 'v3', credentials=creds)

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


def register_weight(date, weight):
    print(date, weight)


def register_fat(date, fat):
    print(date, fat)

if __name__ == '__main__':
    main()
