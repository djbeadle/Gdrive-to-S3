#!/usr/bin/python

import os
import re

import requests

import logging
import boto3
from botocore.exceptions import ClientError
import os

from secrets import secret_key


def upload_file(file_name, bucket, object_name, original_folder_name, folder_date, group_id):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    next_asset_id = None
    try:
        raw = requests.get(f'https://danielbeadle.net/audio/next_asset_id?secret={secret_key}')
        next_asset_id = raw.json()['next_asset_id']
    except Exception as e:
        print(e)
        print(raw.text)
    # json()['next_asset_id']

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, f'{group_id}/{next_asset_id}_{object_name.replace(" ", "_")}', ExtraArgs={
            "Metadata": {
                "original-folder": original_folder_name,
                "guessed-folder-date": str(folder_date)
            }
        })
    except ClientError as e:
        logging.error(e)
        return False
    return True

exclude_list = []

with open('/var/services/homes/djbeadle/LTBB_Audio_Sync/excludes.txt') as f:
    while line := f.readline():
        exclude_list.append(line.strip())

exclude_list = sorted(exclude_list)
exclude_names = set(exclude_list)
print(exclude_names)
print(len(exclude_names))

first = True
for root, dirs, files in os.walk('/var/services/homes/djbeadle/2024-01-06-LTBB-Audio-Backup/', topdown=True):
    if first:
        first = False
        print(f'Starting with {len(dirs)} folders')
        # [dirs.remove(folder) for folder in dirs if folder in exclude_names]
        # https://stackoverflow.com/a/10620948
        dirs[:] = [d for d in dirs if d not in exclude_list]
        print(f'Ending with {len(dirs)} folders')

    current_path = os.path.join(root)
    current_folder = os.path.basename(os.path.normpath(current_path))
    guessed_date = re.search(r"(20\d\d\W\d{1,2}\W\d{1,2})", str(current_folder)).group(0)

    if guessed_date:
        print(f'{root}: {dirs}')
        for file in files:
            # Skip files in exclude_names
            if file in exclude_names:
                continue

            # Skip hidden files
            if file[0] == '.':
                continue
            print(f' - {file}')
            upload_file(
                os.path.join(current_path, file),
                'audio-manager',
                file,
                current_folder,
                guessed_date,
                0, # Group id, 0=LTBB
            )
    else:
        print(f'Can\'t upload, not confident about date in folder name {current_folder}')

