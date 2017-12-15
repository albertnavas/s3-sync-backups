#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
import os
import datetime
import boto3
import botocore

def uploadFile(filename_path):

	try:
	    s3Resource.Object(bucket_name, filename_path).load()
	except botocore.exceptions.ClientError as e:
	    if e.response['Error']['Code'] == "404":
	    	s3Client.upload_file(base_path+filename_path, bucket_name, filename_path)
	    	return True
	    else:
			# raise
			return False
	else:
		print('Backup '+filename_path+' exist!')
		return False

aws_access_key_id = ''
aws_secret_access_key = ''

bucket_name = ''
base_path = '/backup/test/'
backup_paths = ['db/', 'media/']

# Only copy backups from last month
last_month_date = datetime.datetime.now() + datetime.timedelta(-30)

s3Client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
s3Resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

response = s3Client.list_buckets()
buckets = [bucket['Name'] for bucket in response['Buckets']]

if not bucket_name in buckets: print('Bucket not exist')

for backup_path in backup_paths:
	for x in os.walk(base_path+backup_path):
		files = x[2]
		if files:
			for filename in files:
				filename_path = backup_path+filename
				last_modified_date =  datetime.datetime.fromtimestamp(os.path.getmtime(base_path+filename_path))
				if last_modified_date > last_month_date:
					uploadFile(filename_path)

# Delete old backups
bucket = s3Resource.Bucket(bucket_name)
for item in bucket.objects.all():
	# Delete backups than now not exist in the server
	if not os.path.isfile(base_path+item.key):
		s3Client.delete_object(Bucket=bucket_name, Key=item.key)
		print(item.key+' deleted!')

	# Delete old backups
	file = Path(base_path+item.key)
	if file.is_file():
		last_modified_date =  datetime.datetime.fromtimestamp(os.path.getmtime(base_path+item.key))
		if last_modified_date < last_month_date:
			s3Client.delete_object(Bucket=bucket_name, Key=item.key)
			print(item.key+' deleted (old date)!')
