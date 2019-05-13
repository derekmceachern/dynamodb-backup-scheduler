# Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
#
from __future__ import print_function
from datetime import date, datetime, timedelta
import json
import boto3
import time
from botocore.exceptions import ClientError
import os

ddbRegion = os.environ['AWS_DEFAULT_REGION']
ddbTable = os.environ['DDBTable']
backupName = 'Schedule_Backup_V21'
ddb = boto3.client('dynamodb', region_name=ddbRegion)

#--Confiruration for deleting old backup.
#  It will search for old backup and will escape deleting last backup days you mentioned in the backup retention
#  daysToLookBack=2
#
daysToLookBack=7
if 'BackupRetention' in os.environ:
	try:
		daysToLookBack= int(os.environ['BackupRetention'])
	except:
		print("[ERROR] Initialization Error. BackupRetention of", os.environ['BackupRetention'], "is not valid. Setting to:", daysToLookBack)
daysToLookBackL=daysToLookBack-1

minNumberBackups=3
if 'MinimumNumberBackups' in os.environ:
	try:
		minNumberBackups = int(os.environ['MinimumNumberBackups'])
	except:
		print("[ERROR] Initialization Error. minNumberBackups of", os.environ['minNumberBackups'], "is not valid. Setting to:", minNumberBackups)

if minNumberBackups >= daysToLookBackL:
	print("[ERROR] MinimumNumberBackups is >= BackupRetention, this doesn't work because nothing will be deleted.")
	minNumberBackups = daysToLookBackL - 1
	print("[INFO] Setting MinimumNumberBackups to:", minNumberBackups)

def lambda_handler(event, context):
	try:
		#--Create Backup
		#
		print(context.aws_request_id,"[INFO] Initiating backup for table", ddbTable, "with backup name", backupName, "in Region", ddbRegion)
		ddb.create_backup(TableName=ddbTable,BackupName = backupName)
		print(context.aws_request_id, "[INFO] Backup has been taken successfully for table:", ddbTable)

		#--Check for old backups and delete
		#  NOTE: TimeRangeLowerBround is "inclusive" so it should include
		#        backups created on lowerDate
		#        TimeRangeUpperBound is "exclusive" and will not include
		#        backups created on upperDate. i.e. if you run this script
		#        multiple times in a day the backup taken today will not
		#        be included in the return list
		#
		print(context.aws_request_id, "[INFO] Determing number of backups available for last", daysToLookBackL, "days")
		lowerDate=datetime.now() - timedelta(days=daysToLookBackL)
		upperDate=datetime.now()
		print(context.aws_request_id, "[DEBUG] lower bound:", lowerDate, "upper bound:", upperDate)
		responseLatest = ddb.list_backups(TableName=ddbTable, TimeRangeLowerBound=datetime(lowerDate.year, lowerDate.month, lowerDate.day), TimeRangeUpperBound=datetime(upperDate.year, upperDate.month, upperDate.day))
		print(context.aws_request_id, "[DEBUG] responseLatest:", responseLatest)
		latestBackupCount=len(responseLatest['BackupSummaries'])
		print(context.aws_request_id, "[INFO] Total backups available:", latestBackupCount)

		if latestBackupCount <= minNumberBackups:
			print(context.aws_request_id, "[INFO] Minimum Backups required", minNumberBackups, "not deleting anything")
			return

		#--TimeRangeLowerBound is the release of Amazon DynamoDB Backup and Restore - Nov 29, 2017
		#
		print(context.aws_request_id, "[INFO] Looking for backups greater then", daysToLookBack, "days to delete")
		deleteupperDate = datetime.now() - timedelta(days=daysToLookBack)
		print(context.aws_request_id, "[DEBUG] deleteupperDate:", deleteupperDate)
		response = ddb.list_backups(TableName=ddbTable, TimeRangeLowerBound=datetime(2017, 11, 29), TimeRangeUpperBound=datetime(deleteupperDate.year, deleteupperDate.month, deleteupperDate.day))
		print(context.aws_request_id, "[DEBUG] old backup list:", response)

		if 'BackupSummaries' in response:
			for backup in response['BackupSummaries']:
				print(context.aws_request_id, "[INFO] Backup ARN:", backup['BackupArn'], "BackupCreationDateTime:", backup['BackupCreationDateTime'])
				delete_response = ddb.delete_backup(BackupArn=backup['BackupArn'])
				print(context.aws_request_id, "[DEBUG] Backup Delete Response:", delete_response)
				print(context.aws_request_id, "[INFO] Backup Deleted")

	except  ClientError as e:
		print(context.aws_request_id, "[ERROR] Client Exception Caught:", e)

	except ValueError as ve:
		print(context.aws_request_id, "[ERROR] Value Exception Caught:", ve)

	except Exception as ex:
		print(context.aws_request_id, "[ERROR] General Exception Caught:", ex)
