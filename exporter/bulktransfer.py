import json
import urllib
import boto3
import os
import csv
import codecs

s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')

bucket = os.environ['bucket']
key = os.environ['key']
tableName = os.environ['table']

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    #get() does not store in memory
    try:
       obj = s3.Object(bucket, key).get()['Body']
    except:
       print("S3 Object could not be opened.")
    try:
       table = dynamodb.Table(tableName)
    except:
       print("Error loading DynamoDB table. Check if table was created correctly.")

    batch_size = 100
    batch = []

    #DictReader is a generator; not stored in memory
    for row in csv.DictReader(codecs.getreader('utf-8')(obj)):
      if len(batch) >= batch_size:
         write_to_dynamo(batch)
         batch.clear()

      batch.append(row)

    if batch:
      write_to_dynamo(batch)

    print("Done!");
    return {
      'statusCode': 200,
      'body': json.dumps('Uploaded to DynamoDB Table')
    }


def write_to_dynamo(rows):
    try:
      table = dynamodb.Table(tableName)
    except:
      print("Error loading DynamoDB table. Check if table was created correctly")

    try:
      with table.batch_writer() as batch:
         for i in range(len(rows)):
            batch.put_item(Item=rows[i]
            )
    except:
      print("Error executing batch_writer")