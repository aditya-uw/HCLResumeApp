import json
import boto3

# Sends a message to AWS server to delete a resume from S3 database
def lambda_handler(event, context):
    # TODO implement
    print(event)
    s3 = boto3.resource('s3')
    bucket =event['Records'][0]['s3']['bucket']['name']
    key =event['Records'][0]['s3']['object']['key']
    imagekey = key.replace("resume", "image")+".png"
    obj = s3.Object(bucket, imagekey)
    print(obj)
    obj.delete()
    print("image file deleted")
    dynamodb = boto3.resource('dynamodb')
    print("Deleting resource from dynamoDB " + key)
    table = dynamodb.Table('resumewordcloudTable')
    table.delete_item(Key={'name': key})
    print("Deletion completed")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
