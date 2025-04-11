import boto3
import json

def s3_client(
        aws_account_access_key,
        aws_secret_account_access_key,
        region
):
    client_instance = boto3.client(
        's3',
        aws_access_key_id = aws_account_access_key,
        aws_secret_access_key = aws_secret_account_access_key,
        region_name = region
    )
    return client_instance

def json_post_to_s3(
        instance,
        data,
        bucket_name,
        file_path,
        file_name,
):
    instance.put_object(
        Bucket = bucket_name,
        Key = file_path + '/' + file_name + '.json',
        Body = json.dumps(data),
        ContentType = 'application/json'
    )

def json_get_from_s3(
        instance,
        bucket_name,
        file_path,
        file_name,
        raw=False
):
    raw_object = instance.get_object(
        Bucket = bucket_name,
        Key = file_path + '/' + file_name + '.json',
    )

    if raw == True:
        return raw_object
    
    else:
        parsed_object = raw_object['Body'].read().decode('utf-8')
        return parsed_object