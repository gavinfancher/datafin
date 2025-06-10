import boto3
import json
from botocore.exceptions import ClientError

def get_secret():
    secret_name = "apis/polygon"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Parse the JSON string
    secret_data = json.loads(get_secret_value_response['SecretString'])
    
    # Return the specific key you want (adjust key name as needed)
    return secret_data.get('apis/polygon')  # or whatever your key is named

# Test it
if __name__ == "__main__":
    try:
        api_key = get_secret()
        print(f"Polygon API Key: {api_key}")
    except Exception as e:
        print(f"Error: {e}")

# Or if you want to return multiple keys:
def get_polygon_credentials():
    secret_name = "apis/polygon"
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response['SecretString'])
        
        # Return specific keys you need
        return {
            'api_key': secret_data.get('api_key'),
            'aws_access_key_id': secret_data.get('aws_access_key_id'),
            'aws_secret_access_key': secret_data.get('aws_secret_access_key')
        }
        
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        raise