import boto3
import json

from Test_sam01 import response

# create client
client = boto3.client("secretsmanager", "ap-south")

#store secreatid
secret_name = "rds-id"

#create response
result_res = client.get_secret_value(secretId= secret_name)

secret_data = json.loads(result_res["SecretString"])

username = secret_data["username"]
password = secret_data["password"]

secret_name = "RDS-1"
region_name = "ap-south-1"
def call_cred(sec, reg):
    try:
        client  = boto3.client("secretsmanager", region_name=reg)
        response1= client.get_secret_value(secretID=sec)
        if 'SecretString' in response1:
            secret_rds = response1["SecretString"]
        else:
            secret_rds = response1["SecretBinary"].decode("utf-8")
        return json.loads(secret_rds)
    except Exception as e:
        print("Error", e)

data_cred = call_cred(secret_name, region_name)
username_rds = data_cred["username"]
password_rds = data_cred["password"]
