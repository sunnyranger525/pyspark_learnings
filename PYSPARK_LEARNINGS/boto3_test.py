import boto3

s3 = boto3.client("s3")

localPath = "E:\DATA\csvfiles\sale02.scv"
bucket_name = "samsu44"
objectKey = "DATA/products/sales02.csv"

s3.upload_file(localPath, bucket_name, objectKey)
print("uploaded sucessfull.....")

