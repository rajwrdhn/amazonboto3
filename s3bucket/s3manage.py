import logging
import boto3
from botocore.exceptions import ClientError
import os

class manage_s3:
    """
    Managing s3 objects of different types on aws using boto3.
    """

    def __init__(self, bucket_name, region) -> None:
        self.bucket_name = bucket_name
        self.region = region
    
    def create_bucket(self):
        try:
            s3_client = boto3.client('s3')
            location = {'LocationConstraint': self.region}
            s3_client.create_bucket(Bucket=self.bucket_name, CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
        
        return True
    
    def list_buckets(self):
        try:
            s3_client = boto3.client('s3')
            response = s3_client.list_buckets()
        except ClientError as e:
            logging.error(e)

        return response

    def delete_bucket(self):
        try:
            s3_client = boto3.client('s3')
            print("Check whether the bucket has objects...")
            objs = s3_client.list_objects_v2(Bucket=self.bucket_name)
            print(objs)
            count_objs = objs['KeyCount']
            if count_objs == 0:
                response = s3_client.delete_bucket(Bucket=self.bucket_name)
                print(f"{self.bucket_name} has been deleted successfully !!! {response}")
            else:
                print(f"{self.bucket_name} is not empty {count_objs} objects present")
                print("Please make sure S3 bucket is empty before deleting it !!!")

        except ClientError as e :
            logging.error(e)

        return response

    def upload_object_file(self, file_name,object_name=None):
        """
        create s3 object for a specific file.
        """
        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        # Upload the file
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(file_name, self.bucket_name, object_name)
        
        except ClientError as e:
            logging.error(e)
            return False
        return True
    
    def list_objects_from_bucket(self):
        s3_client = boto3.client('s3')
        try:
            response = s3_client.list_objects_v2(Bucket=self.bucket_name)
        
        except ClientError as e:
            logging.error(e)          
        return response
    
    def retrieving_objects_specific_type(self, file_type):
        res_out = {}
        res = self.list_objects_from_bucket()
        
        return 0
    
    def copy_from_bucket_to_bucket(self, bucket_from, key_from, bucket_to, key_to):
        s3_resource = boto3.resource('s3')
        copy_source = {
            'Bucket': bucket_from,
            'Key': key_from
            }
        #bucket = s3_resource.Bucket(bucket_to)
        bucket_to.copy(copy_source, key_to)
        

if __name__=='__main__':
    s3_obj = manage_s3("test-dummy-bucket-1","eu-central-1")
    #re = s3_obj.create_bucket()
    #print(re)

    res_json = s3_obj.list_buckets()
    print('Existing buckets:')
    for bucket in res_json['Buckets']:
        print(f'  {bucket["Name"]}')
    #s3_obj.delete_bucket()

    #s3_obj.upload_object_file("/home/raj/Music/aws/amazonboto3/s3bucket/3.json")

    res = s3_obj.list_objects_from_bucket()
    for ke in res['Contents']:       
        print(f' {ke["Key"]}')
    
    
