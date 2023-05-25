import logging
import boto3
from botocore.exceptions import ClientError


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

    def upload_object_file(self):
        """
        create s3 object for a specific file.
        """
        #path file
        #bucket name and file upload

        return 0
    
    def list_objects_from_bucket(self):
        return 0
    
    def read_s3_object(self):
        return 0
    
    def retrieving_objects_specific_type(self):
        return 0

if __name__=='__main__':
    s3_obj = manage_s3("test-raj-bucket2","eu-central-1")
    #re = s3_obj.create_bucket()
    #print(re)

    #res_json = s3_obj.list_buckets()
    #print('Existing buckets:')
    #for bucket in res_json['Buckets']:
    #    print(f'  {bucket["Name"]}')
    #s3_obj.delete_bucket()


    