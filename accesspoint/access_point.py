import logging
import boto3
from botocore.exceptions import ClientError

class AccessPolicy():

    def __init__(self, account:str):
        self.account = account 

    def get_ap(self, apname:str):
        """
        """
        s3client = boto3.client('s3control')

        response = s3client.get_access_point(
            AccountId= self.account,
            Name=apname
        )

        return response

    def create_ap(self, apname:str, bucket_name:str):
        """
        """
        s3client = boto3.client('s3control')
        response = s3client.create_access_point(
            AccountId=self.account,
            Name=apname,
            Bucket=bucket_name
        )
        
        return response
    
    def list_ap(self):
        """
        """
        s3client = boto3.client('s3control')
        response = s3client.list_access_points(AccountId=self.account) 

        return response

if __name__=='__main__':
    ap_obj = AccessPolicy("444001393398")
    #ap_obj.create_ap("test-dummy-bucket-1")
    r = ap_obj.get_ap("ap1")

    print(r['Endpoints']['ipv4'])

    a = ap_obj.list_ap()
    for nam in a['AccessPointList']:
        print(nam['Name'])