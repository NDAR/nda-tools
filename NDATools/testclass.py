#TODO - add tests that test this functionality for resuming upload of local and remote files 
#TODO - add tests that test this functionality for files that are evenly divisable by filesize. Run test with Multiple Files
import os
import boto3
import math
from boto3.s3.transfer import TransferConfig, S3Transfer

class S3MultipartUpload:
    def __init__(self, mpu_id, file, bucket, key):
        self.mpu_id = mpu_id
        self.file = file
        self.uploaded_parts = []
        self.bucket = bucket
        self.key = key
        self.is_local_file = not self.file.name.startswith('s3://')
        self.file_size = os.stat(file.name).st_size
        
    def resume_mpu(self):
        # set self.incomplete_parts by finding all the parts that are NOT in s3 yet, and still need to be uploaded
        # incomplete_parts will be an array of S3MultipartUploadPart
        # TODO - THIS WILL ONLY LIST A MAXIMUM NUMBER OF RESULTS AT A TIME - NEED TO PAGE THROUGH THEM (SAME WITH LISTING MULTI-PART UPLOADS)
        response = s3.list_parts(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.mpu_id
        )
        already_uploaded_parts = response['Parts']
        
        self.normal_part_size, self.total_number_parts, already_uploaded_parts = self.calc_normal_part_size_and_total_number_parts(already_uploaded_parts)
        
        self.uploaded_parts = dict((x['PartNumber'], x) for x in already_uploaded_parts)
        # TODO - add try catch finally and close file in finally block
        for part_number in range(1, self.total_number_parts + 1):
            if not part_number in self.uploaded_parts:
                part = self.S3MultipartUploadPart(self, part_number)
                part.upload_part()
                self.uploaded_parts["{}".format(part.part_number)] = part
                print("wtf: {}".format(self.uploaded_parts))
                
        parts = list(map(lambda x : dict(ETag=x.etag, PartNumber=x.part_number) , self.uploaded_parts.values()))
        response = s3.complete_multipart_upload(Bucket=self.bucket, Key=self.key, UploadId=self.mpu_id, MultipartUpload=dict(Parts=parts))
        print("response: {}".format(response))
        # TODO - maybe add step to check check-sum so that what was uploaded is the same as what we intended to upload
    
    def calc_normal_part_size_and_total_number_parts(self, already_uploaded_parts):
        """
            There are 4 possible situations:
                1) already_uploaded_parts is empty - we can define part size arbitrarily
                2) There is 1 part uploaded - need to abort upload first , then we can assign part size arbitrarily
                3) There are > 1 part uploaded - part size = size of the first part
        """
        if len(already_uploaded_parts) == 0:
            self.normal_part_size = 5 * 1024 * 1024
        elif len(already_uploaded_parts) == 1:
            already_uploaded_parts = []
            self.normal_part_size = 5 * 1024 * 1024
        else:
            self.normal_part_size = sorted(already_uploaded_parts, key=lambda x: x['PartNumber'])[0]['Size']            
        return self.normal_part_size , math.ceil(self.file_size / self.normal_part_size), already_uploaded_parts

    class S3MultipartUploadPart:
        
        def __init__(self, s3_multipart_upload, part_number):
            self.data = None
            self.s3_multipart_upload = s3_multipart_upload
            self.part_number = part_number
            self.offset = self.calculate_offset()
            self.etag = None
        def calculate_offset(self):
            return (self.part_number - 1) * self.s3_multipart_upload.normal_part_size
        def upload_part(self):
            #first, set data attribute. It is either offset -> offset+normal_part_size or offset->EOF, whichever is first
            
            self.s3_multipart_upload.file.seek(self.offset)
            self.data = self.s3_multipart_upload.file.read(min(self.s3_multipart_upload.normal_part_size, self.s3_multipart_upload.file_size))
            
            #upload ( run this if file is a local file )
            if self.s3_multipart_upload.is_local_file:
                part = s3.upload_part(Body=self.data, Bucket=self.s3_multipart_upload.bucket, Key=self.s3_multipart_upload.key, UploadId=self.s3_multipart_upload.mpu_id, PartNumber=self.part_number)
                self.etag=part['ETag']
            else:
                # TODO    -Add implementation to transfer if file is remote s3 file instead of local file
                pass
                
def resume_mpu(Bucket, Key, File, Mpu_Id):
    # How is Bucket and Key used here?
    S3MultipartUpload(Mpu_Id, File, Bucket, Key).resume_mpu()

#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Bucket.multipart_uploads
bucket="test.nimhda.org"
s3 = boto3.client('s3')

#upload parts of a test file for multi-part upload
bucket="test.nimhda.org"
key="testmpu"
mpu = s3.create_multipart_upload(Bucket=bucket, Key=key)
print("mpu:{}".format(mpu))
parts = []
mpu_id = mpu["UploadId"]
f=open("C:\\Users\\magditsgs\\workspaces\\misc\\scripts\\orgochem.pdf", "rb")
data = f.read(int(5e6))
part = s3.upload_part(Body=data, Bucket=bucket, Key=key, UploadId=mpu_id, PartNumber=1)
parts.append(part)
print("part:{}".format(part))

#run the function we will make that resumes submission of a file
resume_mpu(Bucket=bucket, Key=key, File=f, Mpu_Id=mpu_id)
#S3MultipartUpload(Mpu_Id=Mpu_Id, File=File, Bucket=Bucket, Key=Key).resume_mpu()

# TODO - THIS METHOD RETURNS A MAX OF 1000 UPLOADS, SO IN USHNAS SCRIPT, THE RESULTS NEED TO BE READ UNTIL THE END
#check that the target key exists in amazon and that the multipart upload is complete
response = s3.list_multipart_uploads( Bucket=bucket) 
print("response - outer scope: {}".format(response))
assert 'Uploads' not in response

# TODO - need to determine how localfiles are mapped to s3 files in Ushnas script and incorporate that logic into the final code

""" 
    should only call this if we know that the file parameter has some parts in s3 already
"""    