import hashlib

from NDATools.Utils import *

logger = logging.getLogger(__name__)

class MultiPartsUpload:
    def __init__(self, bucket, prefix, config, access_key, secret_key, session_token):
        self.bucket = bucket
        self.prefix = prefix
        self.config = config
        self.access_key = access_key
        self.secret_key = secret_key
        self.session_token = session_token
        self.client = get_s3_client_with_config(self.access_key, self.secret_key, self.session_token)
        self.incomplete_mpu = []
        self.mpu_to_abort = {}

    def get_multipart_uploads(self):
        try:
            uploads = self.client.list_multipart_uploads(Bucket=self.bucket, Prefix=self.prefix)['Uploads']
            for u in uploads:
                if u not in self.incomplete_mpu:
                    self.incomplete_mpu.append(u)
                else:
                    self.mpu_to_abort[u['UploadId']] = u['Key']
        except KeyError:
            uploads = None

        if self.mpu_to_abort:
            self.abort_mpu()


    def abort_mpu(self):
        for upload_id, key in self.mpu_to_abort.items():
            self.client.abort_multipart_upload(
                Bucket=self.bucket, Key=key, UploadId=upload_id)
class Constants:
    PARTS = 'Parts'
    SIZE = 'Size'
    PART_NUM = 'PartNumber'
    ETAG = 'ETag'


class UploadMultiParts:
    def __init__(self, upload_obj, full_file_path, bucket, prefix, config, credentials, file_size):
        if (file_size > 9999):
            self.chunk_size = (file_size // 9999) # dynamically set chunk size based on file size and aws limit on number of parts
        else:
            self.chunk_size = file_size
        self.upload_obj = upload_obj
        self.full_file_path = full_file_path
        self.upload_id = self.upload_obj['UploadId']
        self.bucket = bucket
        self.key = self.upload_obj['Key']
        filename = self.key.split(prefix+'/')
        filename = "".join(filename[1:])
        self.filename, self.file_size = self.full_file_path[filename]
        self.config = config
        self.access_key = credentials['access_key']
        self.secret_key = credentials['secret_key']
        self.session_token = credentials['session_token']
        self.client = get_s3_client_with_config(self.access_key, self.secret_key, self.session_token)
        self.completed_bytes = 0
        self.completed_parts = 0
        self.parts = []
        self.parts_completed = []

    def get_parts_information(self):
        self.upload_obj = self.client.list_parts(Bucket=self.bucket, Key=self.key,
                                                 UploadId=self.upload_id)

        if Constants.PARTS in self.upload_obj:
            chunk_size = self.upload_obj[Constants.PARTS][0][Constants.SIZE] # size of first part should be size of all subsequent parts
            if chunk_size != 0:
                self.chunk_size = chunk_size
            for p in self.upload_obj[Constants.PARTS]:
                try:
                    self.parts.append({Constants.PART_NUM: p[Constants.PART_NUM], Constants.ETAG: p[Constants.ETAG]})
                    self.parts_completed.append(p[Constants.PART_NUM])
                except KeyError:
                    pass

        self.completed_bytes = self.chunk_size * len(self.parts)

    def check_md5(self, part, data):

        ETag = (part[Constants.ETAG]).split('"')[1]

        md5 = hashlib.md5(data).hexdigest()
        if md5 != ETag:
            message = "The file seems to be modified since previous upload attempt(md5 value does not match)."
            exit_client(signal=signal.SIGTERM, message=message) # force exit because file has been modified (data integrity)

    def upload_part(self, data, i):
        part = self.client.upload_part(Body=data, Bucket=self.bucket, Key=self.key, UploadId=self.upload_id, PartNumber=i)
        self.parts.append({Constants.PART_NUM: i, Constants.ETAG: part[Constants.ETAG]})
        self.completed_bytes += len(data)

    def complete(self):
        self.client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            MultipartUpload={Constants.PARTS: self.parts})
