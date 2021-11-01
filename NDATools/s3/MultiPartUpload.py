import hashlib
import signal

from NDATools.s3.S3Authentication import S3Authentication
from NDATools.utils import Utils
from NDATools.utils.Utils import exit_client


class UploadMultiParts:
    def __init__(self, upload_obj, abs_path, nda_destination_url, credentials, file_size):
        if (file_size > 9999):
            self.chunk_size = (
                        file_size // 9999)  # dynamically set chunk size based on file size and aws limit on number of parts
        else:
            self.chunk_size = file_size
        self.upload_obj = upload_obj
        self.abs_path = abs_path
        self.upload_id = self.upload_obj['UploadId']
        self.bucket, self.key = Utils.deconstruct_s3_url(nda_destination_url)
        self.access_key = credentials['access_key']
        self.secret_key = credentials['secret_key']
        self.session_token = credentials['session_token']
        self.client = S3Authentication.get_s3_client_with_config(self.access_key, self.secret_key, self.session_token)
        self.completed_bytes = 0
        self.completed_parts = 0
        self.parts = []
        self.parts_completed = []

    def get_and_set_part_information(self):
        response = self.client.list_parts(Bucket=self.bucket, Key=self.key, UploadId=self.upload_id)

        if Constants.PARTS in response:
            chunk_size = response[Constants.PARTS][0][
                Constants.SIZE]  # size of first part should be size of all subsequent parts
            if chunk_size != 0:
                self.chunk_size = chunk_size
            for p in response[Constants.PARTS]:
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
            exit_client(signal=signal.SIGTERM,
                        message=message)  # force exit because file has been modified (data integrity)

    def upload_part(self, data, i):
        part = self.client.upload_part(Body=data, Bucket=self.bucket, Key=self.key, UploadId=self.upload_id,
                                       PartNumber=i)
        self.parts.append({Constants.PART_NUM: i, Constants.ETAG: part[Constants.ETAG]})
        self.completed_bytes += len(data)

    def complete(self):
        self.client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            MultipartUpload={Constants.PARTS: self.parts})

    def resume_multipart_upload(self):
        self.get_and_set_part_information()
        seq = 1

        with open(self.abs_path, 'rb') as f:
            while True:
                buffer_start = self.chunk_size * (seq - 1)
                f.seek(buffer_start)
                buffer = f.read(self.chunk_size)
                if len(buffer) == 0:  # EOF
                    break
                if seq in self.parts_completed:
                    part = self.parts[seq - 1]
                    self.check_md5(part, buffer)
                else:
                    self.upload_part(buffer, seq)
                seq += 1
        self.complete()


class Constants:
    PARTS = 'Parts'
    SIZE = 'Size'
    PART_NUM = 'PartNumber'
    ETAG = 'ETag'
