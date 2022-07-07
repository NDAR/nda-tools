from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

'''
This adapter is necessary for the download logic when files are located in alt endpoints and the presigned urls
generated from the query-package-service contain '.' characters
Reason -   VHostCalling format fails SSL certification when bucket contains '.'
see https://stackoverflow.com/questions/34154791/foo-bar-com-s3-amazonaws-com-doesnt-match-either-of-s3-amazonaws-com-s3
'''
class AltEndpointSSLAdapter(HTTPAdapter):

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = PoolManager(
            num_pools=connections, maxsize=maxsize,
            block=block, assert_hostname='s3.amazonaws.com')