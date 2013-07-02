import os
import json
from time import sleep
from  urllib2 import urlopen

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3CreateError
from boto.ses.connection import SESConnection

def get_creds(fname='aws.json'):
    try:
        return json.loads(open(fname).read())
    except IOError:
        print 'Missing credentials. Please put AWS credentials in aws.json file in same folder as transfer.py.\nRequired fields are "access_key" and "secret_key".'
        exit
    
def get_bucket_conn(bucket_name, creds=get_creds()):
    s3conn = S3Connection(creds['access_key'], creds['secret_key'])
    bucket_conn = None
    backoff = 1
    while not bucket_conn:
        try:
            bucket_conn = s3conn.create_bucket(bucket_name)
        except S3CreateError:
            backoff = backoff * 2
            print "Could not create bucket connection. Waiting %s second(s)." % backoff
            if backoff <= 10:
                print "You should also double-check that the bucket is avilable on your account."
            sleep(backoff)
            backoff += 1
    return bucket_conn

def exists_on_s3(s3key, bucket_name=None, bucket_conn=None, return_bucket=False):
    if not bucket_conn or (bucket_name and bucket_conn.name != bucket_name):
        bucket_conn = get_bucket_conn(bucket_name, ACCESSKEY, SECRETKEY)

    k = bucket_conn.get_key(s3key)

    if k:
        if return_bucket:
            return True, bucket_conn
        else:
            return True
    else:
        if return_bucket:
            return False, bucket_conn
        else:
            return False

def download(uri, local_path):
    remote = urlopen(uri)
    try:
        os.makedirs(os.path.dirname(local_path))
    except:
        pass
    print '\nDownloading %s to %s' % (uri, local_path)
    fp = open(local_path, 'wb').write(remote.read())
    print '\nDownload complete'
    return local_path

def upload_to_s3(local_path, s3_key, bucket_conn=None, rrs=True):
    if not bucket_conn:
        bucket_conn = get_bucket_conn(bucket_name, ACCESSKEY, SECRETKEY)
    k = Key(bucket_conn)

    k.key = s3_key

    print "\nUploading\n%s\ns3://%s/%s" % (local_path, k.bucket.name, k.key)
    k.set_contents_from_filename(local_path, reduced_redundancy=rrs)
    print "\nUpload complete\n"
    
    return k

def send_email(to_email, from_email, subject=None, body=None,
               creds=get_creds()):
    ses = SESConnection(creds['access_key'], creds['secret_key'])
    return ses.send_email(from_email.strip(), subject, body, to_email.strip())

