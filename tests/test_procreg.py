
from kyd.handlers import ProcessedRegister

from google.cloud import storage

client = storage.Client()
bucket = storage.Bucket(client, 'ks-objects')
pr = ProcessedRegister(bucket, 'IDI', 'cetipdata')