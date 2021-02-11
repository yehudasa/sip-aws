import json

from env import *
from db import *

from duplog import *
from bilog import *
from datalog import *


def handle_s3_event(fifo_seq, group_id, event):
    logger.info('## EVENT')
    logger.info(event)

    s3 = event['s3']
    s3_bucket = s3['bucket']['name']
    obj = s3['object']
    s3_obj_key = obj['key']
    
    arr = s3_obj_key.split('/', 1)
    
    if len(arr) == 2:
        bucket, obj_key = arr
    else:
        bucket = env_params.top_level_bucket
        obj_key = s3_obj_key
    

    obj_seq = int(obj['sequencer'], 16)

    key = { 'bucket': bucket,
            'obj': obj_key }

    dl = DupLog()            
    success = dl.check_seq(bucket, obj_key, obj_seq)
    if not success:
        logger.info('NOTICE: not storing bilog key for %s/%s' % (bucket, obj_key))
        return
    
    obj_size = obj.get('size', 0)   # delete event might not hold it
    etag = obj.get('eTag', '')
    op = event['eventName']
    timestamp = event['eventTime']
    
    fifo_seq_dec = int(fifo_seq)
    bilog_key = hex(fifo_seq_dec).lstrip('0x').rstrip('L').zfill(20)
    
    bucket_shard_id = ceph_str_hash_linux(obj_key) % env_params.bilog_num_shards
    
    bilog = BILog(bucket, bucket_shard_id)
    
    success = bilog.store_entry(bilog_key,
                      bucket, obj_key, obj_size,
                      etag, op, timestamp)
    
    if success:
        dl = DataLog(bucket, bucket_shard_id, env_params.bilog_num_shards, timestamp)
        
        dl.store_entries()
    

def lambda_handler(event, context):
    # logger.info('## ENVIRONMENT VARIABLES')
    # logger.info(os.environ)

    logger.info('### event:')
    logger.info(json.dumps(event))

    for r in event['Records']:
        body_str = r['body']
        body = json.loads(body_str)
        
        attrs = r['attributes']
        fifo_seq = attrs['SequenceNumber']
        group_id = int(attrs['MessageGroupId'])

        handle_s3_event(fifo_seq, group_id, body)
            

    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }
