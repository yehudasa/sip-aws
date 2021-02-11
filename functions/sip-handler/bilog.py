from env import *
from db import *

def bilog_table_settings_cb():
    return {
        'KeySchema': [
            {
                'AttributeName': 'bucket_shard_id',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'entry_id',
                'KeyType': 'RANGE'  # Partition key
            },
        ],
        'AttributeDefinitions': [
            {
                'AttributeName': 'bucket_shard_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'entry_id',
                'AttributeType': 'S'
            },
        ]
    }


class BILog:
    def __init__(self, bucket, group_id):
        self.table_name = env_params.db_prefix + 'bilog'
        self.bucket = bucket
        self.shard_id = group_id

        self.dbtable = get_table(self.table_name, bilog_table_settings_cb)
        logger.info("Table %s status: %s" % (self.table_name, self.dbtable.table_status))

    def store_entry(self, bilog_key, 
                      bucket, obj_key, obj_size,
                      etag, op, timestamp):
                  
        logger.info("Table %s status: %s" % (self.table_name, self.dbtable.table_status))
    
        key = { 'bucket_shard_id': '%s.%s' % (self.bucket, self.shard_id),
                'entry_id': bilog_key }
    
        try:
            self.dbtable.update_item(
                    Key=key,
                    UpdateExpression='SET #bucket=:bucket, #obj=:obj, #size=:size, #etag=:etag, #op=:op, #ts=:ts',
                    ExpressionAttributeNames={
                        '#bucket': 'bucket',
                        '#obj': 'obj',
                        '#size': 'size',
                        '#etag': 'etag',
                        '#op': 'op',
                        '#ts': 'timestamp',
                        },
                    ExpressionAttributeValues={
                        ':bucket': bucket,
                        ':obj': obj_key,
                        ':size': obj_size,
                        ':etag': etag,
                        ':op': op,
                        ':ts': timestamp,
                    } )
        except BaseException as e:
            logger.error('ERROR: failed to update db (key=%s obj=%s/%s): %s'  % (bilog_key, bucket, obj_key, e))
            raise e
    
        return True

